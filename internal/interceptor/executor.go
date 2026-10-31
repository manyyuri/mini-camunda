// Package interceptor 镜像 org.camunda.bpm.engine.impl.interceptor：
// 命令拦截器链 + 带乐观锁重试的执行器（对应 Camunda 命令在 OptimisticLockingException 上的退避重试）。
package interceptor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"mini-camunda/internal/commandctx"
	"mini-camunda/internal/persistence"
)

// Command 对应 org.camunda.bpm.engine.impl.interceptor.Command。
type Command interface {
	Name() string
	Execute(ctx *commandctx.CommandContext) error
}

// CommandFunc 便捷包装。
type CommandFunc struct {
	CmdName string
	Fn      func(ctx *commandctx.CommandContext) error
}

func (f *CommandFunc) Name() string { return f.CmdName }
func (f *CommandFunc) Execute(ctx *commandctx.CommandContext) error { return f.Fn(ctx) }

// Executor 引擎持有的命令执行器（拦截器链已装配）。
type Executor struct {
	ds    *persistence.DataSource
	env   commandctx.Env
	retries int
}

func NewExecutor(ds *persistence.DataSource, env commandctx.Env, retries int) *Executor {
	return &Executor{ds: ds, env: env, retries: retries}
}

// Execute 跑完整拦截器链：
// LogInterceptor → CommandContextInterceptor（建 ctx + flush）→ TransactionInterceptor（事务）。
// 乐观锁冲突 → 回滚并重试整个命令（默认 3 次）。
func (ex *Executor) Execute(cmd Command) error {
	var lastErr error
	for attempt := 1; attempt <= ex.retries; attempt++ {
		err := ex.executeOnce(cmd)
		if err == nil {
			return nil
		}
		if _, ok := err.(*persistence.OptimisticLockingException); ok && attempt < ex.retries {
			slog.Warn("optimistic locking conflict, retrying command", "cmd", cmd.Name(), "attempt", attempt)
			lastErr = err
			time.Sleep(time.Duration(attempt) * 10 * time.Millisecond) // 指数退避
			continue
		}
		return err
	}
	return fmt.Errorf("command %s failed after %d retries: %w", cmd.Name(), ex.retries, lastErr)
}

// executeOnce Log → Context → Transaction 三层拦截。
func (ex *Executor) executeOnce(cmd Command) error {
	start := time.Now()
	// TransactionInterceptor：开事务
	tx, err := ex.ds.BeginTx()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	// CommandContextInterceptor：建上下文
	ctx := commandctx.New(tx, ex.env, cmd.Name())
	// 执行命令本体
	execErr := cmd.Execute(ctx)
	if execErr != nil {
		_ = tx.Rollback()
		slog.Info("command failed", "cmd", cmd.Name(), "dur", time.Since(start).String(), "err", execErr.Error())
		return execErr
	}
	// 命令成功：flush 脏实体 + 历史，然后提交
	if err := ctx.Flush(); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// LogInterceptor 收尾日志
	slog.Debug("command executed", "cmd", cmd.Name(), "dur", time.Since(start).String())
	_ = context.Background()
	return nil
}
