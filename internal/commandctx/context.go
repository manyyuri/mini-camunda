// Package commandctx 镜像 org.camunda.bpm.engine.impl.context.CommandContext：
// 实体缓存 + 脏集合 + 历史事件暂存，命令提交前统一 flush（对应 DbSqlSession 的语义）。
package commandctx

import (
	"fmt"

	"mini-camunda/internal/persistence"
)

// Env 命令上下文依赖的引擎环境（由 engine 包注入，避免循环引用）。
type Env interface {
	NextID() string
	HistoryLevel() string // none | activity | audit | full
}

type dirtyOp struct {
	kind int // 0 insert / 1 update / 2 delete
	fn   func(tx *persistence.Tx) error
}

type hiOp func(tx *persistence.Tx) error

type CommandContext struct {
	Tx   *persistence.Tx
	env  Env
	name string

	// 实体缓存：id → 实体指针（命令内 cache-through 读写）
	execCache map[string]*persistence.Execution
	taskCache map[string]*persistence.Task
	jobCache  map[string]*persistence.Job
	varCache  map[string]*persistence.Variable

	inserts    []dirtyOp
	updates    []dirtyOp
	deletes    []dirtyOp
	execDead   map[string]bool // 已删除执行的墓碑：同命令内禁止从 DB 回捞
	updateSeen map[string]bool
	varDeleted map[string]bool
	varNew     map[string]bool // 本命令新建的变量：insert 未落库前 update 只改内存
	history    []hiOp
}

func New(tx *persistence.Tx, env Env, cmdName string) *CommandContext {
	return &CommandContext{
		Tx: tx, env: env, name: cmdName,
		execCache:  map[string]*persistence.Execution{},
		taskCache:  map[string]*persistence.Task{},
		jobCache:   map[string]*persistence.Job{},
		varCache:   map[string]*persistence.Variable{},
		updateSeen: map[string]bool{},
		varDeleted: map[string]bool{},
		varNew:     map[string]bool{},
	}
}

func (c *CommandContext) Name() string { return c.name }
func (c *CommandContext) NextID() string { return c.env.NextID() }
func (c *CommandContext) HistoryLevel() string { return c.env.HistoryLevel() }

// ===== 实体缓存（cache-through 读）=====

func (c *CommandContext) Exec(id string) (*persistence.Execution, error) {
	if e, ok := c.execCache[id]; ok {
		return e, nil
	}
	if c.execDead[id] {
		return nil, fmt.Errorf("执行 %s 已在本命令内删除", id)
	}
	e, err := persistence.SelectExecution(c.Tx, id)
	if err != nil {
		return nil, err
	}
	c.execCache[id] = e
	return e, nil
}

func (c *CommandContext) ExecsByInstance(instID string) ([]*persistence.Execution, error) {
	all, err := persistence.SelectExecutionsByProcInstance(c.Tx, instID)
	if err != nil {
		return nil, err
	}
	return c.mergeExecs(all, func(e *persistence.Execution) bool { return e.ProcInstID == instID }), nil
}

func (c *CommandContext) ExecsByParent(parentID string) ([]*persistence.Execution, error) {
	all, err := persistence.SelectExecutionsByParent(c.Tx, parentID)
	if err != nil {
		return nil, err
	}
	return c.mergeExecs(all, func(e *persistence.Execution) bool { return e.ParentID == parentID }), nil
}

// mergeExecs 把缓存（含未 flush 的新建/修改）叠加到 DB 查询结果上：
// 1. 跳过墓碑（本命令已删除）；2. DB 行优先用缓存版本；3. 仅存在于缓存的“新建行”补齐
// （insert 在 flush 阶段才落库，同一命令内后续查询必须能看到它们）。
func (c *CommandContext) mergeExecs(dbRows []*persistence.Execution, cacheMatch func(*persistence.Execution) bool) []*persistence.Execution {
	out := make([]*persistence.Execution, 0, len(dbRows)+4)
	seen := map[string]bool{}
	for _, e := range dbRows {
		if c.execDead[e.ID] {
			seen[e.ID] = true
			continue
		}
		if ce, ok := c.execCache[e.ID]; ok {
			out = append(out, ce)
		} else {
			c.execCache[e.ID] = e
			out = append(out, e)
		}
		seen[e.ID] = true
	}
	// 补上仅存在于缓存（本命令新建、尚未 flush）且命中条件的行
	for id, e := range c.execCache {
		if seen[id] || c.execDead[id] {
			continue
		}
		if cacheMatch(e) {
			out = append(out, e)
		}
	}
	return out
}

func (c *CommandContext) Task(id string) (*persistence.Task, error) {
	if t, ok := c.taskCache[id]; ok {
		return t, nil
	}
	t, err := persistence.SelectTask(c.Tx, id)
	if err != nil {
		return nil, err
	}
	c.taskCache[id] = t
	return t, nil
}

func (c *CommandContext) Job(id string) (*persistence.Job, error) {
	if j, ok := c.jobCache[id]; ok {
		return j, nil
	}
	j, err := persistence.SelectJob(c.Tx, id)
	if err != nil {
		return nil, err
	}
	c.jobCache[id] = j
	return j, nil
}

// TasksByInstance：DB 行 + 缓存叠加（含本命令新建、尚未 flush 的任务）。
func (c *CommandContext) TasksByInstance(instID string) ([]*persistence.Task, error) {
	dbRows, err := persistence.SelectTasksByProcInstance(c.Tx, instID)
	if err != nil {
		return nil, err
	}
	out := make([]*persistence.Task, 0, len(dbRows)+2)
	seen := map[string]bool{}
	for _, t := range dbRows {
		if ct, ok := c.taskCache[t.ID]; ok {
			out = append(out, ct)
		} else {
			c.taskCache[t.ID] = t
			out = append(out, t)
		}
		seen[t.ID] = true
	}
	for id, t := range c.taskCache {
		if !seen[id] && t.ProcInstID == instID {
			out = append(out, t)
		}
	}
	return out, nil
}

// JobsByInstance：DB 行 + 缓存叠加（含本命令新建、尚未 flush 的 Job）。
func (c *CommandContext) JobsByInstance(instID string) ([]*persistence.Job, error) {
	dbRows, err := persistence.SelectJobs(c.Tx, instID)
	if err != nil {
		return nil, err
	}
	out := make([]*persistence.Job, 0, len(dbRows)+2)
	seen := map[string]bool{}
	for _, j := range dbRows {
		if cj, ok := c.jobCache[j.ID]; ok {
			out = append(out, cj)
		} else {
			c.jobCache[j.ID] = j
			out = append(out, j)
		}
		seen[j.ID] = true
	}
	for id, j := range c.jobCache {
		if !seen[id] && j.ProcessInstID == instID {
			out = append(out, j)
		}
	}
	return out, nil
}

// ===== 脏标记（flush 时按 insert → update → delete 顺序执行；同实体 update 去重）=====

// markUpdate 同一实体多次修改只入队一次（闭包引用指针，flush 时取最新状态），
// 否则第二次 UPDATE 的 WHERE REV_ 条件会因第一次已推进 REV_ 而误报乐观锁冲突。
func (c *CommandContext) markUpdate(key string, fn func(tx *persistence.Tx) error) {
	if c.updateSeen[key] {
		return
	}
	c.updateSeen[key] = true
	c.updates = append(c.updates, dirtyOp{1, fn})
}

func (c *CommandContext) InsertExec(e *persistence.Execution) {
	c.execCache[e.ID] = e
	c.inserts = append(c.inserts, dirtyOp{0, func(tx *persistence.Tx) error { return persistence.InsertExecution(tx, e) }})
}

func (c *CommandContext) UpdateExec(e *persistence.Execution) {
	c.markUpdate("exec:"+e.ID, func(tx *persistence.Tx) error { return persistence.UpdateExecution(tx, e) })
}

func (c *CommandContext) DeleteExec(e *persistence.Execution) {
	c.deletes = append(c.deletes, dirtyOp{2, func(tx *persistence.Tx) error { return persistence.DeleteExecution(tx, e.ID) }})
	delete(c.execCache, e.ID)
	if c.execDead == nil {
		c.execDead = map[string]bool{}
	}
	c.execDead[e.ID] = true
}

func (c *CommandContext) InsertTask(t *persistence.Task) {
	c.taskCache[t.ID] = t
	c.inserts = append(c.inserts, dirtyOp{0, func(tx *persistence.Tx) error { return persistence.InsertTask(tx, t) }})
}

func (c *CommandContext) UpdateTask(t *persistence.Task) {
	c.markUpdate("task:"+t.ID, func(tx *persistence.Tx) error { return persistence.UpdateTask(tx, t) })
}

func (c *CommandContext) DeleteTask(t *persistence.Task) {
	c.deletes = append(c.deletes, dirtyOp{2, func(tx *persistence.Tx) error {
		if err := persistence.DeleteIdentityLinksByTask(tx, t.ID); err != nil {
			return err
		}
		return persistence.DeleteTask(tx, t.ID)
	}})
	delete(c.taskCache, t.ID)
}

func (c *CommandContext) InsertJob(j *persistence.Job) {
	c.jobCache[j.ID] = j
	c.inserts = append(c.inserts, dirtyOp{0, func(tx *persistence.Tx) error { return persistence.InsertJob(tx, j) }})
}

func (c *CommandContext) UpdateJob(j *persistence.Job) {
	c.markUpdate("job:"+j.ID, func(tx *persistence.Tx) error { return persistence.UpdateJob(tx, j) })
}

func (c *CommandContext) DeleteJob(j *persistence.Job) {
	c.deletes = append(c.deletes, dirtyOp{2, func(tx *persistence.Tx) error { return persistence.DeleteJob(tx, j.ID) }})
	delete(c.jobCache, j.ID)
}

func (c *CommandContext) InsertVar(v *persistence.Variable) {
	c.varCache[v.ID] = v
	c.varNew[v.ID] = true
	c.inserts = append(c.inserts, dirtyOp{0, func(tx *persistence.Tx) error { return persistence.InsertVariable(tx, v) }})
}

func (c *CommandContext) UpdateVar(v *persistence.Variable) {
	c.varCache[v.ID] = v
	if c.varNew[v.ID] {
		return // insert 尚未落库，直接改内存即可（flush 时写入最终状态）
	}
	c.markUpdate("var:"+v.ID, func(tx *persistence.Tx) error { return persistence.UpdateVariable(tx, v) })
}

func (c *CommandContext) DeleteVar(v *persistence.Variable) {
	c.deletes = append(c.deletes, dirtyOp{2, func(tx *persistence.Tx) error { return persistence.DeleteVariable(tx, v.ID) }})
	delete(c.varCache, v.ID)
	c.varDeleted[v.ID] = true
}

// VariablesByExecution cache-through 读：DB 值叠加本命令内未落库的新增/修改，
// 并排除本命令内已删除的（同一命令里写后读必须能看到，否则网关条件、camunda:in 映射会读到旧值）。
func (c *CommandContext) VariablesByExecution(execID string) ([]*persistence.Variable, error) {
	dbVars, err := persistence.SelectVariablesByExecution(c.Tx, execID)
	if err != nil {
		return nil, err
	}
	out := make([]*persistence.Variable, 0, len(dbVars)+len(c.varCache))
	seen := map[string]bool{}
	for _, v := range dbVars {
		if c.varDeleted[v.ID] {
			continue
		}
		if cv, ok := c.varCache[v.ID]; ok {
			if cv.ExecutionID == execID {
				out = append(out, cv)
				seen[v.ID] = true
			}
			continue // 变量被移到其他执行（不应发生，防御处理）
		}
		out = append(out, v)
		seen[v.ID] = true
	}
	for _, v := range c.varCache {
		if !seen[v.ID] && v.ExecutionID == execID && !c.varDeleted[v.ID] {
			out = append(out, v)
		}
	}
	return out, nil
}

// AddHistory 历史事件在提交时落库（回滚则不落）。
func (c *CommandContext) AddHistory(fn func(tx *persistence.Tx) error) {
	c.history = append(c.history, hiOp(fn))
}

// Flush 命令成功后、事务提交前调用：insert → update → delete → history。
func (c *CommandContext) Flush() error {
	for _, op := range c.inserts {
		if err := op.fn(c.Tx); err != nil {
			return fmt.Errorf("flush insert: %w", err)
		}
	}
	for _, op := range c.updates {
		if err := op.fn(c.Tx); err != nil {
			return fmt.Errorf("flush update: %w", err)
		}
	}
	for _, op := range c.deletes {
		if err := op.fn(c.Tx); err != nil {
			return fmt.Errorf("flush delete: %w", err)
		}
	}
	for _, h := range c.history {
		if err := h(c.Tx); err != nil {
			return fmt.Errorf("flush history: %w", err)
		}
	}
	return nil
}
