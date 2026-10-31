// 服务层：对应 RepositoryService / RuntimeService / TaskService / HistoryService /
// ManagementService / ExternalTaskService 的命令封装（每个公共方法 = 一个命令）。
package engine

import (
	"fmt"
	"time"

	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/commandctx"
	"mini-camunda/internal/errs"
	"mini-camunda/internal/interceptor"
	"mini-camunda/internal/kernel"
	"mini-camunda/internal/persistence"
	"mini-camunda/internal/variable"
)

// execute 便捷封装：建命令并执行。
func (e *Engine) execute(name string, fn func(ctx *commandctx.CommandContext) error) error {
	return e.executor.Execute(&interceptor.CommandFunc{CmdName: name, Fn: fn})
}

// =====================================================================
// RepositoryService
// =====================================================================

// DeployResource 部署一个 BPMN 资源（DeployCmd）。
// 版本规则镜像 Camunda：同 key 自动 +1；流程定义 id = {key}:{version}:{random}。
func (e *Engine) DeployResource(resourceName string, xmlBytes []byte) (*persistence.ProcDefInfo, error) {
	def, err := bpmn.Parse(xmlBytes, resourceName)
	if err != nil {
		return nil, err
	}
	if err := bpmn.Validate(def); err != nil {
		return nil, err
	}

	var out *persistence.ProcDefInfo
	err = e.execute("DeployCmd", func(ctx *commandctx.CommandContext) error {
		dep := &persistence.Deployment{ID: e.NextID(), Name: resourceName, DeployTime: time.Now()}
		if err := persistence.InsertDeployment(ctx.Tx, dep); err != nil {
			return err
		}
		ba := &persistence.ByteArray{ID: e.NextID(), Name: resourceName, DeploymentID: dep.ID, Bytes: xmlBytes}
		if err := persistence.InsertByteArray(ctx.Tx, ba); err != nil {
			return err
		}
		// 版本 = 同 key 最大版本 + 1
		existing, err := persistence.SelectProcDefs(ctx.Tx, def.Key)
		if err != nil {
			return err
		}
		version := 1
		for _, p := range existing {
			if p.Version >= version {
				version = p.Version + 1
			}
		}
		def.ID = e.procDefID(def.Key, version)
		def.Version = version
		def.DeploymentID = dep.ID
		info := &persistence.ProcDefInfo{
			ID: def.ID, Key: def.Key, Version: version,
			DeploymentID: dep.ID, ResourceName: resourceName,
		}
		if err := persistence.InsertProcDef(ctx.Tx, info); err != nil {
			return err
		}
		// timer-start 注册
		k := kernel.New(ctx, e)
		for _, start := range def.TimerStarts {
			if err := k.CreateTimerStartJob(def, start); err != nil {
				return err
			}
		}
		out = info
		return nil
	})
	if err != nil {
		return nil, err
	}
	e.cacheDef(def)
	return out, nil
}

// CreateDeploymentQuery 对应 createDeploymentQuery。
func (e *Engine) Deployments() ([]*persistence.Deployment, error) {
	return persistence.SelectDeployments(e.ds.DB())
}

// ProcDefs 对应 createProcessDefinitionQuery。
func (e *Engine) ProcDefs(key string) ([]*persistence.ProcDefInfo, error) {
	return persistence.SelectProcDefs(e.ds.DB(), key)
}

// =====================================================================
// RuntimeService
// =====================================================================

// StartProcessInstanceByKey（StartProcessInstanceCmd）。
func (e *Engine) StartProcessInstanceByKey(key, businessKey string, vars map[string]any) (string, error) {
	def, err := e.LatestProcDefByKey(key)
	if err != nil {
		return "", errs.NewNotFound("流程定义 key=%s", key)
	}
	var instID string
	err = e.execute("StartProcessInstanceCmd", func(ctx *commandctx.CommandContext) error {
		k := kernel.New(ctx, e)
		root, err := k.StartInstance(def, businessKey, vars)
		if err != nil {
			return err
		}
		instID = root.ProcInstID
		return nil
	})
	return instID, err
}

// CorrelateMessage（CorrelateMessageCmd）。
func (e *Engine) CorrelateMessage(name, businessKey string, corrKeys, payload map[string]any) (int, error) {
	var n int
	err := e.execute("CorrelateMessageCmd", func(ctx *commandctx.CommandContext) error {
		k := kernel.New(ctx, e)
		var err error
		n, err = k.CorrelateMessage(name, businessKey, corrKeys, payload)
		return err
	})
	return n, err
}

// SignalEventReceived（SignalEventReceivedCmd broadcast 语义）。
func (e *Engine) SignalEventReceived(name string, payload map[string]any) error {
	return e.execute("SignalEventReceivedCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).BroadcastSignal(name, payload)
	})
}

// Trigger（TriggerCmd：触发等待中的执行）。
func (e *Engine) Trigger(execID string, vars map[string]any) error {
	return e.execute("TriggerCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).Trigger(execID, vars)
	})
}

// DeleteProcessInstance（DeleteProcessInstanceCmd：级联终止）。
func (e *Engine) DeleteProcessInstance(instID, reason string) error {
	return e.execute("DeleteProcessInstanceCmd", func(ctx *commandctx.CommandContext) error {
		k := kernel.New(ctx, e)
		root, err := k.RootExec(instID)
		if err != nil {
			return err
		}
		return k.DeleteProcessInstance(root, reason)
	})
}

// SetInstanceVariable 修改实例级变量（SetExecutionVariablesCmd 简化）。
func (e *Engine) SetInstanceVariable(instID, name string, value any) error {
	return e.execute("SetVariableCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).SetInstanceVariable(instID, name, value)
	})
}

// =====================================================================
// TaskService
// =====================================================================

func (e *Engine) Tasks(filter persistence.TaskFilter) ([]*persistence.Task, error) {
	return persistence.SelectTasks(e.ds.DB(), filter)
}

func (e *Engine) TaskVariables(taskID string) (map[string]any, error) {
	vars, err := persistence.SelectVariablesByTask(e.ds.DB(), taskID)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for _, v := range vars {
		out[v.Name] = variable.ToValue(v)
	}
	return out, nil
}

func (e *Engine) Claim(taskID, userID string) error {
	return e.execute("TaskClaimCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).Claim(taskID, userID)
	})
}

// CompleteTask（CompleteTaskCmd：完成任务 + 变量 + 推进）。
func (e *Engine) CompleteTask(taskID string, vars map[string]any) error {
	return e.execute("CompleteTaskCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).CompleteTask(taskID, vars)
	})
}

// =====================================================================
// ManagementService（Job / Incident）
// =====================================================================

func (e *Engine) Jobs() ([]*persistence.Job, error)        { return persistence.SelectJobs(e.ds.DB(), "") }
func (e *Engine) Incidents() ([]*persistence.Incident, error) { return persistence.SelectIncidents(e.ds.DB(), "") }

// SetJobRetries（SetJobRetriesCmd：重试重置，自动解决 Incident）。
func (e *Engine) SetJobRetries(jobID string, retries int) error {
	return e.execute("SetJobRetriesCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).SetJobRetries(jobID, retries)
	})
}

// ExecuteJob 手动执行单个 Job（JobExecutor 内部也用它；SetJobRetries 后补触发）。
func (e *Engine) ExecuteJob(jobID string) error {
	err := e.execute("ExecuteJobCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).ExecuteJob(jobID)
	})
	if jhe, ok := err.(*kernel.JobHandlerError); ok {
		// 技术失败：独立事务走重试/Incident（镜像 Camunda 的 FailedJobCommand）
		if ferr := e.execute("JobFailedCmd", func(ctx *commandctx.CommandContext) error {
			return kernel.New(ctx, e).FailJob(jobID, "job-executor", jhe.Err)
		}); ferr != nil {
			return ferr
		}
		return jhe
	}
	return err
}

// AcquireJobs Job 执行器获取（AcquireJobsCmd）。
func (e *Engine) AcquireJobs(workerID string, limit int, lockTime time.Duration) ([]*persistence.Job, error) {
	var out []*persistence.Job
	err := e.execute("AcquireJobsCmd", func(ctx *commandctx.CommandContext) error {
		var err error
		out, err = kernel.New(ctx, e).AcquireJobs(workerID, limit, lockTime)
		return err
	})
	return out, err
}

// =====================================================================
// ExternalTaskService
// =====================================================================

// FetchAndLock 长轮询版 fetch：先立即尝试，空则等待通知或超时。
func (e *Engine) FetchAndLock(topic, workerID string, lockTime time.Duration, maxTasks int, longPoll time.Duration) ([]*kernel.ExtTaskInfo, error) {
	deadline := time.Now().Add(longPoll)
	for {
		var out []*kernel.ExtTaskInfo
		err := e.execute("FetchExtTasksCmd", func(ctx *commandctx.CommandContext) error {
			var err error
			out, err = kernel.New(ctx, e).FetchAndLock(topic, workerID, lockTime, maxTasks)
			return err
		})
		if err != nil {
			return nil, err
		}
		if len(out) > 0 {
			return out, nil
		}
		if longPoll <= 0 || time.Now().After(deadline) {
			return nil, nil
		}
		// 等新任务通知或 500ms 轮询兜底
		select {
		case <-e.extCh:
		case <-time.After(500 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return nil, nil
		}
	}
}

func (e *Engine) CompleteExtTask(id, workerID string, vars map[string]any) error {
	return e.execute("CompleteExtTaskCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).CompleteExtTask(id, workerID, vars)
	})
}

func (e *Engine) FailExtTask(id, workerID, errMsg string, retries int) error {
	return e.execute("FailExtTaskCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).FailExtTask(id, workerID, errMsg, retries)
	})
}

func (e *Engine) SetExtTaskRetries(id string, retries int) error {
	return e.execute("SetExtTaskRetriesCmd", func(ctx *commandctx.CommandContext) error {
		return kernel.New(ctx, e).SetExtTaskRetries(id, retries)
	})
}

// =====================================================================
// 查询（Query API 直读，无事务）
// =====================================================================

func (e *Engine) ProcessInstances() ([]*persistence.Execution, error) {
	return persistence.SelectExecutions(e.ds.DB(), "")
}

func (e *Engine) InstanceVariables(instID string) (map[string]any, error) {
	vars, err := persistence.SelectVariablesByProcInstance(e.ds.DB(), instID)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for _, v := range vars {
		if v.TaskID == "" && !v.IsLocal {
			out[v.Name] = variable.ToValue(v)
		}
	}
	return out, nil
}

func (e *Engine) EventSubscriptions() ([]*persistence.EventSubscription, error) {
	subs, err := persistence.SelectEventSubscriptionsByName(e.ds.DB(), "message", "")
	if err != nil {
		return nil, err
	}
	sig, err := persistence.SelectEventSubscriptionsByName(e.ds.DB(), "signal", "")
	if err != nil {
		return nil, err
	}
	return append(subs, sig...), nil
}

// =====================================================================
// HistoryService
// =====================================================================

func (e *Engine) HistoricProcessInstances(state, procDefKey string) ([]*persistence.HiProcInst, error) {
	return persistence.SelectHiProcInsts(e.ds.DB(), state, procDefKey)
}

func (e *Engine) HistoricActivities(instID string) ([]*persistence.HiActInst, error) {
	return persistence.SelectHiActInsts(e.ds.DB(), instID)
}

func (e *Engine) HistoricTasks(instID string) ([]*persistence.HiTask, error) {
	return persistence.SelectHiTasks(e.ds.DB(), instID)
}

func (e *Engine) HistoricVariables(instID string) ([]*persistence.HiVarInst, error) {
	return persistence.SelectHiVarInsts(e.ds.DB(), instID)
}

func (e *Engine) HistoricJobLogs() ([]*persistence.HiJobLog, error) {
	return persistence.SelectHiJobLogs(e.ds.DB(), "")
}

func (e *Engine) HistoricIncidents(instID string) ([]*persistence.HiIncident, error) {
	return persistence.SelectHiIncidents(e.ds.DB(), instID)
}

var _ = fmt.Sprintf
