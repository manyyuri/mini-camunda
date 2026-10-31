// 外部任务：对应 ExternalTaskEntity + ExternalTaskActivityBehavior + ExternalTaskWorker。
package kernel

import (
	"time"

	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/errs"
	"mini-camunda/internal/persistence"
)

// createExtTask serviceTask(camunda:topic) 等待态。
func (k *Kernel) createExtTask(e *persistence.Execution, act *bpmn.Activity, topic string) {
	t := &persistence.ExtTask{
		ID: k.ctx.NextID(), Topic: topic, Retries: retryCountOf(act, 3),
		ExecutionID: e.ID, ProcInstID: e.ProcInstID, ProcDefID: e.ProcDefID, ActID: act.ID,
	}
	_ = persistence.InsertExtTask(k.ctx.Tx, t)
	k.deps.NotifyExtTask()
}

// ExtTaskInfo fetch 返回结构。
type ExtTaskInfo struct {
	ID        string
	Topic     string
	WorkerID  string
	Retries   int
	LockExp   *time.Time
	Variables map[string]any
	ProcInst  string
	ActID     string
}

// FetchAndLock 命令入口：按 topic 锁定外部任务（对应 FetchExternalTasksCmd）。
func (k *Kernel) FetchAndLock(topic, workerID string, lockTime time.Duration, maxTasks int) ([]*ExtTaskInfo, error) {
	if maxTasks <= 0 {
		maxTasks = 10
	}
	now := k.now()
	avail, err := persistence.SelectAvailableExtTasks(k.ctx.Tx, topic, persistence.FormatTime(now))
	if err != nil {
		return nil, err
	}
	var out []*ExtTaskInfo
	for _, t := range avail {
		if len(out) >= maxTasks {
			break
		}
		if t.WorkerID == workerID && t.LockExpTime != nil && t.LockExpTime.After(now) {
			continue // 已被自己锁定
		}
		exp := now.Add(lockTime)
		t.WorkerID, t.LockExpTime = workerID, &exp
		if err := persistence.UpdateExtTask(k.ctx.Tx, t); err != nil {
			return nil, err
		}
		info := &ExtTaskInfo{ID: t.ID, Topic: t.Topic, WorkerID: workerID, Retries: t.Retries, LockExp: t.LockExpTime, ProcInst: t.ProcInstID, ActID: t.ActID}
		if e, err := k.ctx.Exec(t.ExecutionID); err == nil {
			info.Variables = k.visibleVars(e)
		}
		out = append(out, info)
	}
	return out, nil
}

// CompleteExtTask 命令入口：校验 worker + 锁，写变量，推进流程。
func (k *Kernel) CompleteExtTask(extTaskID, workerID string, vars map[string]any) error {
	t, err := persistence.SelectExtTask(k.ctx.Tx, extTaskID)
	if err != nil {
		return errs.NewNotFound("外部任务 %s", extTaskID)
	}
	if t.WorkerID != workerID {
		return errs.NewConflict("外部任务 %s 未锁定给 worker %s", extTaskID, workerID)
	}
	e, err := k.ctx.Exec(t.ExecutionID)
	if err != nil {
		return errs.NewNotFound("外部任务 %s 的执行", extTaskID)
	}
	root, err := k.rootOf(e)
	if err != nil {
		return err
	}
	for n, v := range vars {
		k.setVariableOnScope(root, n, v, false)
	}
	if err := persistence.DeleteExtTask(k.ctx.Tx, extTaskID); err != nil {
		return err
	}
	if err := k.finishActivity(e); err != nil {
		return err
	}
	return k.Run()
}

// FailExtTask 命令入口：重试或 Incident（对应 HandleExternalTaskFailureCmd）。
func (k *Kernel) FailExtTask(extTaskID, workerID, errMsg string, retries int) error {
	t, err := persistence.SelectExtTask(k.ctx.Tx, extTaskID)
	if err != nil {
		return errs.NewNotFound("外部任务 %s", extTaskID)
	}
	if t.WorkerID != workerID {
		return errs.NewConflict("外部任务 %s 未锁定给 worker %s", extTaskID, workerID)
	}
	if retries >= 0 {
		t.Retries = retries
	} else {
		t.Retries--
	}
	t.ErrorMessage = trimErr(errMsg)
	t.WorkerID = ""
	t.LockExpTime = nil
	if t.Retries > 0 {
		return persistence.UpdateExtTask(k.ctx.Tx, t)
	}
	// 耗尽 → Incident(failedExternalTask)；外部任务保留（可 SetRetries 重试）
	if err := persistence.UpdateExtTask(k.ctx.Tx, t); err != nil {
		return err
	}
	inc := &persistence.Incident{
		ID: k.ctx.NextID(), Type: "failedExternalTask", Msg: t.ErrorMessage, Timestamp: k.now(),
		ExecutionID: t.ExecutionID, ActivityID: t.ActID, ProcInstID: t.ProcInstID, ProcDefID: t.ProcDefID,
	}
	if err := persistence.InsertIncident(k.ctx.Tx, inc); err != nil {
		return err
	}
	k.hiIncidentCreate(inc)
	return nil
}

// SetExtTaskRetries 管理命令：重置外部任务重试并解决 Incident。
func (k *Kernel) SetExtTaskRetries(extTaskID string, retries int) error {
	t, err := persistence.SelectExtTask(k.ctx.Tx, extTaskID)
	if err != nil {
		return errs.NewNotFound("外部任务 %s", extTaskID)
	}
	t.Retries = retries
	t.ErrorMessage = ""
	if err := persistence.UpdateExtTask(k.ctx.Tx, t); err != nil {
		return err
	}
	// 解决对应 Incident（按执行+活动+类型匹配未解决的）
	incidents, _ := persistence.SelectIncidents(k.ctx.Tx, t.ProcInstID)
	for _, inc := range incidents {
		if inc.Type == "failedExternalTask" && inc.ExecutionID == t.ExecutionID && inc.ActivityID == t.ActID {
			if err := persistence.DeleteIncident(k.ctx.Tx, inc.ID); err != nil {
				return err
			}
			k.hiIncidentEnd(inc.ID)
		}
	}
	return nil
}
