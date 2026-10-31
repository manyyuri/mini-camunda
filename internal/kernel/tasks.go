// 用户任务：对应 TaskEntity + UserTaskActivityBehavior。
package kernel

import (
	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/el"
	"mini-camunda/internal/errs"
	"mini-camunda/internal/persistence"
)

// createUserTask 进入 userTask 等待态。
func (k *Kernel) createUserTask(e *persistence.Execution, act *bpmn.Activity) {
	t := &persistence.Task{
		ID: k.ctx.NextID(), Name: act.Name, Created: k.now(), Priority: 50,
		ProcDefID: e.ProcDefID, ProcInstID: e.ProcInstID, ExecutionID: e.ID, TaskDefKey: act.ID,
	}
	if a, ok := act.Props[bpmn.PropAssignee].(string); ok {
		t.Assignee = a
	}
	k.ctx.InsertTask(t)
	// assignee 表达式求值（Camunda 支持 ${}，如 ${starter}）
	if t.Assignee == "" {
		// 未设置则不指派
	} else {
		vars := k.visibleVars(e)
		t.Assignee = elStr(t.Assignee, vars)
		k.ctx.UpdateTask(t)
	}
	// 候选人/组 → ACT_RU_IDENTITYLINK
	vars := k.visibleVars(e)
	if users, ok := act.Props[bpmn.PropCandidateUsers].([]string); ok {
		for _, u := range users {
			k.ctx.Tx.Exec(`INSERT INTO ACT_RU_IDENTITYLINK(ID_, TYPE_, USER_ID_, TASK_ID_) VALUES(?,?,?,?)`,
				k.ctx.NextID(), "candidate", elStr(u, vars), t.ID)
		}
	}
	if groups, ok := act.Props[bpmn.PropCandidateGroups].([]string); ok {
		for _, g := range groups {
			k.ctx.Tx.Exec(`INSERT INTO ACT_RU_IDENTITYLINK(ID_, TYPE_, GROUP_ID_, TASK_ID_) VALUES(?,?,?,?)`,
				k.ctx.NextID(), "candidate", elStr(g, vars), t.ID)
		}
	}
	k.hiTaskCreate(t, e, act)
}

// CompleteTask 命令入口：完成任务 + 写任务局部/实例级变量 + 推进。
func (k *Kernel) CompleteTask(taskID string, vars map[string]any) error {
	t, err := k.ctx.Task(taskID)
	if err != nil {
		return errs.NewNotFound("任务 %s", taskID)
	}
	e, err := k.ctx.Exec(t.ExecutionID)
	if err != nil {
		return err
	}
	// 变量写实例级（Camunda complete 语义）
	root, err := k.rootOf(e)
	if err != nil {
		return err
	}
	for n, v := range vars {
		k.setVariableOnScope(root, n, v, false)
	}
	k.hiTaskEnd(t.ID, "completed")
	k.actInstEnd(e.ActInstID, "completed", t.Assignee)
	k.ctx.DeleteTask(t)
	if err := k.finishActivity(e); err != nil {
		return err
	}
	return k.Run()
}

// SetTaskAssignee / Claim / Complete claim 语义（TaskService）。
func (k *Kernel) Claim(taskID, userID string) error {
	t, err := k.ctx.Task(taskID)
	if err != nil {
		return errs.NewNotFound("任务 %s", taskID)
	}
	if t.Assignee != "" {
		return errs.NewConflict("任务 %s 已被 %s claim", taskID, t.Assignee)
	}
	t.Assignee = userID
	k.ctx.UpdateTask(t)
	return nil
}

func elStr(s string, vars map[string]any) string {
	if len(s) >= 3 && s[:2] == "${" && s[len(s)-1] == '}' {
		if v, err := el.EvaluateString(s, vars); err == nil {
			return v
		}
	}
	return s
}
