// 历史记录辅助：对应 org.camunda.bpm.engine.impl.history.*（HistoryLevel 判断 + 事件写入）。
package kernel

import (
	"time"

	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/persistence"
	"mini-camunda/internal/variable"
)

func (k *Kernel) hlAtLeast(level string) bool {
	cur := k.ctx.HistoryLevel()
	order := map[string]int{"none": 0, "activity": 1, "audit": 2, "full": 3}
	return order[cur] >= order[level]
}

// actInstStart 记录 ACT_HI_ACTINST 开始（activity 级别起记录，镜像 Camunda）。
func (k *Kernel) actInstStart(e *persistence.Execution, act *bpmn.Activity) string {
	if !k.hlAtLeast("activity") {
		return ""
	}
	h := &persistence.HiActInst{
		ID: k.ctx.NextID(), ProcDefID: e.ProcDefID, ProcInstID: e.ProcInstID,
		ExecutionID: e.ID, ActID: act.ID, ActName: act.Name, ActType: act.Type,
		StartTime: k.now(), ActInstState: "running",
	}
	h.ProcDefKey = k.procDefKey(e.ProcDefID)
	h.ParentActInstID = k.parentActInstID(e)
	k.actInsts[h.ID] = h
	k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiActInst(tx, h) })
	return h.ID
}

// actInstEnd 结束活动实例；带 assignee 快照。
func (k *Kernel) actInstEnd(id string, state string, assignee ...string) {
	if id == "" || !k.hlAtLeast("activity") {
		return
	}
	var h *persistence.HiActInst
	var ok bool
	end := k.now()
	if h, ok = k.actInsts[id]; ok {
		h.EndTime = &end
		h.ActInstState = state
		dur := end.Sub(h.StartTime).Milliseconds()
		h.Duration = &dur
	} else {
		// 跨命令结束（等待态）：先读出来再 upsert
		hists, err := persistence.SelectHiActInsts(k.ctx.Tx, "")
		if err != nil {
			return
		}
		for _, x := range hists {
			if x.ID == id {
				h = x
				break
			}
		}
		if h == nil {
			return
		}
		h.EndTime = &end
		h.ActInstState = state
		if !h.StartTime.IsZero() {
			dur := end.Sub(h.StartTime).Milliseconds()
			h.Duration = &dur
		}
	}
	if len(assignee) > 0 {
		h.Assignee = assignee[0]
	}
	snapshot := *h
	k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiActInst(tx, &snapshot) })
}

// parentActInstID 找包含当前活动的作用域执行的活动实例 id。
func (k *Kernel) parentActInstID(e *persistence.Execution) string {
	cur := e
	for cur != nil {
		if cur.IsScope && cur.ActInstID != "" {
			return cur.ActInstID
		}
		if cur.ParentID == "" {
			return ""
		}
		p, err := k.ctx.Exec(cur.ParentID)
		if err != nil {
			return ""
		}
		cur = p
	}
	return ""
}

func (k *Kernel) recordSequenceFlow(e *persistence.Execution, tr *bpmn.Transition) {
	if !k.hlAtLeast("activity") {
		return
	}
	h := &persistence.HiActInst{
		ID: k.ctx.NextID(), ProcDefID: e.ProcDefID, ProcInstID: e.ProcInstID,
		ExecutionID: e.ID, ActID: tr.ID, ActName: tr.ID, ActType: "sequenceFlow",
		StartTime: k.now(), ActInstState: "completed",
	}
	h.EndTime = &h.StartTime
	h.ProcDefKey = k.procDefKey(e.ProcDefID)
	k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiActInst(tx, h) })
}

func (k *Kernel) hiProcInstStart(root *persistence.Execution, def *bpmn.ProcessDefinition) {
	if !k.hlAtLeast("activity") {
		return
	}
	h := &persistence.HiProcInst{
		ID: root.ProcInstID, BusinessKey: root.BusinessKey,
		ProcDefKey: def.Key, ProcDefID: def.ID,
		StartTime: k.now(), State: "running",
	}
	k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiProcInst(tx, h) })
}

func (k *Kernel) hiProcInstEnd(root *persistence.Execution, def *bpmn.ProcessDefinition, state, endActID, deleteReason string) {
	if !k.hlAtLeast("activity") {
		return
	}
	now := k.now()
	h := &persistence.HiProcInst{
		ID: root.ProcInstID, BusinessKey: root.BusinessKey,
		ProcDefKey: def.Key, ProcDefID: def.ID,
		EndTime: &now,
		EndActID: endActID, DeleteReason: deleteReason, State: state,
	}
	// StartTime 留零：upsert（ON CONFLICT）只更新 END_* 字段，保留启动时写入的值
	k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiProcInst(tx, h) })
}

// hiTaskCreate ACT_HI_TASKINST（audit 级别起）。
func (k *Kernel) hiTaskCreate(t *persistence.Task, e *persistence.Execution, act *bpmn.Activity) {
	if !k.hlAtLeast("activity") {
		return
	}
	h := &persistence.HiTask{
		ID: t.ID, ProcDefID: t.ProcDefID, ProcInstID: t.ProcInstID, ExecutionID: t.ExecutionID,
		ActInstID: e.ActInstID, Name: t.Name, Assignee: t.Assignee,
		StartTime: t.Created, TaskDefKey: act.ID,
	}
	h.ProcDefKey = k.procDefKey(t.ProcDefID)
	k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiTask(tx, h) })
}

func (k *Kernel) hiTaskEnd(taskID, deleteReason string) {
	if !k.hlAtLeast("activity") {
		return
	}
	end := k.now()
	hists, err := persistence.SelectHiTasks(k.ctx.Tx, "")
	if err != nil {
		return
	}
	for _, h := range hists {
		if h.ID == taskID {
			h.EndTime = &end
			h.DeleteReason = deleteReason
			if !h.StartTime.IsZero() {
				dur := end.Sub(h.StartTime).Milliseconds()
				h.Duration = &dur
			}
			snapshot := *h
			k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiTask(tx, &snapshot) })
			return
		}
	}
}

// hiVarInst ACT_HI_VARINST（audit 级别起）。
func (k *Kernel) hiVarInst(v *persistence.Variable, state string) {
	if !k.hlAtLeast("audit") {
		return
	}
	now := k.now()
	h := &persistence.HiVarInst{
		ID: v.ID, ProcInstID: v.ProcInstID, ExecutionID: v.ExecutionID, TaskID: v.TaskID,
		Name: v.Name, VarType: v.Type,
		Text:        varText(v),
		Long:        v.Long,
		Double:      v.Double,
		State:       state,
		CreateTime:  now,
		LastUpdated: now,
	}
	k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiVarInst(tx, h) })
}

func varText(v *persistence.Variable) string {
	val := variable.ToValue(v)
	switch x := val.(type) {
	case string:
		return x
	case bool, int64, float64:
		return v.Text
	default:
		if v.Text2 != "" {
			return v.Text2
		}
		return v.Text
	}
}

// hiJobLog ACT_HI_JOBLOG（activity 级别起，镜像 Camunda 的记录点：
// created/succeeded/failed/deleted）。
func (k *Kernel) hiJobLog(j *persistence.Job, state, exception string) {
	if !k.hlAtLeast("activity") {
		return
	}
	h := &persistence.HiJobLog{
		ID: k.ctx.NextID(), Timestamp: k.now(), JobID: j.ID,
		JobRetries: j.Retries, JobDueDate: j.DueDate,
		JobException: exception, JobDefID: j.JobDefID, State: state,
	}
	k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiJobLog(tx, h) })
}

func (k *Kernel) hiIncidentCreate(inc *persistence.Incident) {
	if !k.hlAtLeast("activity") {
		return
	}
	h := &persistence.HiIncident{
		ID: inc.ID, CreateTime: inc.Timestamp, IncidentType: inc.Type, IncidentMsg: inc.Msg,
		ExecutionID: inc.ExecutionID, ActivityID: inc.ActivityID, ProcInstID: inc.ProcInstID,
		CauseIncidentID: inc.CauseIncidentID,
	}
	k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiIncident(tx, h) })
}

func (k *Kernel) hiIncidentEnd(incidentID string) {
	if !k.hlAtLeast("activity") {
		return
	}
	now := k.now()
	hists, err := persistence.SelectHiIncidents(k.ctx.Tx, "")
	if err != nil {
		return
	}
	for _, h := range hists {
		if h.ID == incidentID {
			h.EndTime = &now
			snapshot := *h
			k.ctx.AddHistory(func(tx *persistence.Tx) error { return persistence.InsertHiIncident(tx, &snapshot) })
			return
		}
	}
}

// procDefKey 从部署缓存取 key（失败容错）。
func (k *Kernel) procDefKey(procDefID string) string {
	def, err := k.deps.ProcDef(procDefID)
	if err != nil {
		return ""
	}
	return def.Key
}

var _ = time.Now
