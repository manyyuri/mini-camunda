// 事件系统：消息/信号订阅与触发、边界事件触发、定时器到期触发。
// 对应 org.camunda.bpm.engine.impl.bpmn.behavior 中各事件行为 + EventSubscriptionEntity。
package kernel

import (
	"fmt"

	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/errs"
	"mini-camunda/internal/persistence"
)

func (k *Kernel) createSubscription(e *persistence.Execution, act *bpmn.Activity, evType, name string) {
	s := &persistence.EventSubscription{
		ID: k.ctx.NextID(), EventType: evType, EventName: name,
		ExecutionID: e.ID, ProcInstID: e.ProcInstID, ActivityID: act.ID,
	}
	_ = persistence.InsertEventSubscription(k.ctx.Tx, s)
}

// Trigger 命令入口：手动触发等待中的执行（ReceiveTask 的 messageRef 场景 / RuntimeService.trigger）。
func (k *Kernel) Trigger(execID string, vars map[string]any) error {
	e, err := k.ctx.Exec(execID)
	if err != nil {
		return errs.NewNotFound("执行 %s", execID)
	}
	if !e.IsActive {
		return errs.NewConflict("执行 %s 不是活动执行（当前活动: %s）", execID, e.ActivityID)
	}
	// 删除该执行上的事件订阅（message catch / receive）
	subs, _ := persistence.SelectEventSubscriptionsByExecution(k.ctx.Tx, e.ID)
	for _, s := range subs {
		_ = persistence.DeleteEventSubscription(k.ctx.Tx, s.ID)
	}
	root, err := k.rootOf(e)
	if err != nil {
		return err
	}
	for n, v := range vars {
		k.setVariableOnScope(root, n, v, false)
	}
	if err := k.finishActivity(e); err != nil {
		return err
	}
	return k.Run()
}

// CorrelateMessage 命令入口：消息关联。
// 顺序：运行中订阅（按名字 + 可选 businessKey / correlationKeys 匹配）→ 无匹配则查 message 启动事件。
// 对应 CorrelateMessageCmd。
func (k *Kernel) CorrelateMessage(name, businessKey string, corrKeys, payload map[string]any) (int, error) {
	matched := 0
	subs, err := persistence.SelectEventSubscriptionsByName(k.ctx.Tx, "message", name)
	if err != nil {
		return 0, err
	}
	for _, s := range subs {
		if s.ExecutionID == "" {
			continue
		}
		e, err := k.ctx.Exec(s.ExecutionID)
		if err != nil || !e.IsActive {
			continue
		}
		root, err := k.rootOf(e)
		if err != nil {
			continue
		}
		// businessKey 过滤
		if businessKey != "" && root.BusinessKey != businessKey {
			continue
		}
		// correlationKeys 过滤：实例变量需全部相等
		if !k.matchCorrKeys(root, corrKeys) {
			continue
		}
		if err := k.deliverEvent(e, s, payload); err != nil {
			return matched, err
		}
		matched++
	}
	if matched > 0 {
		// 边界/推进产生的 ops 需要排空（fireBoundary* 只入队不驱动）
		return matched, k.Run()
	}
	// message 启动事件
	defs := k.deps.AllLatestProcDefs()
	for _, def := range defs {
		for _, start := range def.MessageStarts {
			if n, _ := start.Props[bpmn.PropMessageName].(string); n == name {
				if _, err := k.NewInstance(def, businessKey, payload, ""); err != nil {
					return 0, err
				}
				if err := k.Run(); err != nil {
					return 1, err
				}
				return 1, nil
			}
		}
	}
	return 0, errs.NewEngineError("Cannot correlate message '%s': no subscription or message start event found", name)
}

// BroadcastSignal 命令入口：信号广播（触发所有匹配订阅 + 所有 signal 启动事件）。
// 对应 SignalEventReceivedCmd 的 broadcast 语义。
func (k *Kernel) broadcastSignal(name string, payload map[string]any, source *persistence.Execution) error {
	subs, err := persistence.SelectEventSubscriptionsByName(k.ctx.Tx, "signal", name)
	if err != nil {
		return err
	}
	for _, s := range subs {
		if s.ExecutionID == "" {
			continue
		}
		e, err := k.ctx.Exec(s.ExecutionID)
		if err != nil || !e.IsActive {
			continue
		}
		if source != nil && e.ProcInstID == source.ProcInstID {
			// 简化：同实例内的信号订阅也会触发（Camunda 默认信号是广播语义）
		}
		if err := k.deliverEvent(e, s, payload); err != nil {
			return err
		}
	}
	// signal 启动事件：每个定义各启动一个实例
	started := false
	for _, def := range k.deps.AllLatestProcDefs() {
		for _, start := range def.SignalStarts {
			if n, _ := start.Props[bpmn.PropSignalName].(string); n == name {
				if _, err := k.NewInstance(def, "", payload, ""); err != nil {
					return err
				}
				started = true
			}
		}
	}
	if started || len(subs) > 0 {
		// 命令级入口（无外层 Run）：排空全部产生的 ops
		if source == nil {
			return k.Run()
		}
	}
	return nil
}

// BroadcastSignalCmd 服务入口。
func (k *Kernel) BroadcastSignal(name string, payload map[string]any) error {
	return k.broadcastSignal(name, payload, nil)
}

func (k *Kernel) matchCorrKeys(root *persistence.Execution, corrKeys map[string]any) bool {
	if len(corrKeys) == 0 {
		return true
	}
	vars := k.visibleVars(root)
	for name, want := range corrKeys {
		got, ok := vars[name]
		if !ok {
			return false
		}
		if gv, ok := got.(int64); ok {
			if wv, ok := want.(int); ok && int64(wv) == gv {
				continue
			}
		}
		if fmtAny(got) != fmtAny(want) {
			return false
		}
	}
	return true
}

func fmtAny(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return fmt.Sprintf("%d", t)
	case int64:
		return fmt.Sprintf("%d", t)
	case float64:
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// deliverEvent 向订阅所在执行投递事件：区分中间捕获（普通推进）与边界（中断/非中断）。
func (k *Kernel) deliverEvent(e *persistence.Execution, s *persistence.EventSubscription, payload map[string]any) error {
	_ = persistence.DeleteEventSubscription(k.ctx.Tx, s.ID)
	root, err := k.rootOf(e)
	if err != nil {
		return err
	}
	for n, v := range payload {
		k.setVariableOnScope(root, n, v, false)
	}
	def := k.defOf(e)
	act := def.Activities[s.ActivityID]

	// 边界事件：e 是宿主活动的事件作用域执行
	if act.Type == "boundaryEvent" {
		host := def.Activities[act.Props[bpmn.PropAttachedTo].(string)]
		if interrupting, _ := act.Props[bpmn.PropInterrupting].(bool); interrupting {
			return k.fireBoundaryInterrupting(e, host, act)
		}
		return k.fireBoundaryNonInterrupting(e, host, act)
	}

	// 中间捕获 / receiveTask：直接推进
	if err := k.finishActivity(e); err != nil {
		return err
	}
	return k.Run()
}

// fireBoundaryInterrupting 中断型边界：摧毁事件作用域子树，父令牌走边界出边。
func (k *Kernel) fireBoundaryInterrupting(E *persistence.Execution, host, b *bpmn.Activity) error {
	k.actInstEnd(E.ActInstID, "canceled")
	if err := k.deleteExecutionTree(E); err != nil {
		return err
	}
	parent, err := k.ctx.Exec(E.ParentID)
	if err != nil {
		return err
	}
	parent.IsActive = true
	ai := k.actInstStart(parent, b)
	parent.ActivityID, parent.ActInstID = b.ID, ai
	k.ctx.UpdateExec(parent)
	k.push(opTake, parent.ID, "", b.Outgoing[0].ID)
	return nil
}

// fireBoundaryNonInterrupting 非中断型边界：宿主继续，父作用域下新建并发令牌走边界出边。
func (k *Kernel) fireBoundaryNonInterrupting(E *persistence.Execution, host, b *bpmn.Activity) error {
	parent, err := k.ctx.Exec(E.ParentID)
	if err != nil {
		return err
	}
	c := &persistence.Execution{
		ID: k.ctx.NextID(), ProcInstID: E.ProcInstID, ProcDefID: E.ProcDefID,
		ParentID: parent.ID, IsConcurrent: true, IsActive: true,
	}
	ai := k.actInstStart(c, b)
	c.ActivityID, c.ActInstID = b.ID, ai
	k.ctx.InsertExec(c)
	k.push(opTake, c.ID, "", b.Outgoing[0].ID)
	return nil
}

// fireTimerBoundary 定时器边界到期（Job 执行器回调）。
func (k *Kernel) fireTimerBoundary(e *persistence.Execution, host, b *bpmn.Activity) error {
	if interrupting, _ := b.Props[bpmn.PropInterrupting].(bool); interrupting {
		return k.fireBoundaryInterrupting(e, host, b)
	}
	return k.fireBoundaryNonInterrupting(e, host, b)
}
