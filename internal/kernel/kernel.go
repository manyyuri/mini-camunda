// Package kernel 对应 org.camunda.bpm.engine.impl.pvm.runtime + bpmn.behavior + impl.cmd 的核心：
// 执行树操作、原子操作队列、BPMN 元素行为、事件传播、Job 恢复。
//
// 核心不变量（镜像 Camunda）：
//   - 一个命令 = 一个事务；自动活动链在一个命令里推进到下一个等待态
//   - IS_ACTIVE_=1 的执行 = 存活的令牌；作用域执行持有令牌时活动、被令牌越过时非活动
//   - 并行 fork 创建并发子执行，join 收拢；作用域（子流程/调用活动/多实例体/带边界活动）各自建作用域执行
package kernel

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/commandctx"
	"mini-camunda/internal/delegate"
	"mini-camunda/internal/el"
	"mini-camunda/internal/errs"
	"mini-camunda/internal/persistence"
	"mini-camunda/internal/variable"
)

// Deps 引擎环境（由 engine 包注入，避免循环引用）。
type Deps interface {
	NextID() string
	HistoryLevel() string
	Delegate(name string) (delegate.JavaDelegate, bool)
	ProcDef(id string) (*bpmn.ProcessDefinition, error)
	LatestProcDefByKey(key string) (*bpmn.ProcessDefinition, error)
	AllLatestProcDefs() []*bpmn.ProcessDefinition // 每个流程 key 的最新版本（消息/信号启动匹配用）
	NotifyExtTask()                              // 外部任务长轮询唤醒
}

// Kernel 单命令内的执行运行时。
type Kernel struct {
	ctx  *commandctx.CommandContext
	deps Deps

	// 原子操作队列（简化版 AtomicOperation 链：opExec → behavior → opTake → ...）
	ops []op

	// 本命令内创建的活动实例缓存（actInstID → HiActInst），用于结束时回写
	actInsts map[string]*persistence.HiActInst

	// Job 执行时遇到的技术错误（由外层转成重试/Incident）
	handlerErr error
}

type op struct {
	kind        int // opExec / opTake / opFinish
	execID      string
	actID       string
	transID     string
	miInstance  bool // 多实例内层实例执行（不再建作用域、不走 MI 初始化）
	resumeAsync bool // asyncBefore Job 恢复（不再建 Job）
}

const (
	opExec = 1
	opTake = 2
	opFinish = 3
)

func New(ctx *commandctx.CommandContext, deps Deps) *Kernel {
	return &Kernel{ctx: ctx, deps: deps, actInsts: map[string]*persistence.HiActInst{}}
}

func (k *Kernel) now() time.Time { return time.Now() }

func (k *Kernel) defOf(e *persistence.Execution) *bpmn.ProcessDefinition {
	def, err := k.deps.ProcDef(e.ProcDefID)
	if err != nil {
		panic(fmt.Sprintf("加载流程定义失败 %s: %v", e.ProcDefID, err))
	}
	return def
}

// Run 排空操作队列：自动链推进到等待态或结束。
func (k *Kernel) Run() error {
	for len(k.ops) > 0 {
		o := k.ops[0]
		k.ops = k.ops[1:]
		var err error
		switch o.kind {
		case opExec:
			err = k.execActivity(o)
		case opTake:
			err = k.takeTransition(o)
		case opFinish:
			err = k.execFinish(o.execID)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// =====================================================================
// 实例生命周期
// =====================================================================

// NewInstance 创建流程实例（根执行 = 流程实例 id，镜像 Camunda）并推进到第一个等待态。
func (k *Kernel) NewInstance(def *bpmn.ProcessDefinition, businessKey string, vars map[string]any, superExecID string) (*persistence.Execution, error) {
	instID := k.ctx.NextID()
	root := &persistence.Execution{
		ID: instID, ProcInstID: instID, ProcDefID: def.ID, BusinessKey: businessKey,
		IsActive: true, IsScope: true, SuperExec: superExecID,
	}
	k.ctx.InsertExec(root)
	k.hiProcInstStart(root, def)
	for name, v := range vars {
		k.setVariableOnScope(root, name, v, false)
	}
	k.push(opExec, root.ID, def.Initial.ID, "")
	return root, nil
}

// StartInstance 命令入口：新建实例并 Run。
func (k *Kernel) StartInstance(def *bpmn.ProcessDefinition, businessKey string, vars map[string]any) (*persistence.Execution, error) {
	root, err := k.NewInstance(def, businessKey, vars, "")
	if err != nil {
		return nil, err
	}
	if err := k.Run(); err != nil {
		return nil, err
	}
	return root, nil
}

// =====================================================================
// 原子操作：执行活动
// =====================================================================

func (k *Kernel) push(kind int, execID, actID, transID string) {
	k.ops = append(k.ops, op{kind: kind, execID: execID, actID: actID, transID: transID})
}

func (k *Kernel) pushOp(o op) { k.ops = append(k.ops, o) }

func (k *Kernel) execActivity(o op) error {
	e, err := k.ctx.Exec(o.execID)
	if err != nil {
		return err
	}
	def := k.defOf(e)
	act, ok := def.Activities[o.actID]
	if !ok {
		return errs.NewEngineError("流程定义 %s 中不存在活动 %s", def.Key, o.actID)
	}

	// ---- asyncBefore：到达即创建 Job 并停（事务提交点）----
	if ab, _ := act.Props[bpmn.PropAsyncBefore].(bool); ab && !o.resumeAsync {
		ai := k.actInstStart(e, act)
		e.ActivityID, e.ActInstID = act.ID, ai
		k.ctx.UpdateExec(e)
		k.createAsyncJob(e, act, "before")
		return nil // 等待态
	}

	// ---- 作用域执行创建 ----
	if !o.miInstance && k.needsScope(act) {
		s := &persistence.Execution{
			ID: k.ctx.NextID(), ProcInstID: e.ProcInstID, ProcDefID: e.ProcDefID,
			ParentID: e.ID, ScopeActivity: act.ID,
			IsActive: true, IsScope: true, IsEventScope: len(k.boundaries(act)) > 0,
		}
		ai := k.actInstStart(s, act)
		s.ActivityID, s.ActInstID = act.ID, ai
		k.ctx.InsertExec(s)
		// 边界事件实体：定时器边界 → timer Job；消息/信号边界 → 事件订阅
		if err := k.createBoundaryEntities(s, act); err != nil {
			return err
		}
		e.IsActive = false // 令牌转移到子作用域执行
		k.ctx.UpdateExec(e)
		e = s
	} else {
		ai := k.actInstStart(e, act)
		e.ActivityID, e.ActInstID = act.ID, ai
		k.ctx.UpdateExec(e)
	}

	// ---- 多实例初始化 ----
	if mi, ok := act.Props[bpmn.PropMI].(*bpmn.MultiInstanceDef); ok && !o.miInstance {
		return k.miInit(e, act, mi)
	}

	return k.dispatchBehavior(e, act)
}

// needsScope 哪些活动需要自己的作用域执行。
func (k *Kernel) needsScope(act *bpmn.Activity) bool {
	if act.Type == "subProcess" || act.Type == "callActivity" {
		return true
	}
	if _, ok := act.Props[bpmn.PropMI]; ok {
		return true
	}
	return len(k.boundaries(act)) > 0
}

func (k *Kernel) boundaries(act *bpmn.Activity) []*bpmn.Activity {
	bs, _ := act.Props["boundaries"].([]*bpmn.Activity)
	return bs
}

func (k *Kernel) dispatchBehavior(e *persistence.Execution, act *bpmn.Activity) error {
	switch act.Type {
	case "startEvent", "manualTask":
		return k.finishActivity(e)

	case "intermediateThrowEvent":
		if act.Props["startKind"] == "signal-throw" {
			name, _ := act.Props[bpmn.PropSignalName].(string)
			if err := k.broadcastSignal(name, nil, e); err != nil {
				return err
			}
		}
		return k.finishActivity(e)

	case "endEvent":
		return k.endEvent(e, act)

	case "userTask":
		k.createUserTask(e, act)
		return nil // 等待态

	case "serviceTask", "sendTask":
		if topic, ok := act.Props[bpmn.PropTopic].(string); ok {
			k.createExtTask(e, act, topic)
			return nil // 等待态（外部 worker 拉取）
		}
		return k.runDelegate(e, act)

	case "receiveTask", "intermediateCatchEvent":
		if t, ok := act.Props[bpmn.PropTimer].(*bpmn.TimerDef); ok {
			if err := k.createTimerJob(e, act, t, "timer-catch", timerCfg{}); err != nil {
				return err
			}
			return nil // 等待态
		}
		if name, ok := act.Props[bpmn.PropSignalName].(string); ok {
			k.createSubscription(e, act, "signal", name)
			return nil
		}
		name, _ := act.Props[bpmn.PropMessageName].(string)
		k.createSubscription(e, act, "message", name)
		return nil

	case "exclusiveGateway":
		return k.exclusiveGateway(e, act)

	case "parallelGateway":
		return k.parallelGateway(e, act)

	case "subProcess":
		initialID := act.Props[bpmn.PropInitialID].(string)
		k.push(opExec, e.ID, initialID, "")
		return nil

	case "callActivity":
		return k.callActivity(e, act)
	}
	return errs.NewEngineError("不支持的活动类型: %s (%s)", act.Type, act.ID)
}

// =====================================================================
// 网关
// =====================================================================

func (k *Kernel) visibleVars(e *persistence.Execution) map[string]any {
	chain := []*persistence.Execution{}
	for cur := e; cur != nil; {
		chain = append([]*persistence.Execution{cur}, chain...)
		if cur.ParentID == "" {
			break
		}
		p, err := k.ctx.Exec(cur.ParentID)
		if err != nil {
			break
		}
		cur = p
	}
	out := map[string]any{}
	// 从根往下覆盖：靠近令牌的作用域遮蔽外层
	for i := len(chain) - 1; i >= 0; i-- {
		vars, err := k.ctx.VariablesByExecution(chain[i].ID)
		if err != nil {
			continue
		}
		for _, v := range vars {
			if v.TaskID == "" {
				out[v.Name] = variable.ToValue(v)
			}
		}
	}
	return out
}

func (k *Kernel) exclusiveGateway(e *persistence.Execution, act *bpmn.Activity) error {
	vars := k.visibleVars(e)
	for _, tr := range act.Outgoing {
		if tr.IsDefault || tr.Condition == "" {
			continue
		}
		ok, err := el.EvaluateBool(tr.Condition, vars)
		if err != nil {
			return err
		}
		if ok {
			k.push(opTake, e.ID, "", tr.ID)
			return nil
		}
	}
	for _, tr := range act.Outgoing {
		if tr.IsDefault {
			k.push(opTake, e.ID, "", tr.ID)
			return nil
		}
	}
	return errs.NewEngineError("No outgoing sequence flow found on exclusive gateway '%s': all conditions evaluated to false and there is no default flow", act.ID)
}

func (k *Kernel) parallelGateway(e *persistence.Execution, act *bpmn.Activity) error {
	if len(act.Outgoing) >= 2 {
		// fork：在当前作用域执行下创建并发子执行（对应 ConcurrentExecution 创建）
		e.IsActive = false
		k.ctx.UpdateExec(e)
		for _, tr := range act.Outgoing {
			child := &persistence.Execution{
				ID: k.ctx.NextID(), ProcInstID: e.ProcInstID, ProcDefID: e.ProcDefID,
				ParentID: e.ID, IsConcurrent: true, IsActive: true,
			}
			// 子执行从网关出发（takeTransition 依据 ActivityID 查出边）
			child.ActivityID = act.ID
			k.ctx.InsertExec(child)
			k.push(opTake, child.ID, "", tr.ID)
		}
		return nil
	}
	if len(act.Incoming) >= 2 {
		// join：本令牌到达，等待其余
		e.ActivityID = act.ID
		k.ctx.UpdateExec(e)
		return k.tryJoin(e, act)
	}
	// 1-1 直通
	k.push(opTake, e.ID, "", act.Outgoing[0].ID)
	return nil
}

// tryJoin 检查 join 是否所有入边到齐；到齐则收拢并发执行、重新激活作用域执行。
func (k *Kernel) tryJoin(arrived *persistence.Execution, act *bpmn.Activity) error {
	parent, err := k.ctx.Exec(arrived.ParentID)
	if err != nil {
		return err
	}
	children, err := k.ctx.ExecsByParent(parent.ID)
	if err != nil {
		return err
	}
	var waiting []*persistence.Execution
	for _, c := range children {
		if c.IsActive && c.ActivityID == act.ID {
			waiting = append(waiting, c)
		}
	}
	if len(waiting) < len(act.Incoming) {
		return nil // 等待其余分支
	}
	// 收拢：删并发执行，作用域执行重新持有令牌
	for _, c := range waiting {
		k.ctx.DeleteExec(c)
	}
	parent.IsActive = true
	ai := k.actInstStart(parent, act)
	k.actInstEnd(ai, "completed")
	parent.ActivityID, parent.ActInstID = act.ID, ai
	k.ctx.UpdateExec(parent)
	k.push(opTake, parent.ID, "", act.Outgoing[0].ID)
	return nil
}

// =====================================================================
// 流转与活动收尾
// =====================================================================

func (k *Kernel) takeTransition(o op) error {
	e, err := k.ctx.Exec(o.execID)
	if err != nil {
		return err
	}
	def := k.defOf(e)
	var tr *bpmn.Transition
	if e.ActivityID != "" {
		for _, t := range def.Activities[e.ActivityID].Outgoing {
			if t.ID == o.transID {
				tr = t
				break
			}
		}
	}
	if tr == nil {
		return errs.NewEngineError("执行 %s 找不到出边 %s（当前活动 %s）", e.ID, o.transID, e.ActivityID)
	}
	// 离开当前活动：结束其活动实例（若还在 running）
	if e.ActivityID != "" && e.ActInstID != "" {
		k.actInstEnd(e.ActInstID, "completed")
	}
	// sequenceFlow 活动实例（Camunda 在历史中会记录 sequenceFlow 类型活动实例）
	k.recordSequenceFlow(e, tr)
	e.ActivityID, e.ActInstID = "", ""
	k.ctx.UpdateExec(e)
	k.push(opExec, e.ID, tr.TargetID, "")
	return nil
}

// execFinish 由 opFinish 进入：等待态被触发后的统一收尾（asyncAfter 恢复 / timer 捕获到期 / ReceiveTask 触发）。
func (k *Kernel) execFinish(execID string) error {
	e, err := k.ctx.Exec(execID)
	if err != nil {
		return err
	}
	return k.finishActivity(e)
}

// finishActivity 活动行为执行完毕：处理 MI 实例完成 / asyncAfter / 作用域销毁 / 走出边。
func (k *Kernel) finishActivity(e *persistence.Execution) error {
	def := k.defOf(e)
	act := def.Activities[e.ActivityID]

	// 1. 多实例内层实例完成？
	if e.ParentID != "" {
		parent, err := k.ctx.Exec(e.ParentID)
		if err != nil {
			return err
		}
		if mi, ok := act.Props[bpmn.PropMI].(*bpmn.MultiInstanceDef); ok && parent.IsScope && parent.ScopeActivity == act.ID {
			return k.miInstanceComplete(e, parent, act, mi)
		}
	}

	// 2. asyncAfter：离开前创建 Job 并停（事务提交点）
	if aa, _ := act.Props[bpmn.PropAsyncAfter].(bool); aa {
		k.createAsyncJob(e, act, "after")
		return nil
	}

	// 3. 作用域销毁（带边界的活动：userTask/serviceTask/receive 等的事件作用域）
	if e.IsScope && e.ScopeActivity == act.ID && act.Type != "subProcess" {
		k.actInstEnd(e.ActInstID, "completed")
		if err := k.destroyEventScope(e); err != nil {
			return err
		}
		parent, err := k.ctx.Exec(e.ParentID)
		if err != nil {
			return err
		}
		parent.IsActive = true
		parent.ActivityID = act.ID
		k.ctx.UpdateExec(parent)
		k.push(opTake, parent.ID, "", act.Outgoing[0].ID)
		return nil
	}

	// 4. 普通收尾
	k.actInstEnd(e.ActInstID, "completed")
	k.push(opTake, e.ID, "", act.Outgoing[0].ID)
	return nil
}

// destroyEventScope 销毁事件作用域执行：删除其上的边界 Job / 事件订阅与子树。
func (k *Kernel) destroyEventScope(e *persistence.Execution) error {
	return k.deleteExecutionTree(e)
}

// =====================================================================
// 结束事件
// =====================================================================

func (k *Kernel) endEvent(e *persistence.Execution, act *bpmn.Activity) error {
	k.actInstEnd(e.ActInstID, "completed")

	switch act.Props["endKind"] {
	case "terminate":
		// Camunda 7 语义：terminate 结束整个流程实例（含调用活动子实例自身）
		root, err := k.rootOf(e)
		if err != nil {
			return err
		}
		return k.endProcessInstance(root, "internally-terminated", act.ID, act.ID)
	case "error":
		code, _ := act.Props[bpmn.PropErrorCode].(string)
		return k.propagateError(e, act, code)
	}

	// none end
	if e.IsScope && e.ScopeActivity != "" {
		def := k.defOf(e)
		if host, ok := def.Activities[e.ScopeActivity]; ok && host.Type == "subProcess" {
			return k.endSubprocessScope(e, host, act.ID)
		}
	}
	if e.SuperExec != "" {
		// 被调实例根执行结束 → 调用活动收尾
		return k.finishCallActivity(e, act.ID)
	}
	if e.ParentID == "" {
		return k.endProcessInstance(e, "completed", act.ID, "")
	}
	// 并发分支上的普通结束：令牌消亡；若所在作用域再无活动令牌 → 作用域隐式结束
	k.ctx.DeleteExec(e)
	return k.maybeEndScope(e.ParentID, act.ID)
}

// maybeEndScope 分支令牌全部消亡后，隐式结束所在作用域。
func (k *Kernel) maybeEndScope(scopeID, endActID string) error {
	if scopeID == "" {
		return nil
	}
	s, err := k.ctx.Exec(scopeID)
	if err != nil {
		return err
	}
	if s.IsActive {
		return nil // 作用域自身还持有令牌（非 fork 场景）
	}
	children, err := k.ctx.ExecsByParent(s.ID)
	if err != nil {
		return err
	}
	for _, c := range children {
		if c.IsActive {
			return nil // 还有存活令牌
		}
	}
	def := k.defOf(s)
	if s.SuperExec != "" {
		return k.finishCallActivity(s, endActID)
	}
	if s.IsScope && s.ScopeActivity != "" {
		if host, ok := def.Activities[s.ScopeActivity]; ok {
			if host.Type == "subProcess" {
				return k.endSubprocessScope(s, host, endActID)
			}
			if _, isMI := host.Props[bpmn.PropMI]; isMI {
				return nil // MI 体由 miInstanceComplete 管理
			}
		}
	}
	if s.ParentID == "" {
		return k.endProcessInstance(s, "completed", endActID, "")
	}
	return nil
}

// endSubprocessScope 子流程作用域结束：销毁子树，父令牌继续。
func (k *Kernel) endSubprocessScope(s *persistence.Execution, sub *bpmn.Activity, endActID string) error {
	k.actInstEnd(s.ActInstID, "completed")
	if err := k.deleteExecutionTree(s); err != nil {
		return err
	}
	parent, err := k.ctx.Exec(s.ParentID)
	if err != nil {
		return err
	}
	parent.IsActive = true
	parent.ActivityID = sub.ID
	k.ctx.UpdateExec(parent)
	k.push(opTake, parent.ID, "", sub.Outgoing[0].ID)
	return nil
}

// =====================================================================
// 调用活动（callActivity）
// =====================================================================

func (k *Kernel) callActivity(e *persistence.Execution, act *bpmn.Activity) error {
	// e 是调用活动的作用域执行；启动被调实例（SUPER_EXEC_ 连接）
	callee, err := k.deps.LatestProcDefByKey(act.Props[bpmn.PropCalledElement].(string))
	if err != nil {
		return err
	}
	var vars map[string]any = map[string]any{}
	outer := k.visibleVars(e)
	for _, m := range act.Props[bpmn.PropInMapping].([]bpmn.Mapping) {
		var val any
		if m.SourceExpression != "" {
			val, err = el.Evaluate(m.SourceExpression, outer)
			if err != nil {
				return err
			}
		} else {
			if v, ok := outer[m.Source]; ok {
				val = v
			}
		}
		vars[firstNonEmpty(m.Target, m.Source)] = val
	}
	root, err := k.NewInstance(callee, "", vars, e.ID)
	if err != nil {
		return err
	}
	_ = root
	return nil // 被调实例的 ops 已入队；调用方等待
}

// finishCallActivity 被调实例根执行结束：out 映射 → 销毁调用活动作用域 → 调用方继续。
func (k *Kernel) finishCallActivity(calleeRoot *persistence.Execution, endActID string) error {
	callerScope, err := k.ctx.Exec(calleeRoot.SuperExec)
	if err != nil {
		return err
	}
	def := k.defOf(callerScope)
	act := def.Activities[callerScope.ScopeActivity]

	// camunda:out：子根变量 → 调用方作用域
	calleeVars := k.visibleVars(calleeRoot)
	for _, m := range act.Props[bpmn.PropOutMapping].([]bpmn.Mapping) {
		var val any
		if m.SourceExpression != "" {
			val, err = el.Evaluate(m.SourceExpression, calleeVars)
			if err != nil {
				return err
			}
		} else {
			if v, ok := calleeVars[m.Source]; ok {
				val = v
			}
		}
		target, err := k.ctx.Exec(callerScope.ParentID)
		if err != nil {
			return err
		}
		k.setVariableOnScope(target, firstNonEmpty(m.Target, m.Source), val, m.Local)
	}

	// 结束被调实例
	k.endProcessInstance(calleeRoot, "completed", endActID, "")

	// 销毁调用活动作用域执行，父令牌继续
	k.actInstEnd(callerScope.ActInstID, "completed")
	if err := k.deleteExecutionTree(callerScope); err != nil {
		return err
	}
	parent, err := k.ctx.Exec(callerScope.ParentID)
	if err != nil {
		return err
	}
	parent.IsActive = true
	parent.ActivityID = act.ID
	k.ctx.UpdateExec(parent)
	k.push(opTake, parent.ID, "", act.Outgoing[0].ID)
	return nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// =====================================================================
// 错误传播（errorEndEvent / delegate BpmnError）
// =====================================================================

// propagateError 沿执行树向上冒泡，匹配最近的 error 边界；未匹配到根则实例 errored 结束。
// 对应 org.camunda.bpm.engine.impl.bpmn.behavior.ErrorEndEventActivityBehavior 的传播逻辑。
func (k *Kernel) propagateError(e *persistence.Execution, errAct *bpmn.Activity, code string) error {
	cur := e
	for cur != nil {
		if cur.IsScope && cur.ScopeActivity != "" {
			def := k.defOf(cur)
			if host, ok := def.Activities[cur.ScopeActivity]; ok {
				for _, b := range k.boundaries(host) {
					if b.Type != "boundaryEvent" || !hasErrorDef(b) {
						continue
					}
					bCode, _ := b.Props[bpmn.PropErrorCode].(string)
					// catch-all（无 errorRef）匹配任意错误码
					if bCode == "" || bCode == code {
						return k.fireBoundaryInterrupting(cur, host, b)
					}
				}
			}
		}
		if cur.ParentID != "" {
			p, err := k.ctx.Exec(cur.ParentID)
			if err != nil {
				return err
			}
			cur = p
		} else if cur.SuperExec != "" {
			// 跨调用活动传播：被调实例整体结束，错误继续在调用方冒泡
			callerScope, err := k.ctx.Exec(cur.SuperExec)
			if err != nil {
				return err
			}
			k.endProcessInstance(cur, "errored", errAct.ID, "uncaught error "+code)
			cur = callerScope
		} else {
			break
		}
	}
	// 未捕获：实例 errored 结束（不产生 Incident，与 Camunda 一致）
	root, err := k.rootOf(e)
	if err != nil {
		return err
	}
	return k.endProcessInstance(root, "errored", errAct.ID, "uncaught error "+code)
}

func hasErrorDef(b *bpmn.Activity) bool {
	_, hasRef := b.Props[bpmn.PropErrorRef]
	_, hasCode := b.Props[bpmn.PropErrorCode]
	return hasRef || hasCode
}

// =====================================================================
// 实例结束 / 终止 / 级联删除
// =====================================================================

func (k *Kernel) rootOf(e *persistence.Execution) (*persistence.Execution, error) {
	cur := e
	for cur.ParentID != "" {
		p, err := k.ctx.Exec(cur.ParentID)
		if err != nil {
			return nil, err
		}
		cur = p
	}
	return cur, nil
}

// endProcessInstance 流程实例结束：级联清理 RU 数据 + 写历史。
func (k *Kernel) endProcessInstance(root *persistence.Execution, state, endActID, deleteReason string) error {
	instID := root.ProcInstID
	def := k.defOf(root)

	// 1. 任务 → HI end + 删除（走 ctx：命令内新建的任务也要可见）
	tasks, err := k.ctx.TasksByInstance(instID)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		k.hiTaskEnd(t.ID, deleteReason)
		k.ctx.DeleteTask(t)
	}
	// 2. Job → HI deleted + 删除
	jobs, err := k.ctx.JobsByInstance(instID)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		k.hiJobLog(j, "deleted", "")
		k.ctx.DeleteJob(j)
	}
	// 3. 事件订阅 / 外部任务
	subs, err := persistence.SelectEventSubscriptionsByInstance(k.ctx.Tx, instID)
	if err != nil {
		return err
	}
	for _, s := range subs {
		_ = persistence.DeleteEventSubscription(k.ctx.Tx, s.ID)
	}
	exts, err := persistence.SelectExtTasks(k.ctx.Tx, "")
	if err != nil {
		return err
	}
	for _, et := range exts {
		if et.ProcInstID == instID {
			_ = persistence.DeleteExtTask(k.ctx.Tx, et.ID)
		}
	}
	// 4. 变量 HI deleted + 删除
	vars, err := persistence.SelectVariablesByProcInstance(k.ctx.Tx, instID)
	if err != nil {
		return err
	}
	for _, v := range vars {
		k.hiVarInst(v, "deleted")
		k.ctx.DeleteVar(v)
	}
	// 5. 执行树删除
	execs, err := k.ctx.ExecsByInstance(instID)
	if err != nil {
		return err
	}
	for _, e := range execs {
		k.ctx.DeleteExec(e)
	}
	// 6. 未结束的活动实例 → canceled
	if state != "completed" {
		running, err := persistence.SelectHiActInsts(k.ctx.Tx, instID)
		if err == nil {
			for _, h := range running {
				if h.ActInstState == "running" {
					k.actInstEnd(h.ID, "canceled")
				}
			}
		}
	}
	// 7. Incident 清理（HI 保留）
	incidents, _ := persistence.SelectIncidents(k.ctx.Tx, instID)
	for _, inc := range incidents {
		k.hiIncidentEnd(inc.ID)
		_ = persistence.DeleteIncident(k.ctx.Tx, inc.ID)
	}
	// 8. HI procinst end
	k.hiProcInstEnd(root, def, state, endActID, deleteReason)
	return nil
}

// deleteExecutionTree 级联删除执行子树及其运行时实体（任务/Job/订阅/外部任务/局部变量）。
func (k *Kernel) deleteExecutionTree(e *persistence.Execution) error {
	// 收集子树
	var all []*persistence.Execution
	queue := []*persistence.Execution{e}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		all = append(all, cur)
		children, err := k.ctx.ExecsByParent(cur.ID)
		if err != nil {
			return err
		}
		queue = append(queue, children...)
	}
	for _, x := range all {
		// 任务
		tasks, _ := k.ctx.TasksByInstance(x.ProcInstID)
		for _, t := range tasks {
			if t.ExecutionID == x.ID {
				k.hiTaskEnd(t.ID, "")
				k.ctx.DeleteTask(t)
			}
		}
		// Job
		jobs, _ := k.ctx.JobsByInstance(x.ProcInstID)
		for _, j := range jobs {
			if j.ExecutionID == x.ID {
				k.hiJobLog(j, "deleted", "")
				k.ctx.DeleteJob(j)
			}
		}
		// 事件订阅（含边界订阅）
		subs, _ := persistence.SelectEventSubscriptionsByExecution(k.ctx.Tx, x.ID)
		for _, s := range subs {
			_ = persistence.DeleteEventSubscription(k.ctx.Tx, s.ID)
		}
		// 外部任务
		exts, _ := persistence.SelectExtTasks(k.ctx.Tx, "")
		for _, et := range exts {
			if et.ExecutionID == x.ID {
				_ = persistence.DeleteExtTask(k.ctx.Tx, et.ID)
			}
		}
		// 局部变量（挂在执行上的）
		vars, _ := k.ctx.VariablesByExecution(x.ID)
		for _, v := range vars {
			k.hiVarInst(v, "deleted")
			k.ctx.DeleteVar(v)
		}
		k.ctx.DeleteExec(x)
	}
	return nil
}

// =====================================================================
// Delegate 执行
// =====================================================================

type delegateExecutionImpl struct {
	k *Kernel
	e *persistence.Execution
}

func (d *delegateExecutionImpl) ProcessInstanceId() string    { return d.e.ProcInstID }
func (d *delegateExecutionImpl) ProcessDefinitionId() string  { return d.e.ProcDefID }
func (d *delegateExecutionImpl) ActivityId() string           { return d.e.ActivityID }
func (d *delegateExecutionImpl) CurrentActivityName() string {
	def := d.k.defOf(d.e)
	if a, ok := def.Activities[d.e.ActivityID]; ok {
		return a.Name
	}
	return ""
}
func (d *delegateExecutionImpl) GetVariable(name string) (any, bool) {
	vars := d.k.visibleVars(d.e)
	v, ok := vars[name]
	return v, ok
}
func (d *delegateExecutionImpl) SetVariable(name string, value any) {
	root, err := d.k.rootOf(d.e)
	if err != nil {
		return
	}
	d.k.setVariableOnScope(root, name, value, false)
}
func (d *delegateExecutionImpl) SetVariableLocal(name string, value any) {
	d.k.setVariableOnScope(d.e, name, value, true)
}

func (k *Kernel) runDelegate(e *persistence.Execution, act *bpmn.Activity) error {
	name, _ := act.Props[bpmn.PropDelegate].(string)
	d, ok := k.deps.Delegate(name)
	if !ok {
		return errs.NewEngineError("serviceTask %s: 未注册的 delegate '%s'（camunda:class）", act.ID, name)
	}
	if err := d.Execute(&delegateExecutionImpl{k: k, e: e}); err != nil {
		if be, isBpmn := err.(*delegate.BpmnError); isBpmn {
			// 业务异常：按 error 边界传播（不重试）
			return k.propagateError(e, act, be.Code)
		}
		// 技术错误：命令失败回滚 → Job 场景由外层 JobFailedCmd 重试
		return fmt.Errorf("delegate '%s' 执行失败 (%s %s): %w", name, act.Type, act.ID, err)
	}
	return k.finishActivity(e)
}

// =====================================================================
// 变量
// =====================================================================

func (k *Kernel) setVariableOnScope(scope *persistence.Execution, name string, value any, local bool) {
	existing, err := k.ctx.VariablesByExecution(scope.ID)
	if err == nil {
		for _, v := range existing {
			if v.Name == name && v.TaskID == "" {
				nv := variable.ToVariable(v.ID, v.ProcInstID, v.ExecutionID, "", name, value, local)
				nv.Rev = v.Rev
				k.ctx.UpdateVar(nv)
				k.hiVarInst(nv, "updated")
				return
			}
		}
	}
	v := variable.ToVariable(k.ctx.NextID(), scope.ProcInstID, scope.ID, "", name, value, local)
	k.ctx.InsertVar(v)
	k.hiVarInst(v, "created")
}

// SetInstanceVariable 命令入口（REST / RuntimeService）：写实例级变量。
func (k *Kernel) SetInstanceVariable(instID, name string, value any) error {
	execs, err := k.ctx.ExecsByInstance(instID)
	if err != nil {
		return err
	}
	for _, e := range execs {
		if e.ParentID == "" {
			k.setVariableOnScope(e, name, value, false)
			return nil
		}
	}
	return errs.NewNotFound("流程实例 %s", instID)
}

// =====================================================================
// 工具
// =====================================================================

func (k *Kernel) rootExec(instID string) (*persistence.Execution, error) {
	execs, err := k.ctx.ExecsByInstance(instID)
	if err != nil {
		return nil, err
	}
	for _, e := range execs {
		if e.ParentID == "" {
			return e, nil
		}
	}
	return nil, errs.NewNotFound("流程实例 %s 的根执行", instID)
}

func isNoRows(err error) bool { return err == sql.ErrNoRows }

func (k *Kernel) jsonCfg(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func trimErr(msg string) string {
	msg = strings.TrimSpace(msg)
	if len(msg) > 500 {
		return msg[:500]
	}
	return msg
}
