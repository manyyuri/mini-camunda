// 多实例（Multi-Instance）：对应 MultiInstanceActivityBehavior。
// 结构：体作用域执行（S_MI，ScopeActivity=活动id）下挂 N 个实例执行；
// 并行 = N 个并发执行同时活动；顺序 = 单执行循环。
// 完成数记录在体作用域执行局部变量 nrOfInstances / nrOfCompletedInstances / nrOfActiveInstances。
package kernel

import (
	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/el"
	"mini-camunda/internal/errs"
	"mini-camunda/internal/persistence"
)

func (k *Kernel) miInit(s *persistence.Execution, act *bpmn.Activity, mi *bpmn.MultiInstanceDef) error {
	vars := k.visibleVars(s)
	n := 0
	elements := []any{}
	if mi.CollectionExpr != "" {
		out, err := el.Evaluate(mi.CollectionExpr, vars)
		if err != nil {
			return err
		}
		switch list := out.(type) {
		case []any:
			elements = list
		case []string:
			for _, x := range list {
				elements = append(elements, x)
			}
		default:
			return errs.NewEngineError("多实例 %s: collection 表达式结果必须是数组: %T", act.ID, out)
		}
		n = len(elements)
	} else {
		var err error
		n, err = el.EvaluateIntStrict(mi.CardinalityExpr, vars)
		if err != nil {
			return err
		}
		if n < 0 {
			return errs.NewEngineError("多实例 %s: 基数不能为负: %d", act.ID, n)
		}
	}

	k.setVariableOnScope(s, "nrOfInstances", int64(n), true)
	k.setVariableOnScope(s, "nrOfCompletedInstances", int64(0), true)
	k.setVariableOnScope(s, "nrOfActiveInstances", int64(n), true)

	if n == 0 {
		// 空实例直接完成
		return k.finishActivity(s)
	}

	if mi.Parallel {
		for i := 0; i < n; i++ {
			child := &persistence.Execution{
				ID: k.ctx.NextID(), ProcInstID: s.ProcInstID, ProcDefID: s.ProcDefID,
				ParentID: s.ID, IsConcurrent: true, IsActive: true,
			}
			k.ctx.InsertExec(child)
			k.setVariableOnScope(child, "loopCounter", int64(i), true)
			if mi.ElementVariable != "" && mi.CollectionExpr != "" {
				k.setVariableOnScope(child, mi.ElementVariable, elements[i], true)
			}
			k.pushOp(op{kind: opExec, execID: child.ID, actID: act.ID, miInstance: true})
		}
		return nil
	}
	// 顺序：先跑第 0 个
	k.setVariableOnScope(s, "miLoopCounter", int64(0), true)
	child := &persistence.Execution{
		ID: k.ctx.NextID(), ProcInstID: s.ProcInstID, ProcDefID: s.ProcDefID,
		ParentID: s.ID, IsActive: true,
	}
	k.ctx.InsertExec(child)
	k.setVariableOnScope(child, "loopCounter", int64(0), true)
	if mi.ElementVariable != "" && mi.CollectionExpr != "" {
		k.setVariableOnScope(child, mi.ElementVariable, elements[0], true)
	}
	k.pushOp(op{kind: opExec, execID: child.ID, actID: act.ID, miInstance: true})
	return nil
}

// miInstanceComplete 单个实例完成：更新计数、判断完成条件 / 继续下一个 / 全部完成收尾。
func (k *Kernel) miInstanceComplete(child, s *persistence.Execution, act *bpmn.Activity, mi *bpmn.MultiInstanceDef) error {
	miVars := k.visibleVars(s)
	completed := toInt(miVars["nrOfCompletedInstances"]) + 1
	total := toInt(miVars["nrOfInstances"])
	k.setVariableOnScope(s, "nrOfCompletedInstances", int64(completed), true)

	// 完成条件（并行场景可提前退出，镜像 Camunda）
	if mi.CompletionCondition != "" {
		condVars := k.visibleVars(child)
		// 覆盖计数（条件里引用 nrOfCompletedInstances 等标准变量）
		condVars["nrOfCompletedInstances"] = int64(completed)
		condVars["nrOfInstances"] = int64(total)
		condVars["nrOfActiveInstances"] = int64(total - completed)
		if ok, err := el.EvaluateBool(mi.CompletionCondition, condVars); err == nil && ok {
			return k.finishMIBody(s, act, child)
		}
	}

	if mi.Parallel {
		k.ctx.DeleteExec(child)
		if completed >= total {
			return k.finishMIBody(s, act, nil)
		}
		return nil
	}

	// 顺序：还有下一个 → 复用推进
	if completed < total {
		next := completed // 下标
		k.ctx.DeleteExec(child)
		k.setVariableOnScope(s, "miLoopCounter", int64(next), true)
		nchild := &persistence.Execution{
			ID: k.ctx.NextID(), ProcInstID: s.ProcInstID, ProcDefID: s.ProcDefID,
			ParentID: s.ID, IsActive: true,
		}
		k.ctx.InsertExec(nchild)
		k.setVariableOnScope(nchild, "loopCounter", int64(next), true)
		if mi.ElementVariable != "" && mi.CollectionExpr != "" {
			outer, err := el.Evaluate(mi.CollectionExpr, k.visibleVars(s))
			if err != nil {
				return err
			}
			if list, ok := outer.([]any); ok && next < len(list) {
				k.setVariableOnScope(nchild, mi.ElementVariable, list[next], true)
			}
		}
		k.pushOp(op{kind: opExec, execID: nchild.ID, actID: act.ID, miInstance: true})
		return nil
	}
	k.ctx.DeleteExec(child)
	return k.finishMIBody(s, act, nil)
}

// finishMIBody 全部实例完成（或条件提前满足）：结束体作用域、父令牌继续。
func (k *Kernel) finishMIBody(s *persistence.Execution, act *bpmn.Activity, lastChild *persistence.Execution) error {
	// asyncAfter on MI 体
	if aa, _ := act.Props[bpmn.PropAsyncAfter].(bool); aa {
		// 已无活动实例，Job 挂在体执行上，恢复时走 opFinish
		k.createAsyncJob(s, act, "after")
		return nil
	}
	return k.miScopeEnd(s, act)
}

func (k *Kernel) miScopeEnd(s *persistence.Execution, act *bpmn.Activity) error {
	k.actInstEnd(s.ActInstID, "completed")
	if err := k.deleteExecutionTree(s); err != nil {
		return err
	}
	parent, err := k.ctx.Exec(s.ParentID)
	if err != nil {
		return err
	}
	parent.IsActive = true
	parent.ActivityID = act.ID
	k.ctx.UpdateExec(parent)
	k.push(opTake, parent.ID, "", act.Outgoing[0].ID)
	return nil
}

func toInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	}
	return 0
}
