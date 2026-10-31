// kernel 对外 API 入口（供 engine 命令层调用）。
package kernel

import (
	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/errs"
	"mini-camunda/internal/persistence"
)

// RootExec 取流程实例根执行（执行树根 = 实例本体）。
func (k *Kernel) RootExec(instID string) (*persistence.Execution, error) {
	return k.rootExec(instID)
}

// DeleteProcessInstance 运行实例级联终止（DeleteProcessInstanceCmd）：
// 清空该实例全部 RU 数据；HI PROCINST state=internally-terminated。
func (k *Kernel) DeleteProcessInstance(root *persistence.Execution, reason string) error {
	// 被调实例终止：调用方作用域也一并销毁（镜像 Camunda：子实例删除会连带取消调用点）
	if root.SuperExec != "" {
		callerScope, err := k.ctx.Exec(root.SuperExec)
		if err != nil {
			return err
		}
		def := k.defOf(callerScope)
		host := def.Activities[callerScope.ScopeActivity]
		if err := k.endProcessInstance(root, "internally-terminated", "", reason); err != nil {
			return err
		}
		k.actInstEnd(callerScope.ActInstID, "canceled")
		if err := k.deleteExecutionTree(callerScope); err != nil {
			return err
		}
		parent, err := k.ctx.Exec(callerScope.ParentID)
		if err != nil {
			return err
		}
		parent.IsActive = true
		parent.ActivityID = host.ID
		k.ctx.UpdateExec(parent)
		k.push(opTake, parent.ID, "", host.Outgoing[0].ID)
		return k.Run()
	}
	return k.endProcessInstance(root, "internally-terminated", "", reason)
}

// StartDefByKey 无缓存场景由 engine 层保证 def 已就绪；此入口保留扩展性。
func (k *Kernel) CorrelateByKey(key string) (*bpmn.ProcessDefinition, error) {
	def, err := k.deps.LatestProcDefByKey(key)
	if err != nil {
		return nil, errs.NewNotFound("流程定义 key=%s", key)
	}
	return def, nil
}
