// 边界事件实体创建：进入带边界的活动时，在事件作用域执行上注册边界监听。
package kernel

import (
	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/persistence"
)

// createBoundaryEntities 为作用域执行挂上全部边界事件的运行时实体。
// 定时器边界 → timer-boundary Job；消息/信号边界 → 事件订阅（EXECUTION_ID=作用域执行）。
func (k *Kernel) createBoundaryEntities(s *persistence.Execution, host *bpmn.Activity) error {
	for _, b := range k.boundaries(host) {
		interrupting, _ := b.Props[bpmn.PropInterrupting].(bool)
		if t, ok := b.Props[bpmn.PropTimer].(*bpmn.TimerDef); ok {
			if err := k.createTimerJob(s, b, t, "timer-boundary", timerCfg{
				AttachedTo: host.ID, Interrupting: interrupting,
			}); err != nil {
				return err
			}
			continue
		}
		if name, ok := b.Props[bpmn.PropSignalName].(string); ok {
			k.createSubscription(s, b, "signal", name)
			continue
		}
		if name, ok := b.Props[bpmn.PropMessageName].(string); ok {
			k.createSubscription(s, b, "message", name)
		}
		// error 边界无实体：错误传播是同步行为（propagateError）
	}
	return nil
}
