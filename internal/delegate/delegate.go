// Package delegate 镜像 org.camunda.bpm.engine.delegate：
// JavaDelegate（Go 版）与 DelegateExecution。
package delegate

// DelegateExecution 对应 org.camunda.bpm.engine.delegate.DelegateExecution。
type DelegateExecution interface {
	ProcessInstanceId() string
	ProcessDefinitionId() string
	ActivityId() string
	CurrentActivityName() string
	GetVariable(name string) (any, bool)
	SetVariable(name string, value any)  // 写流程实例级（上溯到根作用域）
	SetVariableLocal(name string, value any)
}

// JavaDelegate 对应 org.camunda.bpm.engine.delegate.JavaDelegate。
// BPMN 里 camunda:class="name" → 注册表按 name 取实现。
type JavaDelegate interface {
	Execute(e DelegateExecution) error
}

// JavaDelegateFunc 函数式便捷包装。
type JavaDelegateFunc func(e DelegateExecution) error

func (f JavaDelegateFunc) Execute(e DelegateExecution) error { return f(e) }

// BpmnError 对应 org.camunda.bpm.engine.delegate.BpmnError：
// 业务异常 → 按 error 边界事件传播（不触发 Job 重试）；
// 普通技术 error → Job 重试 / Incident。
type BpmnError struct {
	Code    string
	Message string
}

func (e *BpmnError) Error() string {
	if e.Code != "" {
		return "BpmnError[code=" + e.Code + "]: " + e.Message
	}
	return "BpmnError: " + e.Message
}
