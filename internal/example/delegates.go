// Package example 演示用 JavaDelegate 集合（对应 Camunda 示例工程里的 Delegate 类）。
// 约定 delegate 名：noop / fail / setVariable / flaky。
package example

import (
	"fmt"

	"mini-camunda/internal/delegate"
)

// Noop 什么也不做（演示自动直通活动）。
func Noop() delegate.JavaDelegate {
	return delegate.JavaDelegateFunc(func(e delegate.DelegateExecution) error {
		fmt.Printf("  [delegate noop] 执行活动 %s（实例 %s）\n", e.ActivityId(), e.ProcessInstanceId())
		return nil
	})
}

// Fail 总是抛技术错误 → 触发 Job 重试 / Incident（同步场景则命令失败）。
func Fail() delegate.JavaDelegate {
	return delegate.JavaDelegateFunc(func(e delegate.DelegateExecution) error {
		return fmt.Errorf("delegate 'fail' 被要求失败（活动 %s）", e.ActivityId())
	})
}

// SetVars 写一组固定变量（测试/演示用）。
func SetVars(m map[string]any) delegate.JavaDelegate {
	return delegate.JavaDelegateFunc(func(e delegate.DelegateExecution) error {
		for name, val := range m {
			e.SetVariable(name, val)
		}
		return nil
	})
}

// SetVariable 写一个变量（演示 DelegateExecution 的变量写入语义）。
func SetVariable() delegate.JavaDelegate {
	return delegate.JavaDelegateFunc(func(e delegate.DelegateExecution) error {
		val, _ := e.GetVariable("amount")
		fmt.Printf("  [delegate setVariable] 活动 %s：amount=%v → approved=true\n", e.ActivityId(), val)
		e.SetVariable("approved", true)
		return nil
	})
}

// ThrowBpmnError 抛业务错误 → 走 error 边界事件（不重试）。
func ThrowBpmnError(code, msg string) delegate.JavaDelegate {
	return delegate.JavaDelegateFunc(func(e delegate.DelegateExecution) error {
		fmt.Printf("  [delegate throwBpmnError] 活动 %s 抛出 BpmnError %s\n", e.ActivityId(), code)
		return &delegate.BpmnError{Code: code, Message: msg}
	})
}

// RiskCheck 金额风控：riskScore = amount / 1000。
func RiskCheck() delegate.JavaDelegate {
	return delegate.JavaDelegateFunc(func(e delegate.DelegateExecution) error {
		amount, _ := e.GetVariable("amount")
		var amt int64
		switch v := amount.(type) {
		case int64:
			amt = v
		case float64:
			amt = int64(v)
		case int:
			amt = int64(v)
		}
		score := amt / 1000
		fmt.Printf("  [delegate risk-check] 实例 %s：amount=%d → riskScore=%d\n", e.ProcessInstanceId(), amt, score)
		e.SetVariable("riskScore", score)
		return nil
	})
}

// RegisterDefaultDelegates 教学默认注册表。
func RegisterDefaultDelegates(cfg any) {}
