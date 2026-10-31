// Package el 表达式封装，对应 Camunda 的 JUEL ${...}（实现用 expr-lang）。
package el

import (
	"fmt"
	"strings"

	"github.com/expr-lang/expr"
)

// Evaluate 求值 ${expr}；非 ${} 包裹的原样返回（按字面量处理，支持转义）。
func Evaluate(expression string, vars map[string]any) (any, error) {
	s := strings.TrimSpace(expression)
	if !strings.HasPrefix(s, "${") {
		return s, nil // 字面量
	}
	s = strings.TrimSuffix(strings.TrimPrefix(s, "${"), "}")
	program, err := expr.Compile(s, expr.Env(vars), expr.AllowUndefinedVariables())
	if err != nil {
		return nil, fmt.Errorf("表达式编译失败 '%s': %w", expression, err)
	}
	out, err := expr.Run(program, vars)
	if err != nil {
		return nil, fmt.Errorf("表达式求值失败 '%s': %w", expression, err)
	}
	return out, nil
}

// EvaluateIntStrict 始终把内容当表达式求值并转 int（loopCardinality 等 BPMN 规定必须是表达式的场合）。
func EvaluateIntStrict(expression string, vars map[string]any) (int, error) {
	s := strings.TrimSpace(expression)
	s = strings.TrimSuffix(strings.TrimPrefix(s, "${"), "}")
	program, err := expr.Compile(s, expr.Env(vars), expr.AllowUndefinedVariables())
	if err != nil {
		return 0, fmt.Errorf("表达式编译失败 '%s': %w", expression, err)
	}
	out, err := expr.Run(program, vars)
	if err != nil {
		return 0, fmt.Errorf("表达式求值失败 '%s': %w", expression, err)
	}
	switch t := out.(type) {
	case int:
		return t, nil
	case int64:
		return int(t), nil
	case float64:
		return int(t), nil
	}
	return 0, fmt.Errorf("表达式 '%s' 结果不是整数: %T(%v)", expression, out, out)
}

// EvaluateBool 求值为布尔（网关条件等）。
func EvaluateBool(expression string, vars map[string]any) (bool, error) {
	out, err := Evaluate(expression, vars)
	if err != nil {
		return false, err
	}
	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("表达式 '%s' 结果不是布尔: %T(%v)", expression, out, out)
	}
	return b, nil
}

// EvaluateString 求值为字符串（assignee / event name 等）。
func EvaluateString(expression string, vars map[string]any) (string, error) {
	out, err := Evaluate(expression, vars)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", out), nil
}

// EvaluateInt 求值为整数（基数等）。
func EvaluateInt(expression string, vars map[string]any) (int, error) {
	out, err := Evaluate(expression, vars)
	if err != nil {
		return 0, err
	}
	switch t := out.(type) {
	case int:
		return t, nil
	case int64:
		return int(t), nil
	case float64:
		return int(t), nil
	}
	return 0, fmt.Errorf("表达式 '%s' 结果不是整数: %T(%v)", expression, out, out)
}
