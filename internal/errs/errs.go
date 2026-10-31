// Package errs 引擎错误类型（REST 与命令层共用）。
package errs

import "fmt"

// NotFound 对应 404：找不到实体。
type NotFound struct{ What string }

func (e *NotFound) Error() string { return "Not found: " + e.What }

func NewNotFound(format string, args ...any) error {
	return &NotFound{What: fmt.Sprintf(format, args...)}
}

// BadArgument 对应 400：参数错误。
type BadArgument struct{ Msg string }

func (e *BadArgument) Error() string { return "Bad argument: " + e.Msg }

func NewBadArgument(format string, args ...any) error {
	return &BadArgument{Msg: fmt.Sprintf(format, args...)}
}

// Conflict 对应 409：状态冲突（如重复 claim）。
type Conflict struct{ Msg string }

func (e *Conflict) Error() string { return "Conflict: " + e.Msg }

func NewConflict(format string, args ...any) error {
	return &Conflict{Msg: fmt.Sprintf(format, args...)}
}

// EngineError 其它引擎错误（400/500）。
type EngineError struct{ Msg string }

func (e *EngineError) Error() string { return e.Msg }

func NewEngineError(format string, args ...any) error {
	return &EngineError{Msg: fmt.Sprintf(format, args...)}
}
