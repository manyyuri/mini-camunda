// Package variable 镜像 org.camunda.bpm.engine.impl.variable：
// 类型化变量（ValueFields 持久化形态）与 Go 值互转。
package variable

import (
	"encoding/json"
	"fmt"
	"time"

	"mini-camunda/internal/persistence"
)

// ToVariable 把 Go 值转成持久化变量。
// 类型映射：bool→boolean int64→integer float64→double string→string
//          time.Time→date 其余→json（对应 Camunda 的序列化 ObjectValue，mini 统一 JSON）。
func ToVariable(id, procInstID, execID, taskID, name string, value any, local bool) *persistence.Variable {
	v := &persistence.Variable{ID: id, Name: name, ProcInstID: procInstID, ExecutionID: execID, TaskID: taskID, IsLocal: local}
	switch t := value.(type) {
	case nil:
		v.Type = "null"
	case bool:
		v.Type = "boolean"
		v.Long = new(int64)
		*v.Long = btoi(t)
	case int:
		v.Type = "integer"
		v.Long = new(int64)
		*v.Long = int64(t)
	case int32:
		v.Type = "integer"
		v.Long = new(int64)
		*v.Long = int64(t)
	case int64:
		v.Type = "integer"
		v.Long = new(int64)
		*v.Long = t
	case float64:
		v.Type = "double"
		v.Double = new(float64)
		*v.Double = t
	case float32:
		v.Type = "double"
		v.Double = new(float64)
		*v.Double = float64(t)
	case string:
		v.Type = "string"
		v.Text = t
	case time.Time:
		v.Type = "date"
		v.Text = persistence.FormatTime(t)
	case []byte:
		v.Type = "bytes"
		v.Text2 = string(t) // 简化：bytes 存 TEXT2_，注释说明与 Camunda 存字节表的差异
	default:
		v.Type = "json"
		b, _ := json.Marshal(value)
		v.Text2 = string(b)
	}
	return v
}

func btoi(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// ToValue 从持久化变量还原 Go 值。
func ToValue(v *persistence.Variable) any {
	switch v.Type {
	case "null":
		return nil
	case "boolean":
		return v.Long != nil && *v.Long != 0
	case "integer":
		if v.Long == nil {
			return int64(0)
		}
		return *v.Long
	case "double":
		if v.Double == nil {
			return float64(0)
		}
		return *v.Double
	case "string":
		return v.Text
	case "date":
		return persistence.MustParseTime(v.Text)
	case "bytes":
		return []byte(v.Text2)
	case "json":
		var out any
		if err := json.Unmarshal([]byte(v.Text2), &out); err != nil {
			return v.Text2
		}
		return out
	}
	return v.Text
}

// TypeName 用于历史/REST 展示。
func TypeName(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case int, int32, int64:
		return "integer"
	case float32, float64:
		return "double"
	case string:
		return "string"
	case time.Time:
		return "date"
	case []byte:
		return "bytes"
	default:
		return "json"
	}
}

// Normalize REST 入参（JSON number 默认 float64）规范化为 int64（整数时）。
func Normalize(value any) any {
	switch t := value.(type) {
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
		return t
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		f, _ := t.Float64()
		return f
	case map[string]any, []any:
		return t
	default:
		return t
	}
}

func Describe(v *persistence.Variable) string {
	val := ToValue(v)
	return fmt.Sprintf("%s(%s)=%v", v.Name, v.Type, val)
}
