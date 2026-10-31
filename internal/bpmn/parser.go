// Package bpmn 镜像 org.camunda.bpm.engine.impl.bpmn.parser：
// BPMN XML 解析 → 校验 → PVM（Process Virtual Machine）流程定义模型。
// 说明：解析范围之外（未知元素）一律报错，禁止静默忽略。
package bpmn

import (
	"encoding/xml"
	"fmt"
	"strings"
)

// ===== PVM 模型（对应 org.camunda.bpm.engine.impl.pvm.*）=====

// ProcessDefinition 对应 PvmProcessDefinition。
type ProcessDefinition struct {
	ID            string // {key}:{version}:{random}
	Key           string // BPMN process id
	Version       int
	DeploymentID  string
	ResourceName  string
	Activities    map[string]*Activity
	Initial       *Activity
	StartEvents   []*Activity // 含 message/signal start，启动入口
	MessageStarts []*Activity // messageStartEvent
	SignalStarts  []*Activity // signalStartEvent
	TimerStarts   []*Activity // timerStartEvent
}

// Activity 对应 PvmActivity。Type 为 mini 支持的元素类型。
type Activity struct {
	ID, Name, Type string
	Incoming       []*Transition
	Outgoing       []*Transition
	Props          map[string]any
}

// Transition 对应 PvmTransition（sequence flow）。
type Transition struct {
	ID, SourceID, TargetID string
	Condition              string // ${...}，空为无条件
	IsDefault              bool   // 网关默认流
}

// TimerDef 定时器定义。
type TimerDef struct {
	Kind string // date | duration | cycle
	Expr string // 原始表达式（可为 ${}）
}

// MultiInstanceDef 多实例定义。
type MultiInstanceDef struct {
	Parallel            bool
	CardinalityExpr     string // loopCardinality
	CollectionExpr      string // camunda:collection
	ElementVariable     string // camunda:elementVariable
	CompletionCondition string
}

// Mapping callActivity 的 camunda:in / camunda:out。
type Mapping struct {
	Source           string
	SourceExpression string
	Target           string
	Local            bool
}

// 常用 Props 键
const (
	PropParent          = "parent"          // 父作用域活动 id（子流程内活动的容器），进程级为空
	PropSubprocess      = "subprocess"      // 是否子流程
	PropInitialID       = "initial"         // 子流程内的初始活动 id
	PropDelegate        = "delegate"        // camunda:class
	PropTopic           = "topic"           // camunda:topic
	PropAssignee        = "assignee"
	PropCandidateUsers  = "candidateUsers"  // []string
	PropCandidateGroups = "candidateGroups" // []string
	PropFormKey         = "formKey"
	PropAsyncBefore     = "asyncBefore"
	PropAsyncAfter      = "asyncAfter"
	PropRetryCycle      = "retryCycle" // R3/PT5M
	PropTimer           = "*bpmn.TimerDef"
	PropMessageName     = "messageName"
	PropSignalName      = "signalName"
	PropCalledElement   = "calledElement"
	PropInMapping       = "inMapping"  // []Mapping
	PropOutMapping      = "outMapping" // []Mapping
	PropErrorCode       = "errorCode"  // error end / boundary
	PropErrorRef        = "errorRef"
	PropInterrupting    = "interrupting" // boundary
	PropAttachedTo      = "attachedTo"   // boundary
	PropMI              = "mi"           // *MultiInstanceDef
	PropDefault         = "default"      // gateway 默认流 id
)

// ===== DOM 解析 =====

type node struct {
	XMLName  xml.Name
	Attrs    []xml.Attr
	Children []*node
	Text     string
}

func (n *node) Local() string { return n.XMLName.Local }

func (n *node) attr(name string) (string, bool) {
	for _, a := range n.Attrs {
		if a.Name.Local == name && (a.Name.Space == "" || strings.Contains(a.Name.Space, "camunda") || a.Name.Space == "http://www.w3.org/2001/XMLSchema-instance") {
			return a.Value, true
		}
	}
	return "", false
}

// camundaAttr 取 camunda:* 扩展属性。
func (n *node) camundaAttr(name string) (string, bool) {
	for _, a := range n.Attrs {
		if a.Name.Local == name && strings.Contains(a.Name.Space, "camunda.org") {
			return a.Value, true
		}
	}
	return "", false
}

func (n *node) child(name string) *node {
	for _, c := range n.Children {
		if c.Local() == name {
			return c
		}
	}
	return nil
}

func (n *node) children(name string) []*node {
	var out []*node
	for _, c := range n.Children {
		if c.Local() == name {
			out = append(out, c)
		}
	}
	return out
}

func (n *node) textOf(name string) string {
	if c := n.child(name); c != nil {
		return strings.TrimSpace(c.Text)
	}
	return ""
}

// ParseError 对应 BpmnParseException。
type ParseError struct{ Msg string }

func (e *ParseError) Error() string { return "BpmnParseException: " + e.Msg }

func perr(format string, args ...any) error {
	return &ParseError{Msg: fmt.Sprintf(format, args...)}
}

// Parse 解析 BPMN XML → ProcessDefinition（未定版本的裸定义；版本/id 由部署命令赋值）。
func Parse(xmlBytes []byte, resourceName string) (*ProcessDefinition, error) {
	root, err := parseDOM(xmlBytes)
	if err != nil {
		return nil, perr("%s: XML 解析失败: %v", resourceName, err)
	}
	if root.Local() != "definitions" {
		return nil, perr("%s: 根元素必须是 definitions", resourceName)
	}

	// 顶层引用表：error / message / signal
	errorCodes := map[string]string{}   // errorId -> errorCode
	messageNames := map[string]string{} // messageId -> name
	signalNames := map[string]string{}  // signalId -> name
	for _, n := range root.Children {
		switch n.Local() {
		case "error":
			id, _ := n.attr("id")
			code, _ := n.attr("errorCode")
			errorCodes[id] = code
		case "message":
			id, _ := n.attr("id")
			name, _ := n.attr("name")
			if name == "" {
				name = id
			}
			messageNames[id] = name
		case "signal":
			id, _ := n.attr("id")
			name, _ := n.attr("name")
			if name == "" {
				name = id
			}
			signalNames[id] = name
		}
	}

	procs := root.children("process")
	if len(procs) == 0 {
		return nil, perr("%s: 缺少 process 元素", resourceName)
	}
	if len(procs) > 1 {
		return nil, perr("%s: mini-camunda 每个文件只支持一个 process（%d 个）", resourceName, len(procs))
	}
	proc := procs[0]
	key, _ := proc.attr("id")
	if key == "" {
		return nil, perr("%s: process 缺少 id", resourceName)
	}

	def := &ProcessDefinition{
		Key:           key,
		ResourceName:  resourceName,
		Activities:    map[string]*Activity{},
	}
	if err := parseFlowElements(def, proc, key, "", messageNames, signalNames, errorCodes); err != nil {
		return nil, err
	}
	return def, nil
}

// parseFlowElements 递归解析（子流程嵌套）。
func parseFlowElements(def *ProcessDefinition, container *node, procKey, parentActID string, messages, signals, errors map[string]string) error {
	acts := map[string]*Activity{}
	var flows []*node

	// 第一遍：活动 & 边界事件
	for _, n := range container.Children {
		switch n.Local() {
		case "startEvent", "endEvent", "userTask", "serviceTask", "sendTask", "receiveTask",
			"manualTask", "scriptTask", "exclusiveGateway", "parallelGateway", "inclusiveGateway",
			"subProcess", "callActivity", "intermediateCatchEvent", "intermediateThrowEvent":
			a, err := parseActivity(def, n, procKey, parentActID, messages, signals, errors)
			if err != nil {
				return err
			}
			if _, dup := acts[a.ID]; dup {
				return perr("活动 id 重复: %s", a.ID)
			}
			acts[a.ID] = a
		case "boundaryEvent":
			a, err := parseActivity(def, n, procKey, parentActID, messages, signals, errors)
			if err != nil {
				return err
			}
			acts[a.ID] = a
		case "sequenceFlow":
			flows = append(flows, n)
		case "text", "documentation", "extensionElements", "laneSet", "dataObject", "association", "annotation":
			// 忽略
		default:
			return perr("mini-camunda 不支持的元素: <%s>（活动 id 上下文: %s）", n.Local(), procKey)
		}
	}

	// 边界事件挂到被围活动
	for _, a := range acts {
		if a.Type != "boundaryEvent" {
			continue
		}
		targetID := a.Props[PropAttachedTo].(string)
		host, ok := acts[targetID]
		if !ok {
			return perr("boundaryEvent %s 的 attachedToRef=%s 不存在", a.ID, targetID)
		}
		var bs []*Activity
		if cur, ok := host.Props["boundaries"].([]*Activity); ok {
			bs = cur
		}
		host.Props["boundaries"] = append(bs, a)
	}

	// sequence flow 建边
	for _, f := range flows {
		id, _ := f.attr("id")
		src, _ := f.attr("sourceRef")
		tgt, _ := f.attr("targetRef")
		if id == "" || src == "" || tgt == "" {
			return perr("sequenceFlow 缺少 id/sourceRef/targetRef: %s", id)
		}
		srcAct, ok := acts[src]
		if !ok {
			return perr("sequenceFlow %s 的 sourceRef=%s 不存在", id, src)
		}
		tgtAct, ok := acts[tgt]
		if !ok {
			return perr("sequenceFlow %s 的 targetRef=%s 不存在", id, tgt)
		}
		tr := &Transition{ID: id, SourceID: src, TargetID: tgt}
		if cond := f.child("conditionExpression"); cond != nil {
			tr.Condition = strings.TrimSpace(cond.Text)
		}
		srcAct.Outgoing = append(srcAct.Outgoing, tr)
		tgtAct.Incoming = append(tgtAct.Incoming, tr)
	}

	// 网关默认流
	for _, n := range container.Children {
		switch n.Local() {
		case "exclusiveGateway", "parallelGateway", "inclusiveGateway":
			id, _ := n.attr("id")
			defFlow, has := n.attr("default")
			if has && defFlow != "" {
				found := false
				for _, tr := range acts[id].Outgoing {
					if tr.ID == defFlow {
						tr.IsDefault = true
						found = true
					}
				}
				if !found {
					return perr("网关 %s 的 default=%s 不是其出边", id, defFlow)
				}
			}
		}
	}

	// 汇入 def（子流程活动也平铺进全表）
	for _, a := range acts {
		def.Activities[a.ID] = a
	}

	// 确定初始活动（本作用域内无入边的 startEvent）
	if parentActID == "" {
		for _, a := range acts {
			if a.Type == "startEvent" && len(a.Incoming) == 0 {
				def.Initial = a
				def.StartEvents = append(def.StartEvents, a)
				switch a.Props["startKind"] {
				case "message":
					def.MessageStarts = append(def.MessageStarts, a)
				case "signal":
					def.SignalStarts = append(def.SignalStarts, a)
				case "timer":
					def.TimerStarts = append(def.TimerStarts, a)
				}
			}
		}
		if def.Initial == nil {
			return perr("process %s 缺少无入边的 startEvent", procKey)
		}
	} else {
		// 子流程内部初始活动
		var initial *Activity
		for _, a := range acts {
			if a.Type == "startEvent" && len(a.Incoming) == 0 {
				initial = a
				break
			}
		}
		if initial == nil {
			return perr("subProcess %s 内缺少 startEvent", parentActID)
		}
		def.Activities[parentActID].Props[PropInitialID] = initial.ID
	}
	return nil
}

func parseActivity(def *ProcessDefinition, n *node, procKey, parentActID string, messages, signals, errors map[string]string) (*Activity, error) {
	id, _ := n.attr("id")
	if id == "" {
		return nil, perr("%s 缺少 id", n.Local())
	}
	name, _ := n.attr("name")
	a := &Activity{ID: id, Name: name, Type: n.Local(), Props: map[string]any{
		PropParent: parentActID,
		"startKind": "none",
	}}

	if v, ok := n.camundaAttr("class"); ok {
		a.Props[PropDelegate] = v
	}
	if v, ok := n.camundaAttr("topic"); ok {
		a.Props[PropTopic] = v
	}
	if v, ok := n.camundaAttr("assignee"); ok {
		a.Props[PropAssignee] = v
	}
	if v, ok := n.camundaAttr("candidateUsers"); ok {
		a.Props[PropCandidateUsers] = strings.Split(v, ",")
	}
	if v, ok := n.camundaAttr("candidateGroups"); ok {
		a.Props[PropCandidateGroups] = strings.Split(v, ",")
	}
	if v, ok := n.camundaAttr("formKey"); ok {
		a.Props[PropFormKey] = v
	}
	if v, ok := n.camundaAttr("asyncBefore"); ok && (v == "true" || v == "1") {
		a.Props[PropAsyncBefore] = true
	}
	if v, ok := n.camundaAttr("asyncAfter"); ok && (v == "true" || v == "1") {
		a.Props[PropAsyncAfter] = true
	}
	// receiveTask：messageRef 指向的消息（Camunda 语义）
	if a.Type == "receiveTask" {
		if ref, ok := n.attr("messageRef"); ok && ref != "" {
			name, found := messages[ref]
			if !found {
				return nil, perr("receiveTask %s 的 messageRef=%s 未定义", id, ref)
			}
			a.Props[PropMessageName] = name
		}
	}

	switch a.Type {
	case "startEvent", "intermediateCatchEvent", "endEvent", "intermediateThrowEvent", "boundaryEvent":
		if err := parseEvents(a, n, messages, signals, errors); err != nil {
			return nil, err
		}
	case "serviceTask", "sendTask":
		if _, hasDelegate := a.Props[PropDelegate]; !hasDelegate {
			if _, hasTopic := a.Props[PropTopic]; !hasTopic {
				return nil, perr("serviceTask/sendTask %s 缺少实现：需要 camunda:class 或 camunda:topic", id)
			}
		}
		if _, ok := n.camundaAttr("expression"); ok {
			return nil, perr("serviceTask %s: mini 不支持 camunda:expression，请用 camunda:class", id)
		}
		if _, ok := n.camundaAttr("delegateExpression"); ok {
			return nil, perr("serviceTask %s: mini 不支持 camunda:delegateExpression，请用 camunda:class", id)
		}
	case "scriptTask":
		return nil, perr("scriptTask %s: mini-camunda 未实现 scriptTask", id)
	case "callActivity":
		ce, _ := n.attr("calledElement")
		if ce == "" {
			return nil, perr("callActivity %s 缺少 calledElement", id)
		}
		a.Props[PropCalledElement] = ce
		var in, out []Mapping
		if ext := n.child("extensionElements"); ext != nil {
			for _, m := range ext.Children {
				src, _ := m.camundaAttr("source")
				srcExpr, _ := m.camundaAttr("sourceExpression")
				tgt, _ := m.camundaAttr("target")
				local := false
				if lv, ok := m.camundaAttr("local"); ok && lv == "true" {
					local = true
				}
				switch m.Local() {
				case "in":
					in = append(in, Mapping{Source: src, SourceExpression: srcExpr, Target: tgt, Local: local})
				case "out":
					out = append(out, Mapping{Source: src, SourceExpression: srcExpr, Target: tgt, Local: local})
				default:
					return nil, perr("callActivity %s: 不支持的扩展元素 camunda:%s", id, m.Local())
				}
			}
		}
		a.Props[PropInMapping] = in
		a.Props[PropOutMapping] = out
	case "subProcess":
		a.Props[PropSubprocess] = true
		// 先注册自身（子活动解析会回填 PropInitialID）
		def.Activities[a.ID] = a
		// 递归解析子流程内部
		if err := parseFlowElements(def, n, procKey, id, messages, signals, errors); err != nil {
			return nil, err
		}
	}

	// 多实例（任务级）
	if mi := n.child("multiInstanceLoopCharacteristics"); mi != nil {
		if a.Type == "subProcess" || a.Type == "callActivity" {
			return nil, perr("%s %s: mini 不支持子流程/调用活动上的多实例", a.Type, id)
		}
		isSeq := false
		if v, ok := mi.attr("isSequential"); ok && v == "true" {
			isSeq = true
		}
		d := &MultiInstanceDef{Parallel: !isSeq}
		if card := mi.textOf("loopCardinality"); card != "" {
			d.CardinalityExpr = card
		}
		if ext := mi.child("extensionElements"); ext != nil {
			for _, e := range ext.Children {
				switch e.Local() {
				case "collection":
					d.CollectionExpr, _ = e.camundaAttr("collection")
					d.CollectionExpr = firstNonEmpty(d.CollectionExpr, strings.TrimSpace(e.Text))
				case "elementVariable":
					d.ElementVariable, _ = e.camundaAttr("elementVariable")
					d.ElementVariable = firstNonEmpty(d.ElementVariable, strings.TrimSpace(e.Text))
				}
			}
		}
		if cc := mi.textOf("completionCondition"); cc != "" {
			d.CompletionCondition = cc
		}
		if d.CardinalityExpr == "" && d.CollectionExpr == "" {
			return nil, perr("%s %s: 多实例需要 loopCardinality 或 camunda:collection", a.Type, id)
		}
		a.Props[PropMI] = d
	}

	// failedJobRetryTimeCycle（扩展元素里）
	if ext := n.child("extensionElements"); ext != nil {
		for _, e := range ext.Children {
			if e.Local() == "failedJobRetryTimeCycle" {
				v, _ := e.camundaAttr("value")
				if v == "" {
					v = strings.TrimSpace(e.Text)
				}
				a.Props[PropRetryCycle] = v
			}
		}
	}
	return a, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func parseEvents(a *Activity, n *node, messages, signals, errors map[string]string) error {
	id := a.ID
	var defs []*node
	for _, c := range n.Children {
		switch c.Local() {
		case "timerEventDefinition", "messageEventDefinition", "signalEventDefinition", "errorEventDefinition", "terminateEventDefinition":
			defs = append(defs, c)
		}
	}
	switch a.Type {
	case "startEvent", "intermediateCatchEvent", "intermediateThrowEvent", "boundaryEvent":
		if len(defs) == 0 {
			if a.Type == "intermediateCatchEvent" {
				return perr("%s %s 缺少事件定义（timer/message/signal）", a.Type, id)
			}
			return nil // none start / throw / 无边界定义不允许但边界单独校验
		}
		if len(defs) > 1 {
			return perr("%s %s 只支持一个事件定义", a.Type, id)
		}
	case "endEvent":
		if len(defs) == 0 {
			return nil // none end
		}
		if len(defs) > 1 {
			return perr("endEvent %s 只支持一个事件定义", id)
		}
	}
	if len(defs) == 0 {
		return nil
	}
	d := defs[0]
	switch d.Local() {
	case "timerEventDefinition":
		t := &TimerDef{}
		switch {
		case d.child("timeDate") != nil:
			t.Kind, t.Expr = "date", d.textOf("timeDate")
		case d.child("timeDuration") != nil:
			t.Kind, t.Expr = "duration", d.textOf("timeDuration")
		case d.child("timeCycle") != nil:
			t.Kind, t.Expr = "cycle", d.textOf("timeCycle")
		default:
			return perr("timerEventDefinition %s 缺少 timeDate/timeDuration/timeCycle", id)
		}
		if t.Expr == "" {
			return perr("timerEventDefinition %s 的时间表达式为空", id)
		}
		a.Props[PropTimer] = t
	case "messageEventDefinition":
		ref, _ := d.attr("messageRef")
		name := id // 缺省回退：活动 id
		if ref != "" {
			if v, ok := messages[ref]; ok {
				name = v
			} else {
				return perr("%s %s 的 messageRef=%s 未定义", a.Type, id, ref)
			}
		}
		a.Props[PropMessageName] = name
	case "signalEventDefinition":
		ref, _ := d.attr("signalRef")
		name := id
		if ref != "" {
			if v, ok := signals[ref]; ok {
				name = v
			} else {
				return perr("%s %s 的 signalRef=%s 未定义", a.Type, id, ref)
			}
		}
		a.Props[PropSignalName] = name
	case "errorEventDefinition":
		ref, _ := d.attr("errorRef")
		if ref != "" {
			code, ok := errors[ref]
			if !ok {
				return perr("%s %s 的 errorRef=%s 未定义", a.Type, id, ref)
			}
			a.Props[PropErrorRef] = ref
			a.Props[PropErrorCode] = code
		}
		// 无 errorRef = catch-all
	case "terminateEventDefinition":
		// 类型信息足够
	}
	if a.Type == "boundaryEvent" {
		if _, hasErr := d.attr("errorRef"); d.Local() == "errorEventDefinition" && hasErr == false && a.Props[PropErrorRef] == nil {
			// catch-all error 边界合法
		}
		attachedTo, _ := n.attr("attachedToRef")
		if attachedTo == "" {
			return perr("boundaryEvent %s 缺少 attachedToRef", id)
		}
		a.Props[PropAttachedTo] = attachedTo
		interrupting := true
		if v, ok := n.attr("cancelActivity"); ok && v == "false" {
			interrupting = false
			if d.Local() == "errorEventDefinition" {
				return perr("boundaryEvent %s: error 边界只支持 interrupting", id)
			}
		}
		a.Props[PropInterrupting] = interrupting
	}
	// start/throw 的 kind 标注
	switch {
	case a.Type == "startEvent" || a.Type == "intermediateCatchEvent":
		if _, ok := a.Props[PropTimer]; ok {
			a.Props["startKind"] = "timer"
		} else if _, ok := a.Props[PropMessageName]; ok {
			a.Props["startKind"] = "message"
		} else if _, ok := a.Props[PropSignalName]; ok {
			a.Props["startKind"] = "signal"
		}
	case a.Type == "intermediateThrowEvent":
		if _, ok := a.Props[PropSignalName]; ok {
			a.Props["startKind"] = "signal-throw"
		}
	case a.Type == "endEvent":
		if _, ok := a.Props[PropErrorCode]; ok || d.Local() == "errorEventDefinition" {
			a.Props["endKind"] = "error"
		} else if d.Local() == "terminateEventDefinition" {
			a.Props["endKind"] = "terminate"
		}
	}
	return nil
}

// Validate 结构校验（部署时调用）。
func Validate(def *ProcessDefinition) error {
	for _, a := range def.Activities {
		switch a.Type {
		case "startEvent":
			if len(a.Incoming) != 0 {
				return perr("startEvent %s 不允许有入边", a.ID)
			}
			if len(a.Outgoing) != 1 {
				return perr("startEvent %s 必须恰好一条出边（实际 %d）", a.ID, len(a.Outgoing))
			}
		case "endEvent":
			if len(a.Outgoing) != 0 {
				return perr("endEvent %s 不允许有出边", a.ID)
			}
		case "exclusiveGateway":
			if len(a.Outgoing) > 0 {
				hasDefault := false
				condCount := 0
				for _, tr := range a.Outgoing {
					if tr.IsDefault {
						hasDefault = true
					} else if tr.Condition != "" {
						condCount++
					} else {
						return perr("exclusiveGateway %s 的出边 %s 缺少条件（且不是默认流）", a.ID, tr.ID)
					}
				}
				if condCount == 0 && !hasDefault {
					return perr("exclusiveGateway %s 的出边既无条件也无默认流", a.ID)
				}
			}
		case "parallelGateway":
			if len(a.Incoming) > 1 && len(a.Outgoing) > 1 {
				return perr("parallelGateway %s 不能同时是 fork 和 join", a.ID)
			}
		case "inclusiveGateway":
			return perr("inclusiveGateway %s: mini-camunda 未实现包容网关（见 README 简化说明）", a.ID)
		case "userTask", "serviceTask", "sendTask", "receiveTask", "manualTask":
			if len(a.Outgoing) != 1 {
				return perr("%s %s 必须恰好一条出边（实际 %d）", a.Type, a.ID, len(a.Outgoing))
			}
			if len(a.Incoming) == 0 {
				return perr("%s %s 必须有入边", a.Type, a.ID)
			}
		case "subProcess", "callActivity":
			if len(a.Outgoing) != 1 {
				return perr("%s %s 必须恰好一条出边（实际 %d）", a.Type, a.ID, len(a.Outgoing))
			}
		case "boundaryEvent":
			if len(a.Outgoing) != 1 {
				return perr("boundaryEvent %s 必须恰好一条出边", a.ID)
			}
		case "intermediateCatchEvent", "intermediateThrowEvent":
			if len(a.Incoming) != 1 || len(a.Outgoing) != 1 {
				return perr("%s %s 必须一进一出", a.Type, a.ID)
			}
		}
	}
	return nil
}

func parseDOM(data []byte) (*node, error) {
	dec := xml.NewDecoder(strings.NewReader(string(data)))
	var stack []*node
	var root *node
	for {
		tok, err := dec.Token()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			n := &node{XMLName: t.Name, Attrs: append([]xml.Attr(nil), t.Attr...)}
			if len(stack) > 0 {
				parent := stack[len(stack)-1]
				parent.Children = append(parent.Children, n)
			} else if root == nil {
				root = n
			}
			stack = append(stack, n)
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case xml.CharData:
			if len(stack) > 0 {
				cur := stack[len(stack)-1]
				cur.Text += string(t)
			}
		}
	}
	if root == nil {
		return nil, fmt.Errorf("空文档")
	}
	return root, nil
}
