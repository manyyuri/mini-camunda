// 端到端引擎测试：覆盖 spec 第 7 节的全部验收场景。
package engine_test

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"mini-camunda/internal/delegate"
	"mini-camunda/internal/engine"
	"mini-camunda/internal/example"
	"mini-camunda/internal/persistence"
)

// flaky delegate：剩余失败次数递减，<0 后成功（测试 Job 重试）。
var flakyFailures int64 = 2

func flaky() delegate.JavaDelegate {
	return delegate.JavaDelegateFunc(func(e delegate.DelegateExecution) error {
		n := atomic.AddInt64(&flakyFailures, -1)
		if n < 0 {
			return nil
		}
		return fmt.Errorf("模拟服务不可用（剩余失败 %d 次）", n+1)
	})
}

// countDelegate 记录执行次数。
var delegateCalls int64

func counting() delegate.JavaDelegate {
	return delegate.JavaDelegateFunc(func(e delegate.DelegateExecution) error {
		atomic.AddInt64(&delegateCalls, 1)
		return nil
	})
}

func newEngine(t *testing.T, historyLevel string) *engine.Engine {
	t.Helper()
	cfg := engine.DefaultConfiguration()
	cfg.DBPath = t.TempDir() + "/test.db" // 每个引擎独立库（:memory: 是进程级共享的）
	cfg.HistoryLevel = historyLevel
	cfg.JobPollInterval = 50 * time.Millisecond
	cfg.JobMaxWorkers = 4
	cfg.RegisterDelegate("noop", example.Noop())
	cfg.RegisterDelegate("boom", example.ThrowBpmnError("BOOM", "业务失败"))
	cfg.RegisterDelegate("flaky", flaky())
	cfg.RegisterDelegate("setcy", example.SetVars(map[string]any{"cy": true}))
	cfg.RegisterDelegate("risk-check", example.RiskCheck())
	cfg.RegisterDelegate("counting", counting())
	e, err := engine.NewProcessEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}

func deploy(t *testing.T, e *engine.Engine, file string) *persistence.ProcDefInfo {
	t.Helper()
	data, err := os.ReadFile("../../testdata/bpmn/" + file)
	if err != nil {
		t.Fatal(err)
	}
	info, err := e.DeployResource(file, data)
	if err != nil {
		t.Fatalf("部署 %s 失败: %v", file, err)
	}
	return info
}

// waitUntil 轮询断言（默认 8s 超时）。
func waitUntil(t *testing.T, timeout time.Duration, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", desc)
}

func hiInst(t *testing.T, e *engine.Engine, instID string) *persistence.HiProcInst {
	t.Helper()
	list, err := e.HistoricProcessInstances("", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range list {
		if p.ID == instID {
			return p
		}
	}
	t.Fatalf("历史实例 %s 不存在", instID)
	return nil
}

func histActIDs(t *testing.T, e *engine.Engine, instID string) map[string]bool {
	t.Helper()
	acts, err := e.HistoricActivities(instID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, a := range acts {
		out[a.ActID] = true
	}
	return out
}

// =====================================================================
// 部署与版本
// =====================================================================

func TestDeployVersioning(t *testing.T) {
	e := newEngine(t, "full")
	info1 := deploy(t, e, "hello.bpmn")
	info2 := deploy(t, e, "hello.bpmn")
	if info1.Version != 1 || info2.Version != 2 {
		t.Fatalf("版本递增错误: v%d v%d", info1.Version, info2.Version)
	}
	if info2.Key != "hello" || info2.ID == info1.ID {
		t.Fatalf("流程定义 id 规则错误: %s vs %s", info1.ID, info2.Key)
	}
	// id 格式 {key}:{version}:{random}
	if len(info2.ID) < len("hello:2:x") {
		t.Fatalf("procDefID 格式错误: %s", info2.ID)
	}
	defs, _ := e.ProcDefs("hello")
	if len(defs) != 2 {
		t.Fatalf("应有 2 个版本, got %d", len(defs))
	}
}

func TestUnsupportedElementRejected(t *testing.T) {
	e := newEngine(t, "full")
	data, _ := os.ReadFile("../../testdata/bpmn/unsupported.bpmn")
	_, err := e.DeployResource("unsupported.bpmn", data)
	if err == nil {
		t.Fatal("inclusiveGateway 必须被拒绝")
	}
}

// =====================================================================
// 同步流程 / 排他网关 / 表达式
// =====================================================================

func TestSyncProcess(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "hello.bpmn")
	inst, err := e.StartProcessInstanceByKey("hello", "BK-1", map[string]any{"who": "world"})
	if err != nil {
		t.Fatal(err)
	}
	hp := hiInst(t, e, inst)
	if hp.State != "completed" {
		t.Fatalf("同步流程应立即完成, state=%s", hp.State)
	}
	ids := histActIDs(t, e, inst)
	for _, want := range []string{"start", "st_hello", "end"} {
		if !ids[want] {
			t.Fatalf("缺少活动 %s: %v", want, ids)
		}
	}
	// RU 表应清空
	execs, _ := e.ProcessInstances()
	for _, x := range execs {
		if x.ProcInstID == inst {
			t.Fatal("实例完成后执行应被清理")
		}
	}
}

func TestExclusiveGatewayBranches(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "exclusive.bpmn")

	instHigh, _ := e.StartProcessInstanceByKey("excl-demo", "", map[string]any{"score": 80})
	hp := hiInst(t, e, instHigh)
	if hp.State != "completed" || hp.EndActID != "end" {
		t.Fatalf("高分实例未完成: %+v", hp)
	}
	ids := histActIDs(t, e, instHigh)
	if ids["st_low"] || !ids["st_high"] {
		t.Fatalf("条件分支错误: %v", ids)
	}

	instLow, _ := e.StartProcessInstanceByKey("excl-demo", "", map[string]any{"score": 10})
	ids = histActIDs(t, e, instLow)
	if !ids["st_low"] || ids["st_high"] {
		t.Fatalf("默认分支错误: %v", ids)
	}
}

// =====================================================================
// 并行网关 fork/join
// =====================================================================

func TestParallelGateway(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "parallel.bpmn")
	inst, err := e.StartProcessInstanceByKey("parallel-demo", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	hp := hiInst(t, e, inst)
	if hp.State != "completed" {
		t.Fatalf("并行流程应同步汇合完成, state=%s", hp.State)
	}
	ids := histActIDs(t, e, inst)
	for _, want := range []string{"fork", "st_a", "st_b", "join"} {
		if !ids[want] {
			t.Fatalf("缺少 %s: %v", want, ids)
		}
	}
}

// =====================================================================
// 子流程 / 调用活动
// =====================================================================

func TestSubprocessScope(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "subprocess.bpmn")
	inst, _ := e.StartProcessInstanceByKey("sub-demo", "", nil)
	hp := hiInst(t, e, inst)
	if hp.State != "completed" {
		t.Fatalf("子流程未完成: %s", hp.State)
	}
	ids := histActIDs(t, e, inst)
	for _, want := range []string{"sub_inner", "sub_start", "st_inner", "sub_end"} {
		if !ids[want] {
			t.Fatalf("缺少 %s: %v", want, ids)
		}
	}
}

func TestCallActivityMapping(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "callee.bpmn")
	deploy(t, e, "call.bpmn")
	inst, err := e.StartProcessInstanceByKey("call-demo", "", map[string]any{"x": 42})
	if err != nil {
		t.Fatal(err)
	}
	hp := hiInst(t, e, inst)
	if hp.State != "completed" {
		t.Fatalf("调用活动未完成: %s", hp.State)
	}
	// camunda:out 把子实例的 cy 变量带回主实例 y
	vars, _ := e.HistoricVariables(inst)
	found := false
	for _, v := range vars {
		if v.Name == "y" {
			found = true
		}
	}
	if !found {
		t.Fatalf("camunda:out 输出变量 y 缺失: %v", vars)
	}
	ids := histActIDs(t, e, inst)
	if !ids["call_sub"] {
		t.Fatalf("调用活动活动实例缺失: %v", ids)
	}
}

// =====================================================================
// 多实例
// =====================================================================

func TestMISequentialCollection(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "mi.bpmn")
	inst, err := e.StartProcessInstanceByKey("mi-demo", "", map[string]any{
		"items": []any{"a", "b", "c"},
	})
	if err != nil {
		t.Fatal(err)
	}
	hp := hiInst(t, e, inst)
	if hp.State != "completed" {
		t.Fatalf("MI 未完成: %s", hp.State)
	}
	// 3 个实例：st_mi 的 HI ACTINST 应有 3 条
	acts, _ := e.HistoricActivities(inst)
	n, ended := 0, 0
	for _, a := range acts {
		if a.ActID == "st_mi" {
			n++
			if a.EndTime != nil {
				ended++
			}
		}
	}
	// Camunda 语义：MI body 1 行 + 3 个内层实例行
	if n < 3 || ended < 3 {
		t.Fatalf("顺序 MI 应执行 3 次, got rows=%d ended=%d", n, ended)
	}
}

func TestMIParallelCardinality(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "mipar.bpmn")
	inst, _ := e.StartProcessInstanceByKey("mipar-demo", "", nil)
	hp := hiInst(t, e, inst)
	if hp.State != "completed" {
		t.Fatalf("并行 MI 未完成: %s", hp.State)
	}
	acts, _ := e.HistoricActivities(inst)
	n := 0
	for _, a := range acts {
		if a.ActID == "st_mipar" {
			n++
		}
	}
	if n < 3 {
		t.Fatalf("loopCardinality=3 应执行 3 次, got %d", n)
	}
}

// =====================================================================
// 用户任务 / 认领
// =====================================================================

func TestUserTaskLifecycle(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "timer-boundary.bpmn")
	inst, _ := e.StartProcessInstanceByKey("timerb-demo", "", nil)

	waitUntil(t, 3*time.Second, "用户任务创建", func() bool {
		tasks, _ := e.Tasks(persistence.TaskFilter{})
		return len(tasks) == 1
	})
	tasks, _ := e.Tasks(persistence.TaskFilter{})
	tk := tasks[0]
	if tk.Assignee != "tester" {
		t.Fatalf("assignee 错误: %s", tk.Assignee)
	}

	// claim 冲突
	if err := e.Claim(tk.ID, "other"); err == nil {
		t.Fatal("已指派任务再 claim 应冲突")
	}

	// 完成前等待超时被触发？给足 1.5s 窗口后确认还在
	if err := e.CompleteTask(tk.ID, nil); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, "实例完成", func() bool {
		return hiInst(t, e, inst).State == "completed"
	})
	hp := hiInst(t, e, inst)
	if hp.EndActID != "end_manual" {
		t.Fatalf("人工完成路径错误: %s", hp.EndActID)
	}
}

// =====================================================================
// 边界事件
// =====================================================================

func TestTimerBoundaryInterrupting(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "timer-boundary.bpmn")
	inst, _ := e.StartProcessInstanceByKey("timerb-demo", "", nil)

	waitUntil(t, 6*time.Second, "超时边界触发并完成实例", func() bool {
		return hiInst(t, e, inst).EndTime != nil
	})
	hp := hiInst(t, e, inst)
	if hp.EndActID != "end_timeout" {
		t.Fatalf("应走超时自动拒绝路径, end=%s", hp.EndActID)
	}
	// 被中断的用户任务应已删除
	tasks, _ := e.Tasks(persistence.TaskFilter{ProcInstID: inst})
	if len(tasks) != 0 {
		t.Fatalf("中断后任务应删除, got %d", len(tasks))
	}
	ids := histActIDs(t, e, inst)
	if !ids["st_autorefuse"] || !ids["b_timeout"] {
		t.Fatalf("超时路径历史缺失: %v", ids)
	}
}

func TestErrorBoundary(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "error.bpmn")
	inst, err := e.StartProcessInstanceByKey("error-demo", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	hp := hiInst(t, e, inst)
	if hp.State != "completed" {
		t.Fatalf("错误边界捕获后应正常完成: %s", hp.State)
	}
	if hp.EndActID != "end_caught" {
		t.Fatalf("应走兜底路径: %s", hp.EndActID)
	}
	ids := histActIDs(t, e, inst)
	if !ids["st_recover"] || ids["end_ok"] {
		t.Fatalf("BpmnError 边界传播错误: %v", ids)
	}
	// BpmnError 不产生 Job 失败历史
	logs, _ := e.HistoricJobLogs()
	for _, l := range logs {
		if l.State == "failed" {
			t.Fatal("BpmnError 不应记录 Job 失败")
		}
	}
}

// =====================================================================
// Job 重试 / Incident / 恢复
// =====================================================================

func TestJobRetryAndIncident(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "techfail.bpmn")
	atomic.StoreInt64(&flakyFailures, 2) // 前 2 次失败，第 3 次成功
	inst, _ := e.StartProcessInstanceByKey("techfail-demo", "", nil)

	waitUntil(t, 15*time.Second, "两次失败后重试成功完成", func() bool {
		return hiInst(t, e, inst).State == "completed"
	})
	logs, _ := e.HistoricJobLogs()
	failed := 0
	for _, l := range logs {
		if l.State == "failed" {
			failed++
		}
	}
	if failed != 2 {
		t.Fatalf("应有 2 条失败日志, got %d", failed)
	}
}

func TestIncidentAndSetJobRetries(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "techfail.bpmn")
	atomic.StoreInt64(&flakyFailures, 1000) // 永远失败
	inst, _ := e.StartProcessInstanceByKey("techfail-demo", "", nil)

	var jobID string
	waitUntil(t, 15*time.Second, "重试耗尽产生 Incident", func() bool {
		incs, _ := e.Incidents()
		for _, i := range incs {
			if i.Type == "failedJob" && i.ProcInstID == inst {
				jobID = i.JobID
				return true
			}
		}
		return false
	})

	// 修复故障 → 重置重试 → 自动恢复完成
	atomic.StoreInt64(&flakyFailures, -1)
	if err := e.SetJobRetries(jobID, 1); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 15*time.Second, "SetJobRetries 恢复执行", func() bool {
		return hiInst(t, e, inst).State == "completed"
	})
	// Incident 应已解决（删除）
	incs, _ := e.Incidents()
	for _, i := range incs {
		if i.JobID == jobID {
			t.Fatal("恢复后 Incident 应解决")
		}
	}
}

// =====================================================================
// 定时器
// =====================================================================

func TestTimerIntermediateCatch(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "timer-catch.bpmn")
	started := time.Now()
	inst, _ := e.StartProcessInstanceByKey("timercatch-demo", "", nil)

	waitUntil(t, 8*time.Second, "1.5s 后定时器触发完成", func() bool {
		return hiInst(t, e, inst).EndTime != nil
	})
	if d := time.Since(started); d < 1400*time.Millisecond {
		t.Fatalf("定时器提前触发: %v", d)
	}
	ids := histActIDs(t, e, inst)
	if !ids["wait_timer"] {
		t.Fatalf("定时捕获事件缺失: %v", ids)
	}
}

func TestTimerStart(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "timer-start.bpmn")
	waitUntil(t, 8*time.Second, "定时启动事件创建实例", func() bool {
		h, _ := e.HistoricProcessInstances("", "timerstart-demo")
		return len(h) >= 1
	})
	h, _ := e.HistoricProcessInstances("", "timerstart-demo")
	if h[0].State != "completed" {
		t.Fatalf("定时启动的实例应自动跑完: %s", h[0].State)
	}
}

// =====================================================================
// 消息 / 信号
// =====================================================================

func TestMessageCorrelation(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "message.bpmn")
	inst1, _ := e.StartProcessInstanceByKey("message-demo", "BK-A", map[string]any{"tag": "one"})
	inst2, _ := e.StartProcessInstanceByKey("message-demo", "BK-B", map[string]any{"tag": "two"})

	// corrKeys 不匹配 → MessageNotCorrelated 错误（Camunda 语义）
	n, err := e.CorrelateMessage("go-msg", "", map[string]any{"tag": "none"}, nil)
	if err == nil || n != 0 {
		t.Fatalf("不匹配的 corrKeys 应报错: n=%d err=%v", n, err)
	}
	// 精确关联 inst2
	n, err = e.CorrelateMessage("go-msg", "", map[string]any{"tag": "two"}, map[string]any{"kicked": true})
	if err != nil || n != 1 {
		t.Fatalf("关联失败: n=%d err=%v", n, err)
	}
	waitUntil(t, 3*time.Second, "inst2 完成", func() bool {
		return hiInst(t, e, inst2).State == "completed"
	})
	if hiInst(t, e, inst1).State == "completed" {
		t.Fatal("inst1 不应被触发")
	}
	// businessKey 关联 inst1
	if _, err := e.CorrelateMessage("go-msg", "BK-A", nil, nil); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, "inst1 完成", func() bool {
		return hiInst(t, e, inst1).State == "completed"
	})
}

func TestMessageStart(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "msgstart.bpmn")
	n, err := e.CorrelateMessage("kick-msg", "MSG-BK", nil, map[string]any{"via": "message"})
	if err != nil || n != 1 {
		t.Fatalf("消息启动失败: n=%d err=%v", n, err)
	}
	h, _ := e.HistoricProcessInstances("", "msgstart-demo")
	if len(h) != 1 || h[0].BusinessKey != "MSG-BK" {
		t.Fatalf("消息启动实例错误: %+v", h)
	}
	if h[0].State != "completed" {
		t.Fatalf("启动后应直通: %s", h[0].State)
	}
}

func TestSignalBroadcastAndStart(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "signal.bpmn")
	deploy(t, e, "sigstart.bpmn")

	i1, _ := e.StartProcessInstanceByKey("signal-demo", "", nil)
	i2, _ := e.StartProcessInstanceByKey("signal-demo", "", nil)
	if err := e.SignalEventReceived("alert-sig", nil); err != nil {
		t.Fatal(err)
	}
	// 两个等待实例都被广播触发 + 信号启动一个新实例
	waitUntil(t, 3*time.Second, "广播触发两个实例", func() bool {
		return hiInst(t, e, i1).State == "completed" && hiInst(t, e, i2).State == "completed"
	})
	waitUntil(t, 3*time.Second, "信号启动实例", func() bool {
		h, _ := e.HistoricProcessInstances("", "sigstart-demo")
		return len(h) == 1
	})
}

func TestSignalThrowInsideProcess(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "signal.bpmn")
	deploy(t, e, "sigthrow.bpmn")

	waiter, _ := e.StartProcessInstanceByKey("signal-demo", "", nil)
	thrower, _ := e.StartProcessInstanceByKey("sigthrow-demo", "", nil)
	waitUntil(t, 3*time.Second, "thrower 完成", func() bool {
		return hiInst(t, e, thrower).State == "completed"
	})
	waitUntil(t, 3*time.Second, "等待者被同进程内抛出的信号唤醒", func() bool {
		return hiInst(t, e, waiter).State == "completed"
	})
}

// =====================================================================
// 终止 / 删除实例
// =====================================================================

func TestTerminateEnd(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "terminate.bpmn")
	inst, err := e.StartProcessInstanceByKey("term-demo", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	hp := hiInst(t, e, inst)
	if hp.State != "internally-terminated" {
		t.Fatalf("terminate 语义: state=%s", hp.State)
	}
	if hp.EndActID != "end_term" {
		t.Fatalf("终止活动错误: %s", hp.EndActID)
	}
	// 全部 RU 清理
	execs, _ := e.ProcessInstances()
	for _, x := range execs {
		if x.ProcInstID == inst {
			t.Fatal("terminate 后 RU 执行应清空")
		}
	}
	tasks, _ := e.Tasks(persistence.TaskFilter{})
	if len(tasks) != 0 {
		t.Fatal("terminate 后右支路用户任务应删除")
	}
}

func TestDeleteProcessInstance(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "timer-boundary.bpmn")
	inst, _ := e.StartProcessInstanceByKey("timerb-demo", "", nil)
	waitUntil(t, 3*time.Second, "任务创建", func() bool {
		tasks, _ := e.Tasks(persistence.TaskFilter{})
		return len(tasks) == 1
	})
	if err := e.DeleteProcessInstance(inst, "test-delete"); err != nil {
		t.Fatal(err)
	}
	hp := hiInst(t, e, inst)
	if hp.State != "internally-terminated" || hp.DeleteReason != "test-delete" {
		t.Fatalf("删除语义错误: %+v", hp)
	}
}

// =====================================================================
// 历史级别
// =====================================================================

func TestHistoryLevels(t *testing.T) {
	e := newEngine(t, "none")
	deploy(t, e, "hello.bpmn")
	inst, _ := e.StartProcessInstanceByKey("hello", "", nil)
	acts, _ := e.HistoricActivities(inst)
	if len(acts) != 0 {
		t.Fatalf("historyLevel=none 不应有活动历史, got %d", len(acts))
	}
	h, _ := e.HistoricProcessInstances("", "hello")
	if len(h) != 0 {
		t.Fatal("historyLevel=none 不应有实例历史（Camunda 语义：none 不写 HI）")
	}
}

// =====================================================================
// 外部任务
// =====================================================================

func TestExternalTaskLifecycle(t *testing.T) {
	e := newEngine(t, "full")
	deploy(t, e, "loan-review.bpmn")
	inst, err := e.StartProcessInstanceByKey("loan-review", "EXT-1", map[string]any{
		"amount": 100, "reviewer": "x",
	})
	if err != nil {
		t.Fatal(err)
	}

	var (
		taskID string
		retries int
	)
	waitUntil(t, 5*time.Second, "外部任务可获取", func() bool {
		ets, err := e.FetchAndLock("credit-check", "w1", 5*time.Minute, 5, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(ets) == 0 {
			return false
		}
		taskID, retries = ets[0].ID, ets[0].Retries
		return true
	})
	if retries != 3 {
		t.Fatalf("外部任务默认重试应为 3, got %d", retries)
	}

	// 错误 worker 不能完成
	if err := e.CompleteExtTask(taskID, "other-worker", nil); err == nil {
		t.Fatal("workerId 不匹配应失败")
	}

	// 失败两次 → retries 耗尽 → Incident
	if err := e.FailExtTask(taskID, "w1", "征信服务 5xx", 2); err != nil {
		t.Fatal(err)
	}
	if err := e.FailExtTask(taskID, "w1", "征信服务 5xx", 0); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, "外部任务 Incident", func() bool {
		incs, _ := e.Incidents()
		for _, i := range incs {
			if i.Type == "failedExternalTask" && i.ProcInstID == inst {
				return true
			}
		}
		return false
	})

	// SetExtTaskRetries 恢复 → 完成流程
	if err := e.SetExtTaskRetries(taskID, 1); err != nil {
		t.Fatal(err)
	}
	ets, err := e.FetchAndLock("credit-check", "w1", 5*time.Minute, 5, 0)
	if err != nil || len(ets) != 1 || ets[0].ID != taskID {
		t.Fatalf("恢复后应可重新获取: %v %v", ets, err)
	}
	if err := e.CompleteExtTask(taskID, "w1", map[string]any{"creditOk": true}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, "外部任务完成 → 流程结束", func() bool {
		return hiInst(t, e, inst).State == "completed"
	})
	hp := hiInst(t, e, inst)
	if hp.EndActID != "end_done" {
		t.Fatalf("应放款完成: %s", hp.EndActID)
	}
}

// =====================================================================
// 持久化重启（文件库）
// =====================================================================

func TestPersistenceAcrossRestart(t *testing.T) {
	db := t.TempDir() + "/restart.db"

	cfg := engine.DefaultConfiguration()
	cfg.DBPath = db
	cfg.EnableJobExecutor = false
	cfg.RegisterDelegate("noop", example.Noop())
	e1, err := engine.NewProcessEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e1.DeployResource("message.bpmn", mustRead("../../testdata/bpmn/message.bpmn")); err != nil {
		t.Fatal(err)
	}
	inst, err := e1.StartProcessInstanceByKey("message-demo", "RESTART", nil)
	if err != nil {
		t.Fatal(err)
	}
	e1.Close()

	// 重启：部署缓存、等待中的实例恢复
	e2, err := engine.NewProcessEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	defs, _ := e2.ProcDefs("message-demo")
	if len(defs) != 1 {
		t.Fatal("重启后部署丢失")
	}
	if _, err := e2.CorrelateMessage("go-msg", "RESTART", nil, nil); err != nil {
		t.Fatalf("重启后关联失败: %v", err)
	}
	waitUntil(t, 3*time.Second, "重启后完成", func() bool {
		h, _ := e2.HistoricProcessInstances("", "message-demo")
		for _, p := range h {
			if p.ID == inst {
				return p.State == "completed"
			}
		}
		return false
	})
}

func mustRead(p string) []byte {
	data, err := os.ReadFile(p)
	if err != nil {
		panic(err)
	}
	return data
}
