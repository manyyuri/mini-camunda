// demo：loan-review 教学剧本（对应 Camunda 官方演示示例的角色）。
package main

import (
	"fmt"
	"time"

	"mini-camunda/internal/engine"
	"mini-camunda/internal/example"
	"mini-camunda/internal/kernel"
	"mini-camunda/internal/persistence"
)

func runDemo(dbPath string) error {
	cfg := engine.DefaultConfiguration()
	cfg.DBPath = dbPath
	cfg.RegisterDelegate("noop", example.Noop())
	cfg.RegisterDelegate("risk-check", example.RiskCheck())
	e, err := engine.NewProcessEngine(cfg)
	if err != nil {
		return err
	}
	defer e.Close()

	// 1. 部署
	info, err := e.DeployResource("loan-review.bpmn", mustRead("testdata/bpmn/loan-review.bpmn"))
	if err != nil {
		return err
	}
	fmt.Printf("▶ 部署 loan-review v%d（%s）\n", info.Version, info.ID)

	// 2. 大额申请：走人工复核路径（异步 Job + 用户任务 + 边界事件）
	fmt.Println("\n▶ 场景 A：大额申请（amount=500000，风控分 500 → 人工复核）")
	instA, err := e.StartProcessInstanceByKey("loan-review", "LOAN-2026-001", map[string]any{
		"amount": 500000, "reviewer": "andy",
	})
	if err != nil {
		return err
	}
	fmt.Printf("  实例已启动: %s\n", instA)
	waitFor(2*time.Second, func() bool {
		tasks, _ := e.Tasks(persistence.TaskFilter{})
		return len(tasks) > 0
	})

	tasks, _ := e.Tasks(persistence.TaskFilter{})
	var reviewID string
	for _, t := range tasks {
		fmt.Printf("  用户任务: [%s] %s（办理人 %s）\n", t.TaskDefKey, t.Name, t.Assignee)
		reviewID = t.ID
	}

	// 3. 非中断边界：催办消息
	fmt.Println("\n▶ 场景 A-1：发送催办消息（非中断边界）")
	if _, err := e.CorrelateMessage("催办", "LOAN-2026-001", nil, nil); err != nil {
		return err
	}
	time.Sleep(300 * time.Millisecond)
	acts, _ := e.HistoricActivities(instA)
	for _, a := range acts {
		if a.ActID == "st_notify_user" {
			fmt.Println("  ✓ 催办通知已发送（st_notify_user 执行过），复核任务未被中断")
		}
	}

	// 4. 完成复核 → 放款
	fmt.Println("\n▶ 场景 A-2：复核通过 → 放款提交")
	if err := e.CompleteTask(reviewID, map[string]any{"approved": true}); err != nil {
		return err
	}
	waitFor(2*time.Second, func() bool {
		h, _ := e.HistoricProcessInstances("", "loan-review")
		for _, p := range h {
			if p.ID == instA && p.EndTime != nil {
				return true
			}
		}
		return false
	})
	h, _ := e.HistoricProcessInstances("", "loan-review")
	for _, p := range h {
		if p.ID == instA {
			fmt.Printf("  ✓ 实例结束: state=%s 结束活动=%s\n", p.State, p.EndActID)
		}
	}

	// 5. 小额申请：外部征信 Worker 演示
	fmt.Println("\n▶ 场景 B：小额申请（amount=8000 → 外部征信查询，模拟 Worker）")
	instB, err := e.StartProcessInstanceByKey("loan-review", "LOAN-2026-002", map[string]any{
		"amount": 8000, "reviewer": "bella",
	})
	if err != nil {
		return err
	}
	ets, err := e.FetchAndLock("credit-worker", "credit-worker", 5*time.Minute, 5, 0)
	if err != nil {
		return err
	}
	for _, et := range ets {
		fmt.Printf("  Worker 取到外部任务: %s（topic=%s，变量 creditOk 由外部系统给出）\n", et.ActID, et.Topic)
		if err := e.CompleteExtTask(et.ID, "credit-worker", map[string]any{"creditOk": true}); err != nil {
			return err
		}
	}
	waitFor(2*time.Second, func() bool {
		h, _ := e.HistoricProcessInstances("", "loan-review")
		for _, p := range h {
			if p.ID == instB && p.EndTime != nil {
				return true
			}
		}
		return false
	})
	fmt.Printf("  ✓ 实例 %s 已放款完成\n", instB)

	// 6. 历史时间线
	fmt.Println("\n▶ 实例 A 的历史活动时间线：")
	acts, _ = e.HistoricActivities(instA)
	for _, a := range acts {
		fmt.Printf("  %-16s %-14s %s\n", a.ActID, a.ActType, a.StartTime.Format("15:04:05.000"))
	}

	// 7. Job / 订阅 / 历史变量统计
	jobs, _ := e.Jobs()
	subs, _ := e.EventSubscriptions()
	hv, _ := e.HistoricVariables(instA)
	fmt.Printf("\n▶ 运行时余留：jobs=%d subscriptions=%d；实例A历史变量=%d 条\n", len(jobs), len(subs), len(hv))
	var _ = kernel.JobHandlerError{}
	fmt.Println("\n演示结束。完整能力（并行网关/子流程/多实例/补偿重试/Incident）见测试与 README。")
	return nil
}

func waitFor(d time.Duration, fn func() bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func mustRead(p string) []byte {
	data, err := readFileBytes(p)
	if err != nil {
		panic(err)
	}
	return data
}
