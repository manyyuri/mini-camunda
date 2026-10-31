// Job 子系统：对应 JobEntity + JobManager + ExecuteJobCmd + JobRetryInterceptor 语义。
// HandlerType：async-continuation（异步延续）| timer-catch | timer-boundary | timer-start。
package kernel

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/el"
	"mini-camunda/internal/errs"
	"mini-camunda/internal/persistence"
)

// asyncCfg HANDLER_CFG_ JSON。
type asyncCfg struct {
	Kind       string `json:"kind"`       // before | after
	ActivityID string `json:"activityId"`
}

// timerCfg 定时器 Job 的 HANDLER_CFG_。
type timerCfg struct {
	ActivityID    string `json:"activityId"`
	AttachedTo    string `json:"attachedTo,omitempty"`    // 边界：宿主活动
	Interrupting  bool   `json:"interrupting,omitempty"`  // 边界：是否中断
	ProcDefID     string `json:"procDefId,omitempty"`     // timer-start：流程定义
	DueDate       string `json:"dueDate,omitempty"`       // 计算出的绝对时间（RFC3339）
}

// jobDefFor 获取或创建 JobDefinition（镜像 Camunda 按 (procDef, act, type, config) 唯一）。
func (k *Kernel) jobDefFor(e *persistence.Execution, act *bpmn.Activity, jobType, config string) string {
	d, _ := persistence.SelectJobDefByKey(k.ctx.Tx, e.ProcDefID, act.ID, jobType, config)
	if d != nil {
		return d.ID
	}
	def := k.defOf(e)
	id := k.ctx.NextID()
	_ = persistence.InsertJobDef(k.ctx.Tx, &persistence.JobDef{
		ID: id, ProcDefID: e.ProcDefID, ProcDefKey: def.Key, ActID: act.ID,
		JobType: jobType, JobConfig: config,
	})
	return id
}

// retryCount 解析活动上的 R{n}/PT.. 重试周期（失败后重试 n 次）。
// jobBackoff 重试退避：R{n}/PT.. 的时长；无配置时固定 10s。
func (k *Kernel) jobBackoff(j *persistence.Job) time.Duration {
	backoff := 10 * time.Second
	if j.ExecutionID != "" {
		if e, err := k.ctx.Exec(j.ExecutionID); err == nil {
			def := k.defOf(e)
			if act := def.Activities[e.ActivityID]; act != nil {
				if cyc, ok := act.Props[bpmn.PropRetryCycle].(string); ok {
					if i := strings.Index(cyc, "/"); i >= 0 && i+1 < len(cyc) {
						if d, err := iso8601Duration(cyc[i+1:]); err == nil && d > 0 {
							backoff = d
						}
					}
				}
			}
		}
	}
	return backoff
}

func retryCountOf(act *bpmn.Activity, defRetries int) int {
	if cycle, ok := act.Props[bpmn.PropRetryCycle].(string); ok && cycle != "" {
		if m := regexp.MustCompile(`^R(\d+)/`).FindStringSubmatch(cycle); m != nil {
			if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
				return n
			}
		}
	}
	return defRetries
}

func (k *Kernel) createAsyncJob(e *persistence.Execution, act *bpmn.Activity, kind string) {
	cfg, _ := json.Marshal(asyncCfg{Kind: kind, ActivityID: act.ID})
	j := &persistence.Job{
		ID: k.ctx.NextID(), Type: "message", Exclusive: true,
		ExecutionID: e.ID, ProcessInstID: e.ProcInstID, ProcDefID: e.ProcDefID,
		Retries: retryCountOf(act, 3), DueDate: ptrTime(k.now()),
		HandlerType: "async-continuation", HandlerCfg: string(cfg),
	}
	j.JobDefID = k.jobDefFor(e, act, "async-continuation", kind)
	k.ctx.InsertJob(j)
	k.hiJobLog(j, "created", "")
}

func ptrTime(t time.Time) *time.Time { return &t }

// iso8601Duration 解析 PT{n}S/PT{n}M/PT{n}H/P{n}D/P{n}DT{h}H{m}M{s}S。
func iso8601Duration(s string) (time.Duration, error) {
	re := regexp.MustCompile(`^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)
	m := re.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("无法解析 ISO8601 时长: %q", s)
	}
	d := time.Duration(0)
	if m[1] != "" {
		n, _ := strconv.Atoi(m[1])
		d += time.Duration(n) * 24 * time.Hour
	}
	if m[2] != "" {
		n, _ := strconv.Atoi(m[2])
		d += time.Duration(n) * time.Hour
	}
	if m[3] != "" {
		n, _ := strconv.Atoi(m[3])
		d += time.Duration(n) * time.Minute
	}
	if m[4] != "" {
		f, _ := strconv.ParseFloat(m[4], 64)
		d += time.Duration(f * float64(time.Second))
	}
	if d == 0 {
		return 0, fmt.Errorf("ISO8601 时长为 0: %q", s)
	}
	return d, nil
}

// computeTimerDue 计算定时器到期时间（date/duration/cycle）。
func (k *Kernel) computeTimerDue(t *bpmn.TimerDef, vars map[string]any) (time.Time, error) {
	now := k.now()
	switch t.Kind {
	case "date":
		out, err := el.Evaluate(t.Expr, vars)
		if err != nil {
			return now, err
		}
		switch v := out.(type) {
		case time.Time:
			return v, nil
		case string:
			for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
				if tt, err := time.Parse(layout, v); err == nil {
					return tt, nil
				}
			}
			return now, fmt.Errorf("无法解析 timeDate: %q", v)
		}
		return now, fmt.Errorf("timeDate 结果类型不支持: %T", out)
	case "duration", "cycle":
		out, err := el.Evaluate(t.Expr, vars)
		if err != nil {
			return now, err
		}
		expr := fmt.Sprintf("%v", out)
		d, err := iso8601Duration(expr)
		if err != nil {
			return now, err
		}
		return now.Add(d), nil
	}
	return now, fmt.Errorf("未知定时器类型: %s", t.Kind)
}

func (k *Kernel) createTimerJob(e *persistence.Execution, act *bpmn.Activity, t *bpmn.TimerDef, handlerType string, extra timerCfg) error {
	vars := k.visibleVars(e)
	due, err := k.computeTimerDue(t, vars)
	if err != nil {
		return err
	}
	cfg := timerCfg{ActivityID: act.ID, DueDate: persistence.FormatTime(due)}
	if handlerType == "timer-boundary" {
		cfg.AttachedTo = extra.AttachedTo
		cfg.Interrupting = extra.Interrupting
	}
	if handlerType == "timer-start" {
		cfg.ProcDefID = extra.ProcDefID
	}
	cfgBytes, _ := json.Marshal(cfg)
	j := &persistence.Job{
		ID: k.ctx.NextID(), Type: "timer", Exclusive: true,
		ExecutionID: e.ID, ProcessInstID: e.ProcInstID, ProcDefID: e.ProcDefID,
		Retries: 1, DueDate: &due,
		HandlerType: handlerType, HandlerCfg: string(cfgBytes),
	}
	j.JobDefID = k.jobDefFor(e, act, handlerType, "")
	k.ctx.InsertJob(j)
	k.hiJobLog(j, "created", "")
	return nil
}

// CreateTimerStartJob 部署时为 timer 启动事件创建 Job（对应部署时的定时器启动注册）。
// 幂等：同一定义同一起始事件已存在未触发 Job 则跳过（引擎重启重建注册表时防重复）。
func (k *Kernel) CreateTimerStartJob(def *bpmn.ProcessDefinition, start *bpmn.Activity) error {
	if existing := k.findTimerStartJob(def.ID, start.ID); existing != nil {
		return nil
	}
	t := start.Props[bpmn.PropTimer].(*bpmn.TimerDef)
	due, err := k.computeTimerDueWithEmpty(t)
	if err != nil {
		return err
	}
	cfg := timerCfg{ActivityID: start.ID, ProcDefID: def.ID, DueDate: persistence.FormatTime(due)}
	cfgBytes, _ := json.Marshal(cfg)
	jd, _ := persistence.SelectJobDefByKey(k.ctx.Tx, def.ID, start.ID, "timer-start", "")
	if jd == nil {
		jd = &persistence.JobDef{ID: k.ctx.NextID(), ProcDefID: def.ID, ProcDefKey: def.Key, ActID: start.ID, JobType: "timer-start", JobConfig: ""}
		if err := persistence.InsertJobDef(k.ctx.Tx, jd); err != nil {
			return err
		}
	}
	j := &persistence.Job{
		ID: k.ctx.NextID(), Type: "timer", Exclusive: true,
		ProcDefID: def.ID, Retries: 1, DueDate: &due,
		HandlerType: "timer-start", HandlerCfg: string(cfgBytes), JobDefID: jd.ID,
	}
	k.ctx.InsertJob(j)
	k.hiJobLog(j, "created", "")
	return nil
}

func (k *Kernel) findTimerStartJob(procDefID, actID string) *persistence.Job {
	jobs, err := persistence.SelectJobs(k.ctx.Tx, "")
	if err != nil {
		return nil
	}
	for _, j := range jobs {
		if j.HandlerType != "timer-start" || j.ProcDefID != procDefID {
			continue
		}
		var cfg timerCfg
		if json.Unmarshal([]byte(j.HandlerCfg), &cfg) == nil && cfg.ActivityID == actID {
			return j
		}
	}
	return nil
}

func (k *Kernel) computeTimerDueWithEmpty(t *bpmn.TimerDef) (time.Time, error) {
	now := k.now()
	if t.Kind == "date" {
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
			if tt, err := time.Parse(layout, t.Expr); err == nil {
				return tt, nil
			}
		}
		return now, fmt.Errorf("无法解析 timeDate: %q", t.Expr)
	}
	d, err := iso8601Duration(t.Expr)
	if err != nil {
		return now, err
	}
	return now.Add(d), nil
}

// =====================================================================
// Job 执行（ExecuteJobCmd 核心）
// =====================================================================

// ExecuteJob 执行一个已获取（锁定）的 Job。
// 技术错误返回 JobHandlerError（外层转 JobFailedCmd：重试/Incident）；事务整体回滚。
func (k *Kernel) ExecuteJob(jobID string) error {
	j, err := k.ctx.Job(jobID)
	if err != nil {
		return errs.NewNotFound("Job %s", jobID)
	}
	var cfg asyncCfg
	var tcfg timerCfg

	switch j.HandlerType {
	case "async-continuation":
		_ = json.Unmarshal([]byte(j.HandlerCfg), &cfg)
		e, err := k.ctx.Exec(j.ExecutionID)
		if err != nil {
			// 执行已不存在（实例已结束）→ Job 静默删除
			k.quietDeleteJob(j)
			return nil
		}
		// 先删 Job（执行成功则连同提交；失败回滚恢复）
		k.ctx.DeleteJob(j)
		if cfg.Kind == "before" {
			k.pushOp(op{kind: opExec, execID: e.ID, actID: cfg.ActivityID, resumeAsync: true})
		} else {
			k.pushOp(op{kind: opFinish, execID: e.ID})
		}
		err = k.Run()
		if err != nil {
			return wrapJobHandlerErr(err)
		}
		k.hiJobLog(j, "succeeded", "")
		return nil

	case "timer-catch":
		_ = json.Unmarshal([]byte(j.HandlerCfg), &tcfg)
		e, err := k.ctx.Exec(j.ExecutionID)
		if err != nil {
			k.quietDeleteJob(j)
			return nil
		}
		k.ctx.DeleteJob(j)
		k.pushOp(op{kind: opFinish, execID: e.ID})
		if err := k.Run(); err != nil {
			return wrapJobHandlerErr(err)
		}
		k.hiJobLog(j, "succeeded", "")
		return nil

	case "timer-boundary":
		_ = json.Unmarshal([]byte(j.HandlerCfg), &tcfg)
		e, err := k.ctx.Exec(j.ExecutionID)
		if err != nil {
			k.quietDeleteJob(j)
			return nil
		}
		def := k.defOf(e)
		b := def.Activities[tcfg.ActivityID]
		host := def.Activities[tcfg.AttachedTo]
		k.ctx.DeleteJob(j)
		if err := k.fireTimerBoundary(e, host, b); err != nil {
			return wrapJobHandlerErr(err)
		}
		if err := k.Run(); err != nil {
			return wrapJobHandlerErr(err)
		}
		k.hiJobLog(j, "succeeded", "")
		return nil

	case "timer-start":
		_ = json.Unmarshal([]byte(j.HandlerCfg), &tcfg)
		def, err := k.deps.ProcDef(tcfg.ProcDefID)
		if err != nil {
			k.quietDeleteJob(j)
			return err
		}
		k.ctx.DeleteJob(j)
		if _, err := k.NewInstance(def, "", nil, ""); err != nil {
			return wrapJobHandlerErr(err)
		}
		if err := k.Run(); err != nil {
			return wrapJobHandlerErr(err)
		}
		k.hiJobLog(j, "succeeded", "")
		// 周期定时器：重建 Job（mini 把 cycle 当重复调度：到点再挂一个）
		if t, ok := startTimerDef(def, tcfg.ActivityID); ok && t.Kind == "cycle" {
			_ = k.CreateTimerStartJob(def, def.Activities[tcfg.ActivityID])
		}
		return nil
	}
	return errs.NewEngineError("未知 Job HandlerType: %s", j.HandlerType)
}

func startTimerDef(def *bpmn.ProcessDefinition, actID string) (*bpmn.TimerDef, bool) {
	if a, ok := def.Activities[actID]; ok {
		t, ok := a.Props[bpmn.PropTimer].(*bpmn.TimerDef)
		return t, ok
	}
	return nil, false
}

func (k *Kernel) quietDeleteJob(j *persistence.Job) {
	k.ctx.DeleteJob(j)
	k.hiJobLog(j, "deleted", "")
}

// JobHandlerError 技术性失败：外层（jobexec）用独立事务走 JobFailed 流程。
type JobHandlerError struct{ Err error }

func (e *JobHandlerError) Error() string { return "job handler failed: " + e.Err.Error() }
func (e *JobHandlerError) Unwrap() error { return e.Err }

func wrapJobHandlerErr(err error) error {
	if _, ok := err.(*JobHandlerError); ok {
		return err
	}
	return &JobHandlerError{Err: err}
}

// =====================================================================
// Job 失败（JobFailedCmd）：独立事务，重试退避 + Incident。
// =====================================================================

// FailJob 技术失败处理：RETRIES_-1；耗尽 → Incident(failedJob)。
// 退避时长优先取 failedJobRetryTimeCycle 的 PT 段（Camunda 语义），否则固定 10s（简化点，README 说明）。
func (k *Kernel) FailJob(jobID, workerID string, cause error) error {
	j, err := k.ctx.Job(jobID)
	if err != nil {
		return nil // Job 已消失（实例结束等），无需处理
	}
	j.Retries--
	j.LockOwner = ""
	j.LockExpTime = nil
	j.ExceptionMsg = trimErr(cause.Error())
	if j.Retries > 0 {
		due := k.now().Add(k.jobBackoff(j))
		j.DueDate = &due
		k.ctx.UpdateJob(j)
		k.hiJobLog(j, "failed", j.ExceptionMsg)
		return nil
	}
	// 重试耗尽 → Incident
	due := k.now().Add(365 * 24 * time.Hour) // 冻结：等 SetJobRetries 重置
	j.DueDate = &due
	k.ctx.UpdateJob(j)
	k.hiJobLog(j, "failed", j.ExceptionMsg)

	actID := ""
	if j.ExecutionID != "" {
		if e, err := k.ctx.Exec(j.ExecutionID); err == nil {
			actID = e.ActivityID
		}
	}
	inc := &persistence.Incident{
		ID: k.ctx.NextID(), Type: "failedJob", Msg: j.ExceptionMsg, Timestamp: k.now(),
		ExecutionID: j.ExecutionID, ActivityID: actID, ProcInstID: j.ProcessInstID,
		ProcDefID: j.ProcDefID, JobID: j.ID,
	}
	if err := persistence.InsertIncident(k.ctx.Tx, inc); err != nil {
		return err
	}
	k.hiIncidentCreate(inc)
	return nil
}

// SetJobRetries 管理命令：重置重试数；有未解决 Incident 则解决之（语义：Job 可再次被获取）。
func (k *Kernel) SetJobRetries(jobID string, retries int) error {
	j, err := k.ctx.Job(jobID)
	if err != nil {
		return errs.NewNotFound("Job %s", jobID)
	}
	j.Retries = retries
	j.LockOwner = ""
	j.LockExpTime = nil
	j.ExceptionMsg = ""
	due := k.now()
	j.DueDate = &due
	k.ctx.UpdateJob(j)

	if inc, _ := persistence.SelectIncidentByJob(k.ctx.Tx, jobID); inc != nil {
		if err := persistence.DeleteIncident(k.ctx.Tx, inc.ID); err != nil {
			return err
		}
		k.hiIncidentEnd(inc.ID)
	}
	return nil
}

// AcquireJobs 获取到期 Job（AcquireJobsCmd）：锁定排他语义 = 同实例最多一个排他 Job 在执行。
func (k *Kernel) AcquireJobs(workerID string, limit int, lockTime time.Duration) ([]*persistence.Job, error) {
	now := k.now()
	nowStr := persistence.FormatTime(now)
	due, err := persistence.SelectDueJobs(k.ctx.Tx, nowStr, limit*3)
	if err != nil {
		return nil, err
	}
	lockedInsts, err := persistence.SelectLockedExclusiveJobInstances(k.ctx.Tx, nowStr, workerID)
	if err != nil {
		return nil, err
	}
	var acquired []*persistence.Job
	for _, j := range due {
		if len(acquired) >= limit {
			break
		}
		if j.Exclusive && lockedInsts[j.ProcessInstID] {
			continue // 实例已有排他 Job 在执行 → 跳过（集群安全语义）
		}
		if j.LockOwner == workerID && j.LockExpTime != nil && j.LockExpTime.After(now) {
			continue // 自己已锁定的（正在执行）
		}
		exp := now.Add(lockTime)
		j.LockOwner, j.LockExpTime = workerID, &exp
		k.ctx.UpdateJob(j)
		acquired = append(acquired, j)
		if j.Exclusive && j.ProcessInstID != "" {
			lockedInsts[j.ProcessInstID] = true
		}
	}
	return acquired, nil
}
