package persistence

import "time"

// ===== Runtime 实体（对应 ACT_RU_* 表）=====
// 每个实体对应 Camunda 的 EntityImpl，REV_ 用于乐观锁（Camunda 的 PersistentObject）。

// Execution 对应 ExecutionEntity / ACT_RU_EXECUTION 一行。
// 执行树：PARENT_ID_ 构成树；作用域执行(IS_SCOPE_)代表一个作用域（流程/子流程/多实例体/事件作用域），
// 并发执行(IS_CONCURRENT_)是并行网关 fork 出的令牌。
type Execution struct {
	ID            string
	Rev           int
	ProcInstID    string
	BusinessKey   string
	ProcDefID     string
	ActivityID    string
	ActInstID     string
	ParentID      string
	SuperExec     string // 被调实例根执行 → 调用方执行（callActivity）
	IsActive      bool
	IsConcurrent  bool
	IsScope       bool
	IsEventScope  bool
	ScopeActivity string // 本执行代表的的作用域活动 id（子流程/调用活动/多实例体/带边界的活动），根执行为空
}

// Variable 对应 VariableInstanceEntity / ACT_RU_VARIABLE。
type Variable struct {
	ID           string
	Rev          int
	Type         string // string|integer|double|boolean|date|json|null|bytes
	Name         string
	ProcInstID   string
	ExecutionID  string
	TaskID       string
	ByteArrayID  string
	Double       *float64
	Long         *int64
	Text         string
	Text2        string // json 原文 / bytes 的字节表回链
	IsLocal      bool
}

// Task 对应 TaskEntity / ACT_RU_TASK。
type Task struct {
	ID           string
	Rev          int
	Name         string
	Assignee     string
	Created      time.Time
	Priority     int
	DueDate      *time.Time
	ProcDefID    string
	ProcInstID   string
	ExecutionID  string
	TaskDefKey   string
}

// IdentityLink 对应 IdentityLinkEntity / ACT_RU_IDENTITYLINK（mini 仅存任务候选）。
type IdentityLink struct {
	ID       string
	Type     string // candidate
	UserID   string
	GroupID  string
	TaskID   string
	ProcDefID string
}

// Job 对应 JobEntity / ACT_RU_JOB。TYPE_: message(异步延续) | timer(定时器)。
type Job struct {
	ID              string
	Rev             int
	Type            string
	LockOwner       string
	LockExpTime     *time.Time
	Exclusive       bool
	ExecutionID     string
	ProcessInstID   string
	ProcDefID       string
	Retries         int
	DueDate         *time.Time
	HandlerType     string // async-continuation | timer-catch | timer-boundary | timer-start
	HandlerCfg      string // JSON payload
	ExceptionMsg    string
	JobDefID        string
}

// JobDef 对应 JobDefinitionEntity / ACT_RU_JOBDEF。
type JobDef struct {
	ID            string
	ProcDefID     string
	ProcDefKey    string
	ActID         string
	JobType       string
	JobConfig     string
}

// EventSubscription 对应 EventSubscriptionEntity / ACT_RU_EVENT_SUBSCR。
type EventSubscription struct {
	ID            string
	Rev           int
	EventType     string // message | signal
	EventName     string
	ExecutionID   string
	ProcInstID    string
	ActivityID    string
	Configuration string
}

// ExtTask 对应 ExternalTaskEntity / ACT_RU_EXT_TASK。
type ExtTask struct {
	ID           string
	Rev          int
	Topic        string
	WorkerID     string
	LockExpTime  *time.Time
	Retries      int
	ErrorMessage string
	ExecutionID  string
	ProcInstID   string
	ProcDefID    string
	ActID        string
}

// Incident 对应 IncidentEntity / ACT_RU_INCIDENT。
type Incident struct {
	ID              string
	Type            string // failedJob | failedExternalTask
	Msg             string
	Timestamp       time.Time
	ExecutionID     string
	ActivityID      string
	ProcInstID      string
	ProcDefID       string
	CauseIncidentID string
	JobID           string
}

// ===== Repository 实体 =====

type Deployment struct {
	ID        string
	Name      string
	DeployTime time.Time
}

type ProcDefInfo struct {
	ID           string
	Key          string
	Version      int
	DeploymentID string
	ResourceName string
}

type ByteArray struct {
	ID           string
	Name         string
	DeploymentID string
	Bytes        []byte
}

// ===== History 实体（对应 ACT_HI_* 表）=====

type HiProcInst struct {
	ID            string // = ProcInstID
	BusinessKey   string
	ProcDefKey    string
	ProcDefID     string
	StartTime     time.Time
	EndTime       *time.Time
	EndActID      string
	DeleteReason  string
	State         string // running | completed | internally-terminated | errored
}

type HiActInst struct {
	ID              string
	ParentActInstID string
	ProcDefKey      string
	ProcDefID       string
	ProcInstID      string
	ExecutionID     string
	ActID           string
	ActName         string
	ActType         string
	Assignee        string
	StartTime       time.Time
	EndTime         *time.Time
	Duration        *int64
	ActInstState    string // running | completed | canceled
}

type HiTask struct {
	ID           string // = TaskID
	ProcDefKey   string
	ProcDefID    string
	ProcInstID   string
	ExecutionID  string
	ActInstID    string
	Name         string
	Assignee     string
	StartTime    time.Time
	EndTime      *time.Time
	Duration     *int64
	TaskDefKey   string
	DeleteReason string
}

type HiVarInst struct {
	ID           string
	ProcInstID   string
	ExecutionID  string
	TaskID       string
	Name         string
	VarType      string
	Text         string
	Long         *int64
	Double       *float64
	State        string // created | updated | deleted
	CreateTime   time.Time
	LastUpdated  time.Time
}

type HiJobLog struct {
	ID           string
	Timestamp    time.Time
	JobID        string
	JobDueDate   *time.Time
	JobRetries   int
	JobException string
	JobDefID     string
	State        string // created | succeeded | failed | deleted
}

type HiIncident struct {
	ID              string // = IncidentID
	CreateTime      time.Time
	EndTime         *time.Time
	IncidentType    string
	IncidentMsg     string
	ExecutionID     string
	ActivityID      string
	ProcInstID      string
	CauseIncidentID string
}
