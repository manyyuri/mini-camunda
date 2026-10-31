package persistence

import (
	"database/sql"
	"fmt"
)

// 手写 SQL Mapper，语义对应 Camunda MyBatis mapper 的 statement。

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func i2b(i int) bool { return i != 0 }

// ===== ACT_RU_EXECUTION =====
// SCOPE_ACTIVITY_ 为 mini 扩展列：Camunda 无此列（作用域关系在内存中推导），
// mini 持久化它以便重启后重建执行树作用域结构。
const execCols = `ID_, REV_, PROC_INST_ID_, BUSINESS_KEY_, PROC_DEF_ID_, ACT_ID_, ACT_INST_ID_, PARENT_ID_, SUPER_EXEC_, SCOPE_ACTIVITY_, IS_ACTIVE_, IS_CONCURRENT_, IS_SCOPE_, IS_EVENT_SCOPE_`

func scanExec(s scanner) (*Execution, error) {
	e := &Execution{}
	var bk, act, actInst, parent, super, scopeAct sql.NullString
	var active, conc, scope, eventScope sql.NullInt64
	if err := s.Scan(&e.ID, &e.Rev, &e.ProcInstID, &bk, &e.ProcDefID, &act, &actInst, &parent, &super, &scopeAct, &active, &conc, &scope, &eventScope); err != nil {
		return nil, err
	}
	e.BusinessKey, e.ActivityID, e.ActInstID, e.ParentID, e.SuperExec, e.ScopeActivity = bk.String, act.String, actInst.String, parent.String, super.String, scopeAct.String
	e.IsActive, e.IsConcurrent, e.IsScope, e.IsEventScope = i2b(int(active.Int64)), i2b(int(conc.Int64)), i2b(int(scope.Int64)), i2b(int(eventScope.Int64))
	return e, nil
}

type scanner interface{ Scan(...any) error }

func InsertExecution(db DBTX, e *Execution) error {
	e.Rev = 1
	_, err := db.Exec(`INSERT INTO ACT_RU_EXECUTION (`+execCols+`) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.ID, e.Rev, e.ProcInstID, e.BusinessKey, e.ProcDefID, e.ActivityID, e.ActInstID, e.ParentID, e.SuperExec, e.ScopeActivity,
		b2i(e.IsActive), b2i(e.IsConcurrent), b2i(e.IsScope), b2i(e.IsEventScope))
	return err
}

// UpdateExecution 乐观锁：WHERE ID_=? AND REV_=?，0 行 → OptimisticLockingException。
func UpdateExecution(db DBTX, e *Execution) error {
	res, err := db.Exec(`UPDATE ACT_RU_EXECUTION SET REV_=REV_+1, ACT_ID_=?, ACT_INST_ID_=?, SCOPE_ACTIVITY_=?, IS_ACTIVE_=?, IS_CONCURRENT_=?, IS_SCOPE_=?, IS_EVENT_SCOPE_=?
		WHERE ID_=? AND REV_=?`,
		e.ActivityID, e.ActInstID, e.ScopeActivity, b2i(e.IsActive), b2i(e.IsConcurrent), b2i(e.IsScope), b2i(e.IsEventScope), e.ID, e.Rev)
	if err != nil {
		return err
	}
	return checkRev(res, "Execution", e.ID)
}

func DeleteExecution(db DBTX, id string) error {
	_, err := db.Exec(`DELETE FROM ACT_RU_EXECUTION WHERE ID_=?`, id)
	return err
}

func SelectExecution(db DBTX, id string) (*Execution, error) {
	row := db.QueryRow(`SELECT `+execCols+` FROM ACT_RU_EXECUTION WHERE ID_=?`, id)
	return scanExec(row)
}

func SelectExecutionsByProcInstance(db DBTX, procInstID string) ([]*Execution, error) {
	rows, err := db.Query(`SELECT `+execCols+` FROM ACT_RU_EXECUTION WHERE PROC_INST_ID_=? ORDER BY ID_`, procInstID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Execution
	for rows.Next() {
		e, err := scanExec(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func SelectExecutionsByParent(db DBTX, parentID string) ([]*Execution, error) {
	rows, err := db.Query(`SELECT `+execCols+` FROM ACT_RU_EXECUTION WHERE PARENT_ID_=? ORDER BY ID_`, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Execution
	for rows.Next() {
		e, err := scanExec(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func SelectExecutionBySuperExec(db DBTX, superExecID string) (*Execution, error) {
	row := db.QueryRow(`SELECT `+execCols+` FROM ACT_RU_EXECUTION WHERE SUPER_EXEC_=?`, superExecID)
	e, err := scanExec(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return e, err
}

// SelectExecutionsByActID 找"停在指定活动上的活动执行"（join 检测用）。
func SelectExecutionsByActIDAndInstance(db DBTX, procInstID, actID string) ([]*Execution, error) {
	rows, err := db.Query(`SELECT `+execCols+` FROM ACT_RU_EXECUTION WHERE PROC_INST_ID_=? AND ACT_ID_=? AND IS_ACTIVE_=1 ORDER BY ID_`, procInstID, actID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Execution
	for rows.Next() {
		e, err := scanExec(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// SelectExecutions 通用运行实例查询。
func SelectExecutions(db DBTX, procInstID string) ([]*Execution, error) {
	if procInstID != "" {
		return SelectExecutionsByProcInstance(db, procInstID)
	}
	rows, err := db.Query(`SELECT ` + execCols + ` FROM ACT_RU_EXECUTION ORDER BY ID_`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Execution
	for rows.Next() {
		e, err := scanExec(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ===== ACT_RU_VARIABLE =====

const varCols = `ID_, REV_, TYPE_, NAME_, PROC_INST_ID_, EXECUTION_ID_, TASK_ID_, BYTEARRAY_ID_, DOUBLE_, LONG_, TEXT_, TEXT2_, IS_LOCAL_`

func scanVar(s scanner) (*Variable, error) {
	v := &Variable{}
	var execID, taskID, baID sql.NullString
	var dbl sql.NullFloat64
	var lng sql.NullInt64
	var text, text2 sql.NullString
	if err := s.Scan(&v.ID, &v.Rev, &v.Type, &v.Name, &v.ProcInstID, &execID, &taskID, &baID, &dbl, &lng, &text, &text2, &v.IsLocal); err != nil {
		return nil, err
	}
	v.ExecutionID, v.TaskID, v.ByteArrayID = execID.String, taskID.String, baID.String
	if dbl.Valid {
		f := dbl.Float64
		v.Double = &f
	}
	if lng.Valid {
		l := lng.Int64
		v.Long = &l
	}
	v.Text, v.Text2 = text.String, text2.String
	return v, nil
}

func InsertVariable(db DBTX, v *Variable) error {
	v.Rev = 1
	_, err := db.Exec(`INSERT INTO ACT_RU_VARIABLE (`+varCols+`) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		v.ID, v.Rev, v.Type, v.Name, v.ProcInstID, v.ExecutionID, v.TaskID, v.ByteArrayID, v.Double, v.Long, v.Text, v.Text2, b2i(v.IsLocal))
	return err
}

func UpdateVariable(db DBTX, v *Variable) error {
	res, err := db.Exec(`UPDATE ACT_RU_VARIABLE SET REV_=REV_+1, TYPE_=?, DOUBLE_=?, LONG_=?, TEXT_=?, TEXT2_=? WHERE ID_=? AND REV_=?`,
		v.Type, v.Double, v.Long, v.Text, v.Text2, v.ID, v.Rev)
	if err != nil {
		return err
	}
	return checkRev(res, "Variable", v.ID)
}

func DeleteVariable(db DBTX, id string) error {
	_, err := db.Exec(`DELETE FROM ACT_RU_VARIABLE WHERE ID_=?`, id)
	return err
}

func SelectVariable(db DBTX, id string) (*Variable, error) {
	row := db.QueryRow(`SELECT `+varCols+` FROM ACT_RU_VARIABLE WHERE ID_=?`, id)
	return scanVar(row)
}

// SelectVariablesByExecution 指定执行的局部变量。
func SelectVariablesByExecution(db DBTX, execID string) ([]*Variable, error) {
	rows, err := db.Query(`SELECT `+varCols+` FROM ACT_RU_VARIABLE WHERE EXECUTION_ID_=? ORDER BY NAME_`, execID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Variable
	for rows.Next() {
		v, err := scanVar(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func SelectVariablesByProcInstance(db DBTX, procInstID string) ([]*Variable, error) {
	rows, err := db.Query(`SELECT `+varCols+` FROM ACT_RU_VARIABLE WHERE PROC_INST_ID_=? ORDER BY NAME_, ID_`, procInstID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Variable
	for rows.Next() {
		v, err := scanVar(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func SelectVariablesByTask(db DBTX, taskID string) ([]*Variable, error) {
	rows, err := db.Query(`SELECT `+varCols+` FROM ACT_RU_VARIABLE WHERE TASK_ID_=? ORDER BY NAME_`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Variable
	for rows.Next() {
		v, err := scanVar(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ===== ACT_RU_TASK =====

const taskCols = `ID_, REV_, NAME_, ASSIGNEE_, CREATED_, PRIORITY_, DUE_DATE_, PROC_DEF_ID_, PROC_INST_ID_, EXECUTION_ID_, TASK_DEF_KEY_`

func scanTask(s scanner) (*Task, error) {
	t := &Task{}
	var created string
	var due sql.NullString
	if err := s.Scan(&t.ID, &t.Rev, &t.Name, &t.Assignee, &created, &t.Priority, &due, &t.ProcDefID, &t.ProcInstID, &t.ExecutionID, &t.TaskDefKey); err != nil {
		return nil, err
	}
	t.Created = MustParseTime(created)
	if due.Valid && due.String != "" {
		d := MustParseTime(due.String)
		t.DueDate = &d
	}
	return t, nil
}

func InsertTask(db DBTX, t *Task) error {
	t.Rev = 1
	var due any
	if t.DueDate != nil {
		due = FormatTime(*t.DueDate)
	}
	_, err := db.Exec(`INSERT INTO ACT_RU_TASK (`+taskCols+`) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		t.ID, t.Rev, t.Name, t.Assignee, FormatTime(t.Created), t.Priority, due, t.ProcDefID, t.ProcInstID, t.ExecutionID, t.TaskDefKey)
	return err
}

func UpdateTask(db DBTX, t *Task) error {
	var due any
	if t.DueDate != nil {
		due = FormatTime(*t.DueDate)
	}
	res, err := db.Exec(`UPDATE ACT_RU_TASK SET REV_=REV_+1, NAME_=?, ASSIGNEE_=?, PRIORITY_=?, DUE_DATE_=? WHERE ID_=? AND REV_=?`,
		t.Name, t.Assignee, t.Priority, due, t.ID, t.Rev)
	if err != nil {
		return err
	}
	return checkRev(res, "Task", t.ID)
}

func DeleteTask(db DBTX, id string) error {
	_, err := db.Exec(`DELETE FROM ACT_RU_TASK WHERE ID_=?`, id)
	return err
}

func SelectTask(db DBTX, id string) (*Task, error) {
	row := db.QueryRow(`SELECT `+taskCols+` FROM ACT_RU_TASK WHERE ID_=?`, id)
	return scanTask(row)
}

type TaskFilter struct {
	Assignee      string
	CandidateGroup string
	CandidateUser  string
	ProcInstID    string
	Active        bool
}

func SelectTasks(db DBTX, f TaskFilter) ([]*Task, error) {
	q := `SELECT ` + taskCols + ` FROM ACT_RU_TASK t WHERE 1=1`
	var args []any
	if f.Assignee != "" {
		q += ` AND t.ASSIGNEE_=?`
		args = append(args, f.Assignee)
	}
	if f.ProcInstID != "" {
		q += ` AND t.PROC_INST_ID_=?`
		args = append(args, f.ProcInstID)
	}
	if f.CandidateGroup != "" {
		q += ` AND EXISTS(SELECT 1 FROM ACT_RU_IDENTITYLINK l WHERE l.TASK_ID_=t.ID_ AND l.TYPE_='candidate' AND l.GROUP_ID_=?)`
		args = append(args, f.CandidateGroup)
	}
	if f.CandidateUser != "" {
		q += ` AND EXISTS(SELECT 1 FROM ACT_RU_IDENTITYLINK l WHERE l.TASK_ID_=t.ID_ AND l.TYPE_='candidate' AND l.USER_ID_=?)`
		args = append(args, f.CandidateUser)
	}
	q += ` ORDER BY t.CREATED_`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func SelectTasksByProcInstance(db DBTX, procInstID string) ([]*Task, error) {
	return SelectTasks(db, TaskFilter{ProcInstID: procInstID})
}

// ===== ACT_RU_IDENTITYLINK =====

func InsertIdentityLink(db DBTX, l *IdentityLink) error {
	_, err := db.Exec(`INSERT INTO ACT_RU_IDENTITYLINK(ID_, TYPE_, USER_ID_, GROUP_ID_, TASK_ID_, PROC_DEF_ID_) VALUES(?,?,?,?,?,?)`,
		l.ID, l.Type, l.UserID, l.GroupID, l.TaskID, l.ProcDefID)
	return err
}

func DeleteIdentityLinksByTask(db DBTX, taskID string) error {
	_, err := db.Exec(`DELETE FROM ACT_RU_IDENTITYLINK WHERE TASK_ID_=?`, taskID)
	return err
}

// ===== ACT_RU_JOB / JOBDEF =====

const jobCols = `ID_, REV_, TYPE_, LOCK_OWNER_, LOCK_EXP_TIME_, EXCLUSIVE_, EXECUTION_ID_, PROCESS_INSTANCE_ID_, PROC_DEF_ID_, RETRIES_, DUEDATE_, HANDLER_TYPE_, HANDLER_CFG_, EXCEPTION_MSG_, JOB_DEF_ID_`

func scanJob(s scanner) (*Job, error) {
	j := &Job{}
	var lockOwner sql.NullString
	var lockExp, due sql.NullString
	var excl int
	var execID, procInst, procDef, cfg, excMsg, jobDefID sql.NullString
	if err := s.Scan(&j.ID, &j.Rev, &j.Type, &lockOwner, &lockExp, &excl, &execID, &procInst, &procDef, &j.Retries, &due, &j.HandlerType, &cfg, &excMsg, &jobDefID); err != nil {
		return nil, err
	}
	j.LockOwner = lockOwner.String
	if lockExp.Valid && lockExp.String != "" {
		t := MustParseTime(lockExp.String)
		j.LockExpTime = &t
	}
	j.Exclusive = i2b(excl)
	j.ExecutionID, j.ProcessInstID, j.ProcDefID = execID.String, procInst.String, procDef.String
	if due.Valid && due.String != "" {
		t := MustParseTime(due.String)
		j.DueDate = &t
	}
	j.HandlerCfg, j.ExceptionMsg, j.JobDefID = cfg.String, excMsg.String, jobDefID.String
	return j, nil
}

func InsertJob(db DBTX, j *Job) error {
	j.Rev = 1
	var lockExp, due any
	if j.LockExpTime != nil {
		lockExp = FormatTime(*j.LockExpTime)
	}
	if j.DueDate != nil {
		due = FormatTime(*j.DueDate)
	}
	_, err := db.Exec(`INSERT INTO ACT_RU_JOB (`+jobCols+`) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.Rev, j.Type, j.LockOwner, lockExp, b2i(j.Exclusive), j.ExecutionID, j.ProcessInstID, j.ProcDefID, j.Retries, due, j.HandlerType, j.HandlerCfg, j.ExceptionMsg, j.JobDefID)
	return err
}

func UpdateJob(db DBTX, j *Job) error {
	var lockExp, due any
	if j.LockExpTime != nil {
		lockExp = FormatTime(*j.LockExpTime)
	}
	if j.DueDate != nil {
		due = FormatTime(*j.DueDate)
	}
	res, err := db.Exec(`UPDATE ACT_RU_JOB SET REV_=REV_+1, LOCK_OWNER_=?, LOCK_EXP_TIME_=?, RETRIES_=?, DUEDATE_=?, EXCEPTION_MSG_=? WHERE ID_=? AND REV_=?`,
		j.LockOwner, lockExp, j.Retries, due, j.ExceptionMsg, j.ID, j.Rev)
	if err != nil {
		return err
	}
	return checkRev(res, "Job", j.ID)
}

func DeleteJob(db DBTX, id string) error {
	_, err := db.Exec(`DELETE FROM ACT_RU_JOB WHERE ID_=?`, id)
	return err
}

func SelectJob(db DBTX, id string) (*Job, error) {
	row := db.QueryRow(`SELECT `+jobCols+` FROM ACT_RU_JOB WHERE ID_=?`, id)
	return scanJob(row)
}

// SelectDueJobs 获取到期未锁（或锁过期）的 Job —— 对应 AcquireJobsCmd 的查询。
func SelectDueJobs(db DBTX, now string, limit int) ([]*Job, error) {
	rows, err := db.Query(`SELECT `+jobCols+` FROM ACT_RU_JOB
		WHERE (LOCK_EXP_TIME_ IS NULL OR LOCK_EXP_TIME_=? OR LOCK_EXP_TIME_<?)
		  AND (DUEDATE_ IS NULL OR DUEDATE_<=?)
		ORDER BY DUEDATE_ LIMIT ?`, now, now, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// SelectLockedExclusiveJobInstances 已被锁定排他 Job 占用的流程实例集合。
func SelectLockedExclusiveJobInstances(db DBTX, now string, owner string) (map[string]bool, error) {
	rows, err := db.Query(`SELECT DISTINCT PROCESS_INSTANCE_ID_ FROM ACT_RU_JOB
		WHERE EXCLUSIVE_=1 AND LOCK_EXP_TIME_ IS NOT NULL AND LOCK_EXP_TIME_>? AND LOCK_OWNER_<>?`, now, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var inst sql.NullString
		if err := rows.Scan(&inst); err != nil {
			return nil, err
		}
		if inst.String != "" {
			out[inst.String] = true
		}
	}
	return out, rows.Err()
}

func SelectJobs(db DBTX, procInstID string) ([]*Job, error) {
	q := `SELECT ` + jobCols + ` FROM ACT_RU_JOB`
	var args []any
	if procInstID != "" {
		q += ` WHERE PROCESS_INSTANCE_ID_=?`
		args = append(args, procInstID)
	}
	q += ` ORDER BY ID_`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func InsertJobDef(db DBTX, d *JobDef) error {
	_, err := db.Exec(`INSERT INTO ACT_RU_JOBDEF(ID_, PROC_DEF_ID_, PROC_DEF_KEY_, ACT_ID_, JOB_TYPE_, JOB_CONFIGURATION_) VALUES(?,?,?,?,?,?)`,
		d.ID, d.ProcDefID, d.ProcDefKey, d.ActID, d.JobType, d.JobConfig)
	return err
}

func SelectJobDef(db DBTX, id string) (*JobDef, error) {
	row := db.QueryRow(`SELECT ID_, PROC_DEF_ID_, PROC_DEF_KEY_, ACT_ID_, JOB_TYPE_, JOB_CONFIGURATION_ FROM ACT_RU_JOBDEF WHERE ID_=?`, id)
	d := &JobDef{}
	err := row.Scan(&d.ID, &d.ProcDefID, &d.ProcDefKey, &d.ActID, &d.JobType, &d.JobConfig)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return d, err
}

func SelectJobDefByKey(db DBTX, procDefID, actID, jobType, jobConfig string) (*JobDef, error) {
	row := db.QueryRow(`SELECT ID_, PROC_DEF_ID_, PROC_DEF_KEY_, ACT_ID_, JOB_TYPE_, JOB_CONFIGURATION_ FROM ACT_RU_JOBDEF
		WHERE PROC_DEF_ID_=? AND ACT_ID_=? AND JOB_TYPE_=? AND JOB_CONFIGURATION_=?`, procDefID, actID, jobType, jobConfig)
	d := &JobDef{}
	err := row.Scan(&d.ID, &d.ProcDefID, &d.ProcDefKey, &d.ActID, &d.JobType, &d.JobConfig)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return d, err
}

// ===== ACT_RU_EVENT_SUBSCR =====

const subCols = `ID_, REV_, EVENT_TYPE_, EVENT_NAME_, EXECUTION_ID_, PROC_INST_ID_, ACTIVITY_ID_, CONFIGURATION_`

func scanSub(s scanner) (*EventSubscription, error) {
	e := &EventSubscription{}
	var cfg sql.NullString
	if err := s.Scan(&e.ID, &e.Rev, &e.EventType, &e.EventName, &e.ExecutionID, &e.ProcInstID, &e.ActivityID, &cfg); err != nil {
		return nil, err
	}
	e.Configuration = cfg.String
	return e, nil
}

func InsertEventSubscription(db DBTX, e *EventSubscription) error {
	e.Rev = 1
	_, err := db.Exec(`INSERT INTO ACT_RU_EVENT_SUBSCR (`+subCols+`) VALUES(?,?,?,?,?,?,?,?)`,
		e.ID, e.Rev, e.EventType, e.EventName, e.ExecutionID, e.ProcInstID, e.ActivityID, e.Configuration)
	return err
}

func DeleteEventSubscription(db DBTX, id string) error {
	_, err := db.Exec(`DELETE FROM ACT_RU_EVENT_SUBSCR WHERE ID_=?`, id)
	return err
}

func SelectEventSubscriptionsByName(db DBTX, eventType, name string) ([]*EventSubscription, error) {
	rows, err := db.Query(`SELECT `+subCols+` FROM ACT_RU_EVENT_SUBSCR WHERE EVENT_TYPE_=? AND EVENT_NAME_=? ORDER BY ID_`, eventType, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*EventSubscription
	for rows.Next() {
		s, err := scanSub(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func SelectEventSubscriptionsByExecution(db DBTX, execID string) ([]*EventSubscription, error) {
	rows, err := db.Query(`SELECT `+subCols+` FROM ACT_RU_EVENT_SUBSCR WHERE EXECUTION_ID_=?`, execID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*EventSubscription
	for rows.Next() {
		s, err := scanSub(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func SelectEventSubscriptionsByInstance(db DBTX, procInstID string) ([]*EventSubscription, error) {
	rows, err := db.Query(`SELECT `+subCols+` FROM ACT_RU_EVENT_SUBSCR WHERE PROC_INST_ID_=? ORDER BY ID_`, procInstID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*EventSubscription
	for rows.Next() {
		s, err := scanSub(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ===== ACT_RU_EXT_TASK =====

const extCols = `ID_, REV_, TOPIC_NAME_, WORKER_ID_, LOCK_EXP_TIME_, RETRIES_, ERROR_MESSAGE_, EXECUTION_ID_, PROC_INST_ID_, PROC_DEF_ID_, ACT_ID_`

func scanExt(s scanner) (*ExtTask, error) {
	e := &ExtTask{}
	var worker, lockExp, errMsg sql.NullString
	if err := s.Scan(&e.ID, &e.Rev, &e.Topic, &worker, &lockExp, &e.Retries, &errMsg, &e.ExecutionID, &e.ProcInstID, &e.ProcDefID, &e.ActID); err != nil {
		return nil, err
	}
	e.WorkerID, e.ErrorMessage = worker.String, errMsg.String
	if lockExp.Valid && lockExp.String != "" {
		t := MustParseTime(lockExp.String)
		e.LockExpTime = &t
	}
	return e, nil
}

func InsertExtTask(db DBTX, e *ExtTask) error {
	e.Rev = 1
	var lockExp any
	if e.LockExpTime != nil {
		lockExp = FormatTime(*e.LockExpTime)
	}
	_, err := db.Exec(`INSERT INTO ACT_RU_EXT_TASK (`+extCols+`) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		e.ID, e.Rev, e.Topic, e.WorkerID, lockExp, e.Retries, e.ErrorMessage, e.ExecutionID, e.ProcInstID, e.ProcDefID, e.ActID)
	return err
}

func UpdateExtTask(db DBTX, e *ExtTask) error {
	var lockExp any
	if e.LockExpTime != nil {
		lockExp = FormatTime(*e.LockExpTime)
	}
	res, err := db.Exec(`UPDATE ACT_RU_EXT_TASK SET REV_=REV_+1, WORKER_ID_=?, LOCK_EXP_TIME_=?, RETRIES_=?, ERROR_MESSAGE_=? WHERE ID_=? AND REV_=?`,
		e.WorkerID, lockExp, e.Retries, e.ErrorMessage, e.ID, e.Rev)
	if err != nil {
		return err
	}
	return checkRev(res, "ExtTask", e.ID)
}

func DeleteExtTask(db DBTX, id string) error {
	_, err := db.Exec(`DELETE FROM ACT_RU_EXT_TASK WHERE ID_=?`, id)
	return err
}

func SelectExtTask(db DBTX, id string) (*ExtTask, error) {
	row := db.QueryRow(`SELECT `+extCols+` FROM ACT_RU_EXT_TASK WHERE ID_=?`, id)
	return scanExt(row)
}

// SelectAvailableExtTasks 可被 fetch 的外部任务（未锁或锁过期，且有剩余重试）。
func SelectAvailableExtTasks(db DBTX, topic, now string) ([]*ExtTask, error) {
	rows, err := db.Query(`SELECT `+extCols+` FROM ACT_RU_EXT_TASK
		WHERE TOPIC_NAME_=? AND RETRIES_>0 AND (LOCK_EXP_TIME_ IS NULL OR LOCK_EXP_TIME_<?) ORDER BY ID_`, topic, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ExtTask
	for rows.Next() {
		e, err := scanExt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func SelectExtTasks(db DBTX, topic string) ([]*ExtTask, error) {
	q := `SELECT ` + extCols + ` FROM ACT_RU_EXT_TASK`
	var args []any
	if topic != "" {
		q += ` WHERE TOPIC_NAME_=?`
		args = append(args, topic)
	}
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ExtTask
	for rows.Next() {
		e, err := scanExt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ===== ACT_RU_INCIDENT =====

func InsertIncident(db DBTX, i *Incident) error {
	_, err := db.Exec(`INSERT INTO ACT_RU_INCIDENT(ID_, INCIDENT_TYPE_, INCIDENT_MSG_, INCIDENT_TIMESTAMP_, EXECUTION_ID_, ACTIVITY_ID_, PROC_INST_ID_, PROC_DEF_ID_, CAUSE_INCIDENT_ID_, JOB_ID_)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		i.ID, i.Type, i.Msg, FormatTime(i.Timestamp), i.ExecutionID, i.ActivityID, i.ProcInstID, i.ProcDefID, i.CauseIncidentID, i.JobID)
	return err
}

func DeleteIncident(db DBTX, id string) error {
	_, err := db.Exec(`DELETE FROM ACT_RU_INCIDENT WHERE ID_=?`, id)
	return err
}

func SelectIncidentByJob(db DBTX, jobID string) (*Incident, error) {
	row := db.QueryRow(`SELECT ID_, INCIDENT_TYPE_, INCIDENT_MSG_, INCIDENT_TIMESTAMP_, EXECUTION_ID_, ACTIVITY_ID_, PROC_INST_ID_, PROC_DEF_ID_, CAUSE_INCIDENT_ID_, JOB_ID_ FROM ACT_RU_INCIDENT WHERE JOB_ID_=?`, jobID)
	i := &Incident{}
	var msg, execID, actID, cause sql.NullString
	var ts sql.NullString
	if err := row.Scan(&i.ID, &i.Type, &msg, &ts, &execID, &actID, &i.ProcInstID, &i.ProcDefID, &cause, &i.JobID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	i.Msg, i.ExecutionID, i.ActivityID, i.CauseIncidentID = msg.String, execID.String, actID.String, cause.String
	i.Timestamp = MustParseTime(ts.String)
	return i, nil
}

func SelectIncidents(db DBTX, procInstID string) ([]*Incident, error) {
	q := `SELECT ID_, INCIDENT_TYPE_, INCIDENT_MSG_, INCIDENT_TIMESTAMP_, EXECUTION_ID_, ACTIVITY_ID_, PROC_INST_ID_, PROC_DEF_ID_, CAUSE_INCIDENT_ID_, JOB_ID_ FROM ACT_RU_INCIDENT`
	var args []any
	if procInstID != "" {
		q += ` WHERE PROC_INST_ID_=?`
		args = append(args, procInstID)
	}
	q += ` ORDER BY ID_`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Incident
	for rows.Next() {
		i := &Incident{}
		var msg, execID, actID, cause, ts sql.NullString
		if err := rows.Scan(&i.ID, &i.Type, &msg, &ts, &execID, &actID, &i.ProcInstID, &i.ProcDefID, &cause, &i.JobID); err != nil {
			return nil, err
		}
		i.Msg, i.ExecutionID, i.ActivityID, i.CauseIncidentID = msg.String, execID.String, actID.String, cause.String
		i.Timestamp = MustParseTime(ts.String)
		out = append(out, i)
	}
	return out, rows.Err()
}

// ===== Repository =====

func InsertDeployment(db DBTX, d *Deployment) error {
	_, err := db.Exec(`INSERT INTO ACT_RE_DEPLOYMENT(ID_, NAME_, DEPLOY_TIME_) VALUES(?,?,?)`, d.ID, d.Name, FormatTime(d.DeployTime))
	return err
}

func SelectDeployment(db DBTX, id string) (*Deployment, error) {
	row := db.QueryRow(`SELECT ID_, NAME_, DEPLOY_TIME_ FROM ACT_RE_DEPLOYMENT WHERE ID_=?`, id)
	d := &Deployment{}
	var name sql.NullString
	var ts string
	if err := row.Scan(&d.ID, &name, &ts); err != nil {
		return nil, err
	}
	d.Name = name.String
	d.DeployTime = MustParseTime(ts)
	return d, nil
}

func SelectDeployments(db DBTX) ([]*Deployment, error) {
	rows, err := db.Query(`SELECT ID_, NAME_, DEPLOY_TIME_ FROM ACT_RE_DEPLOYMENT ORDER BY DEPLOY_TIME_`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Deployment
	for rows.Next() {
		d := &Deployment{}
		var name sql.NullString
		var ts string
		if err := rows.Scan(&d.ID, &name, &ts); err != nil {
			return nil, err
		}
		d.Name = name.String
		d.DeployTime = MustParseTime(ts)
		out = append(out, d)
	}
	return out, rows.Err()
}

func InsertByteArray(db DBTX, b *ByteArray) error {
	_, err := db.Exec(`INSERT INTO ACT_GE_BYTEARRAY(ID_, NAME_, DEPLOYMENT_ID_, BYTES_) VALUES(?,?,?,?)`, b.ID, b.Name, b.DeploymentID, b.Bytes)
	return err
}

func SelectByteArray(db DBTX, id string) (*ByteArray, error) {
	row := db.QueryRow(`SELECT ID_, NAME_, DEPLOYMENT_ID_, BYTES_ FROM ACT_GE_BYTEARRAY WHERE ID_=?`, id)
	b := &ByteArray{}
	var name, dep sql.NullString
	if err := row.Scan(&b.ID, &name, &dep, &b.Bytes); err != nil {
		return nil, err
	}
	b.Name, b.DeploymentID = name.String, dep.String
	return b, nil
}

func SelectByteArraysByDeployment(db DBTX, deploymentID string) ([]*ByteArray, error) {
	rows, err := db.Query(`SELECT ID_, NAME_, DEPLOYMENT_ID_, BYTES_ FROM ACT_GE_BYTEARRAY WHERE DEPLOYMENT_ID_=? ORDER BY NAME_`, deploymentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ByteArray
	for rows.Next() {
		b := &ByteArray{}
		var name, dep sql.NullString
		if err := rows.Scan(&b.ID, &name, &dep, &b.Bytes); err != nil {
			return nil, err
		}
		b.Name, b.DeploymentID = name.String, dep.String
		out = append(out, b)
	}
	return out, rows.Err()
}

func InsertProcDef(db DBTX, p *ProcDefInfo) error {
	_, err := db.Exec(`INSERT INTO ACT_RE_PROCDEF(ID_, KEY_, VERSION_, DEPLOYMENT_ID_, RESOURCE_NAME_, HAS_START_FORM_KEY_) VALUES(?,?,?,?,?,0)`,
		p.ID, p.Key, p.Version, p.DeploymentID, p.ResourceName)
	return err
}

func SelectProcDef(db DBTX, id string) (*ProcDefInfo, error) {
	row := db.QueryRow(`SELECT ID_, KEY_, VERSION_, DEPLOYMENT_ID_, RESOURCE_NAME_, HAS_START_FORM_KEY_ FROM ACT_RE_PROCDEF WHERE ID_=?`, id)
	p := &ProcDefInfo{}
	if err := row.Scan(&p.ID, &p.Key, &p.Version, &p.DeploymentID, &p.ResourceName, new(int)); err != nil {
		return nil, err
	}
	return p, nil
}

func SelectProcDefs(db DBTX, key string) ([]*ProcDefInfo, error) {
	q := `SELECT ID_, KEY_, VERSION_, DEPLOYMENT_ID_, RESOURCE_NAME_, HAS_START_FORM_KEY_ FROM ACT_RE_PROCDEF`
	var args []any
	if key != "" {
		q += ` WHERE KEY_=?`
		args = append(args, key)
	}
	q += ` ORDER BY KEY_, VERSION_`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ProcDefInfo
	for rows.Next() {
		p := &ProcDefInfo{}
		if err := rows.Scan(&p.ID, &p.Key, &p.Version, &p.DeploymentID, &p.ResourceName, new(int)); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ===== History =====

func InsertHiProcInst(db DBTX, h *HiProcInst) error {
	var end any
	if h.EndTime != nil {
		end = FormatTime(*h.EndTime)
	}
	_, err := db.Exec(`INSERT INTO ACT_HI_PROCINST(ID_, BUSINESS_KEY_, PROC_DEF_KEY_, PROC_DEF_ID_, START_TIME_, END_TIME_, END_ACT_ID_, DELETE_REASON_, STATE_)
		VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(ID_) DO UPDATE SET END_TIME_=excluded.END_TIME_, END_ACT_ID_=excluded.END_ACT_ID_, DELETE_REASON_=excluded.DELETE_REASON_, STATE_=excluded.STATE_`,
		h.ID, h.BusinessKey, h.ProcDefKey, h.ProcDefID, FormatTime(h.StartTime), end, h.EndActID, h.DeleteReason, h.State)
	return err
}

func SelectHiProcInsts(db DBTX, state, procDefKey string) ([]*HiProcInst, error) {
	q := `SELECT ID_, BUSINESS_KEY_, PROC_DEF_KEY_, PROC_DEF_ID_, START_TIME_, END_TIME_, END_ACT_ID_, DELETE_REASON_, STATE_ FROM ACT_HI_PROCINST WHERE 1=1`
	var args []any
	if state == "running" {
		q += ` AND STATE_='running'`
	} else if state == "completed" {
		q += ` AND STATE_='completed'`
	} else if state == "finished" {
		q += ` AND STATE_<>'running'`
	}
	if procDefKey != "" {
		q += ` AND PROC_DEF_KEY_=?`
		args = append(args, procDefKey)
	}
	q += ` ORDER BY START_TIME_`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*HiProcInst
	for rows.Next() {
		h := &HiProcInst{}
		var bk, end, endAct, reason sql.NullString
		var start string
		if err := rows.Scan(&h.ID, &bk, &h.ProcDefKey, &h.ProcDefID, &start, &end, &endAct, &reason, &h.State); err != nil {
			return nil, err
		}
		h.BusinessKey, h.EndActID, h.DeleteReason = bk.String, endAct.String, reason.String
		h.StartTime = MustParseTime(start)
		if end.Valid && end.String != "" {
			t := MustParseTime(end.String)
			h.EndTime = &t
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func InsertHiActInst(db DBTX, h *HiActInst) error {
	var end any
	var dur any
	if h.EndTime != nil {
		end = FormatTime(*h.EndTime)
	}
	if h.Duration != nil {
		dur = *h.Duration
	}
	_, err := db.Exec(`INSERT INTO ACT_HI_ACTINST(ID_, PARENT_ACT_INST_ID_, PROC_DEF_KEY_, PROC_DEF_ID_, PROC_INST_ID_, EXECUTION_ID_, ACT_ID_, ACT_NAME_, ACT_TYPE_, ASSIGNEE_, START_TIME_, END_TIME_, DURATION_, ACT_INST_STATE_)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(ID_) DO UPDATE SET END_TIME_=excluded.END_TIME_, DURATION_=excluded.DURATION_, ACT_INST_STATE_=excluded.ACT_INST_STATE_`,
		h.ID, h.ParentActInstID, h.ProcDefKey, h.ProcDefID, h.ProcInstID, h.ExecutionID, h.ActID, h.ActName, h.ActType, h.Assignee, FormatTime(h.StartTime), end, dur, h.ActInstState)
	return err
}

func SelectHiActInsts(db DBTX, procInstID string) ([]*HiActInst, error) {
	q := `SELECT ID_, PARENT_ACT_INST_ID_, PROC_DEF_KEY_, PROC_DEF_ID_, PROC_INST_ID_, EXECUTION_ID_, ACT_ID_, ACT_NAME_, ACT_TYPE_, ASSIGNEE_, START_TIME_, END_TIME_, DURATION_, ACT_INST_STATE_ FROM ACT_HI_ACTINST`
	var args []any
	if procInstID != "" {
		q += ` WHERE PROC_INST_ID_=?`
		args = append(args, procInstID)
	}
	q += ` ORDER BY START_TIME_, ID_`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*HiActInst
	for rows.Next() {
		h := &HiActInst{}
		var parent, assignee, end sql.NullString
		var start string
		var dur sql.NullInt64
		if err := rows.Scan(&h.ID, &parent, &h.ProcDefKey, &h.ProcDefID, &h.ProcInstID, &h.ExecutionID, &h.ActID, &h.ActName, &h.ActType, &assignee, &start, &end, &dur, &h.ActInstState); err != nil {
			return nil, err
		}
		h.ParentActInstID, h.Assignee = parent.String, assignee.String
		h.StartTime = MustParseTime(start)
		if end.Valid && end.String != "" {
			t := MustParseTime(end.String)
			h.EndTime = &t
		}
		if dur.Valid {
			d := dur.Int64
			h.Duration = &d
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func InsertHiTask(db DBTX, h *HiTask) error {
	var end, dur any
	if h.EndTime != nil {
		end = FormatTime(*h.EndTime)
	}
	if h.Duration != nil {
		dur = *h.Duration
	}
	_, err := db.Exec(`INSERT INTO ACT_HI_TASKINST(ID_, PROC_DEF_KEY_, PROC_DEF_ID_, PROC_INST_ID_, EXECUTION_ID_, ACT_INST_ID_, NAME_, ASSIGNEE_, START_TIME_, END_TIME_, DURATION_, TASK_DEF_KEY_, DELETE_REASON_)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(ID_) DO UPDATE SET END_TIME_=excluded.END_TIME_, DURATION_=excluded.DURATION_, DELETE_REASON_=excluded.DELETE_REASON_`,
		h.ID, h.ProcDefKey, h.ProcDefID, h.ProcInstID, h.ExecutionID, h.ActInstID, h.Name, h.Assignee, FormatTime(h.StartTime), end, dur, h.TaskDefKey, h.DeleteReason)
	return err
}

func SelectHiTasks(db DBTX, procInstID string) ([]*HiTask, error) {
	q := `SELECT ID_, PROC_DEF_KEY_, PROC_DEF_ID_, PROC_INST_ID_, EXECUTION_ID_, ACT_INST_ID_, NAME_, ASSIGNEE_, START_TIME_, END_TIME_, DURATION_, TASK_DEF_KEY_, DELETE_REASON_ FROM ACT_HI_TASKINST`
	var args []any
	if procInstID != "" {
		q += ` WHERE PROC_INST_ID_=?`
		args = append(args, procInstID)
	}
	q += ` ORDER BY START_TIME_`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*HiTask
	for rows.Next() {
		h := &HiTask{}
		var end sql.NullString
		var start string
		var dur sql.NullInt64
		if err := rows.Scan(&h.ID, &h.ProcDefKey, &h.ProcDefID, &h.ProcInstID, &h.ExecutionID, &h.ActInstID, &h.Name, &h.Assignee, &start, &end, &dur, &h.TaskDefKey, &h.DeleteReason); err != nil {
			return nil, err
		}
		h.StartTime = MustParseTime(start)
		if end.Valid && end.String != "" {
			t := MustParseTime(end.String)
			h.EndTime = &t
		}
		if dur.Valid {
			d := dur.Int64
			h.Duration = &d
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func InsertHiVarInst(db DBTX, h *HiVarInst) error {
	// 镜像 Camunda：ACT_HI_VARINST 每个变量实例一行，STATE_ 记录最新状态
	_, err := db.Exec(`INSERT INTO ACT_HI_VARINST(ID_, PROC_INST_ID_, EXECUTION_ID_, TASK_ID_, NAME_, VAR_TYPE_, TEXT_, LONG_, DOUBLE_, STATE_, CREATE_TIME_, LAST_UPDATED_TIME_)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(ID_) DO UPDATE SET STATE_=excluded.STATE_, TEXT_=excluded.TEXT_, LONG_=excluded.LONG_, DOUBLE_=excluded.DOUBLE_, LAST_UPDATED_TIME_=excluded.LAST_UPDATED_TIME_`,
		h.ID, h.ProcInstID, h.ExecutionID, h.TaskID, h.Name, h.VarType, h.Text, h.Long, h.Double, h.State, FormatTime(h.CreateTime), FormatTime(h.LastUpdated))
	return err
}

func SelectHiVarInsts(db DBTX, procInstID string) ([]*HiVarInst, error) {
	q := `SELECT ID_, PROC_INST_ID_, EXECUTION_ID_, TASK_ID_, NAME_, VAR_TYPE_, TEXT_, LONG_, DOUBLE_, STATE_, CREATE_TIME_, LAST_UPDATED_TIME_ FROM ACT_HI_VARINST`
	var args []any
	if procInstID != "" {
		q += ` WHERE PROC_INST_ID_=?`
		args = append(args, procInstID)
	}
	q += ` ORDER BY CREATE_TIME_, ID_`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*HiVarInst
	for rows.Next() {
		h := &HiVarInst{}
		var ct, lut string
		if err := rows.Scan(&h.ID, &h.ProcInstID, &h.ExecutionID, &h.TaskID, &h.Name, &h.VarType, &h.Text, &h.Long, &h.Double, &h.State, &ct, &lut); err != nil {
			return nil, err
		}
		h.CreateTime, h.LastUpdated = MustParseTime(ct), MustParseTime(lut)
		out = append(out, h)
	}
	return out, rows.Err()
}

func InsertHiJobLog(db DBTX, h *HiJobLog) error {
	var due any
	if h.JobDueDate != nil {
		due = FormatTime(*h.JobDueDate)
	}
	_, err := db.Exec(`INSERT INTO ACT_HI_JOBLOG(ID_, TIMESTAMP_, JOB_ID_, JOB_DUE_DATE_, JOB_RETRIES_, JOB_EXCEPTION_MSG_, JOB_DEF_ID_, STATE_) VALUES(?,?,?,?,?,?,?,?)`,
		h.ID, FormatTime(h.Timestamp), h.JobID, due, h.JobRetries, h.JobException, h.JobDefID, h.State)
	return err
}

func SelectHiJobLogs(db DBTX, procInstID string) ([]*HiJobLog, error) {
	// mini 的 joblog 不带实例列，全量返回（按时间排序）；注释说明与 Camunda 差异
	rows, err := db.Query(`SELECT ID_, TIMESTAMP_, JOB_ID_, JOB_DUE_DATE_, JOB_RETRIES_, JOB_EXCEPTION_MSG_, JOB_DEF_ID_, STATE_ FROM ACT_HI_JOBLOG ORDER BY TIMESTAMP_`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*HiJobLog
	for rows.Next() {
		h := &HiJobLog{}
		var ts string
		var due sql.NullString
		if err := rows.Scan(&h.ID, &ts, &h.JobID, &due, &h.JobRetries, &h.JobException, &h.JobDefID, &h.State); err != nil {
			return nil, err
		}
		h.Timestamp = MustParseTime(ts)
		if due.Valid && due.String != "" {
			t := MustParseTime(due.String)
			h.JobDueDate = &t
		}
		out = append(out, h)
	}
	_ = procInstID
	return out, rows.Err()
}

func InsertHiIncident(db DBTX, h *HiIncident) error {
	var end any
	if h.EndTime != nil {
		end = FormatTime(*h.EndTime)
	}
	_, err := db.Exec(`INSERT INTO ACT_HI_INCIDENT(ID_, CREATE_TIME_, END_TIME_, INCIDENT_TYPE_, INCIDENT_MSG_, EXECUTION_ID_, ACTIVITY_ID_, PROC_INST_ID_, CAUSE_INCIDENT_ID_)
		VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(ID_) DO UPDATE SET END_TIME_=excluded.END_TIME_`,
		h.ID, FormatTime(h.CreateTime), end, h.IncidentType, h.IncidentMsg, h.ExecutionID, h.ActivityID, h.ProcInstID, h.CauseIncidentID)
	return err
}

func SelectHiIncidents(db DBTX, procInstID string) ([]*HiIncident, error) {
	q := `SELECT ID_, CREATE_TIME_, END_TIME_, INCIDENT_TYPE_, INCIDENT_MSG_, EXECUTION_ID_, ACTIVITY_ID_, PROC_INST_ID_, CAUSE_INCIDENT_ID_ FROM ACT_HI_INCIDENT`
	var args []any
	if procInstID != "" {
		q += ` WHERE PROC_INST_ID_=?`
		args = append(args, procInstID)
	}
	q += ` ORDER BY CREATE_TIME_`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*HiIncident
	for rows.Next() {
		h := &HiIncident{}
		var ct, end sql.NullString
		if err := rows.Scan(&h.ID, &ct, &end, &h.IncidentType, &h.IncidentMsg, &h.ExecutionID, &h.ActivityID, &h.ProcInstID, &h.CauseIncidentID); err != nil {
			return nil, err
		}
		h.CreateTime = MustParseTime(ct.String)
		if end.Valid && end.String != "" {
			t := MustParseTime(end.String)
			h.EndTime = &t
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// checkRev 乐观锁检查：UPDATE 影响 0 行说明 REV_ 被别人改过。
func checkRev(res sql.Result, entity, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return &OptimisticLockingException{Entity: entity, ID: id}
	}
	return nil
}

// OptimisticLockingException 对应 Camunda 同名异常，由 TransactionInterceptor 捕获重试。
type OptimisticLockingException struct {
	Entity string
	ID     string
}

func (e *OptimisticLockingException) Error() string {
	return fmt.Sprintf("OptimisticLockingException: %s %s was updated by another command concurrently", e.Entity, e.ID)
}
