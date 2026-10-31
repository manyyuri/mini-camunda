// Package rest 镜像 Camunda Engine REST API（/engine-rest 前缀）的教学子集。
package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"mini-camunda/internal/engine"
	"mini-camunda/internal/errs"
	"mini-camunda/internal/persistence"
)

// Handler 路由器。
type Handler struct {
	engine *engine.Engine
	mux    *http.ServeMux
}

func New(e *engine.Engine) *Handler {
	h := &Handler{engine: e, mux: http.NewServeMux()}
	h.routes()
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

func (h *Handler) routes() {
	m := h.mux
	// Deployment
	m.HandleFunc("POST /engine-rest/deployment/create", h.deploymentCreate)
	// Process definition
	m.HandleFunc("GET /engine-rest/process-definition", h.procDefList)
	m.HandleFunc("POST /engine-rest/process-definition/key/{key}/start", h.startByKey)
	// Process instance
	m.HandleFunc("GET /engine-rest/process-instance", h.instanceList)
	m.HandleFunc("DELETE /engine-rest/process-instance/{id}", h.instanceDelete)
	// Execution / trigger
	m.HandleFunc("POST /engine-rest/execution/{id}/trigger", h.trigger)
	// Message / Signal
	m.HandleFunc("POST /engine-rest/message", h.message)
	m.HandleFunc("POST /engine-rest/signal", h.signal)
	// Task
	m.HandleFunc("GET /engine-rest/task", h.taskList)
	m.HandleFunc("POST /engine-rest/task/{id}/claim", h.taskClaim)
	m.HandleFunc("POST /engine-rest/task/{id}/complete", h.taskComplete)
	// Variable
	m.HandleFunc("PUT /engine-rest/process-instance/{id}/variables/{name}", h.setVariable)
	// Job / Incident
	m.HandleFunc("GET /engine-rest/job", h.jobList)
	m.HandleFunc("POST /engine-rest/job/{id}/retries", h.jobRetries)
	m.HandleFunc("GET /engine-rest/incident", h.incidentList)
	// External task
	m.HandleFunc("POST /engine-rest/external-task/fetchAndLock", h.extFetchLock)
	m.HandleFunc("POST /engine-rest/external-task/{id}/complete", h.extComplete)
	m.HandleFunc("POST /engine-rest/external-task/{id}/failure", h.extFailure)
	// History
	m.HandleFunc("GET /engine-rest/history/process-instance", h.hiProcInst)
	m.HandleFunc("GET /engine-rest/history/activity-instance", h.hiActInst)
	m.HandleFunc("GET /engine-rest/history/task", h.hiTask)
	m.HandleFunc("GET /engine-rest/history/variable-instance", h.hiVar)
	m.HandleFunc("GET /engine-rest/history/job-log", h.hiJobLog)
	m.HandleFunc("GET /engine-rest/history/incident", h.hiIncident)
}

// ===== JSON 工具 =====

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

type apiError struct {
	Error string `json:"error"`
}

func writeErr(w http.ResponseWriter, err error) {
	switch err.(type) {
	case *errs.NotFound:
		writeJSON(w, 404, apiError{err.Error()})
	case *errs.BadArgument:
		writeJSON(w, 400, apiError{err.Error()})
	case *errs.Conflict:
		writeJSON(w, 409, apiError{err.Error()})
	default:
		writeJSON(w, 500, apiError{err.Error()})
	}
}

func body[T any](r *http.Request) (*T, error) {
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
	if err != nil {
		return nil, err
	}
	var out T
	if len(raw) == 0 {
		return &out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, errs.NewBadArgument("请求体解析失败: %v", err)
	}
	return &out, nil
}

// 变量映射：{name: {value, type}}
type varMap map[string]struct {
	Value any    `json:"value"`
	Type  string `json:"type,omitempty"`
}

func (m varMap) normalized() map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = normalizeJSON(v.Value)
	}
	return out
}

func normalizeJSON(v any) any {
	switch t := v.(type) {
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
		return t
	case map[string]any, []any, string, bool, nil:
		return t
	default:
		return t
	}
}

func varsToJSON(vars map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range vars {
		out[k] = map[string]any{"value": v}
	}
	return out
}

// ===== Deployment =====

type deployReq struct {
	Name      string `json:"deployment-name"`
	Resources []struct {
		Name     string `json:"name"`
		Bpmn20Xml string `json:"bpmn20Xml"`
	} `json:"resources"`
}

func (h *Handler) deploymentCreate(w http.ResponseWriter, r *http.Request) {
	req, err := body[deployReq](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if len(req.Resources) == 0 {
		writeJSON(w, 400, apiError{"缺少 resources"})
		return
	}
	var info *persistence.ProcDefInfo
	var perr error
	for _, res := range req.Resources {
		info, perr = h.engine.DeployResource(res.Name, []byte(res.Bpmn20Xml))
		if perr != nil {
			break
		}
	}
	if perr != nil {
		writeJSON(w, 400, apiError{perr.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{
		"id": info.DeploymentID, "name": req.Name,
		"processDefinition": map[string]any{
			"id": info.ID, "key": info.Key, "version": info.Version, "resource": info.ResourceName,
		},
	})
}

// ===== Process definition / instance =====

func (h *Handler) procDefList(w http.ResponseWriter, r *http.Request) {
	defs, err := h.engine.ProcDefs(r.URL.Query().Get("key"))
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, d := range defs {
		out = append(out, map[string]any{
			"id": d.ID, "key": d.Key, "version": d.Version,
			"deploymentId": d.DeploymentID, "resource": d.ResourceName,
		})
	}
	writeJSON(w, 200, out)
}

type startReq struct {
	BusinessKey string      `json:"businessKey"`
	Variables   varMap      `json:"variables"`
}

func (h *Handler) startByKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	req, err := body[startReq](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	instID, err := h.engine.StartProcessInstanceByKey(key, req.BusinessKey, req.Variables.normalized())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": instID, "definitionKey": key, "businessKey": req.BusinessKey, "ended": false})
}

func (h *Handler) instanceList(w http.ResponseWriter, r *http.Request) {
	execs, err := h.engine.ProcessInstances()
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, e := range execs {
		if e.ParentID == "" {
			out = append(out, map[string]any{
				"id": e.ProcInstID, "businessKey": e.BusinessKey, "definitionId": e.ProcDefID,
				"activityId": e.ActivityID, "ended": false,
			})
		}
	}
	writeJSON(w, 200, out)
}

func (h *Handler) instanceDelete(w http.ResponseWriter, r *http.Request) {
	if err := h.engine.DeleteProcessInstance(r.PathValue("id"), "deleted via REST"); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 204, nil)
}

func (h *Handler) trigger(w http.ResponseWriter, r *http.Request) {
	req, err := body[struct {
		Variables varMap `json:"variables"`
	}](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.engine.Trigger(r.PathValue("id"), req.Variables.normalized()); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 204, nil)
}

// ===== Message / Signal =====

type msgReq struct {
	MessageName     string `json:"messageName"`
	BusinessKey     string `json:"businessKey"`
	CorrelationKeys varMap `json:"correlationKeys"`
	ProcessVariables varMap `json:"processVariables"`
}

func (h *Handler) message(w http.ResponseWriter, r *http.Request) {
	req, err := body[msgReq](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if req.MessageName == "" {
		writeJSON(w, 400, apiError{"缺少 messageName"})
		return
	}
	n, err := h.engine.CorrelateMessage(req.MessageName, req.BusinessKey, req.CorrelationKeys.normalized(), req.ProcessVariables.normalized())
	if err != nil {
		// Camunda 语义：消息关联失败 → 400
		writeJSON(w, 400, apiError{fmt.Sprintf("Cannot correlate message '%s': %v", req.MessageName, err)})
		return
	}
	writeJSON(w, 200, map[string]any{"correlated": n})
}

type sigReq struct {
	Name      string `json:"name"`
	Variables varMap `json:"variables"`
}

func (h *Handler) signal(w http.ResponseWriter, r *http.Request) {
	req, err := body[sigReq](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if req.Name == "" {
		writeJSON(w, 400, apiError{"缺少 name"})
		return
	}
	if err := h.engine.SignalEventReceived(req.Name, req.Variables.normalized()); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 204, nil)
}

// ===== Task =====

func (h *Handler) taskList(w http.ResponseWriter, r *http.Request) {
	f := persistence.TaskFilter{Assignee: r.URL.Query().Get("assignee")}
	tasks, err := h.engine.Tasks(f)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, t := range tasks {
		out = append(out, map[string]any{
			"id": t.ID, "name": t.Name, "assignee": t.Assignee, "taskDefinitionKey": t.TaskDefKey,
			"processInstanceId": t.ProcInstID, "created": t.Created.Format(time.RFC3339),
		})
	}
	writeJSON(w, 200, out)
}

func (h *Handler) taskClaim(w http.ResponseWriter, r *http.Request) {
	req, err := body[struct {
		UserId string `json:"userId"`
	}](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.engine.Claim(r.PathValue("id"), req.UserId); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 204, nil)
}

func (h *Handler) taskComplete(w http.ResponseWriter, r *http.Request) {
	req, err := body[struct {
		Variables varMap `json:"variables"`
	}](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.engine.CompleteTask(r.PathValue("id"), req.Variables.normalized()); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 204, nil)
}

func (h *Handler) setVariable(w http.ResponseWriter, r *http.Request) {
	req, err := body[struct {
		Value any `json:"value"`
	}](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.engine.SetInstanceVariable(r.PathValue("id"), r.PathValue("name"), normalizeJSON(req.Value)); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 204, nil)
}

// ===== Job / Incident =====

func (h *Handler) jobList(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.engine.Jobs()
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, j := range jobs {
		out = append(out, map[string]any{
			"id": j.ID, "type": j.Type, "retries": j.Retries,
			"handlerType": j.HandlerType, "dueDate": j.DueDate, "exceptionMessage": j.ExceptionMsg,
			"processInstanceId": j.ProcessInstID,
		})
	}
	writeJSON(w, 200, out)
}

func (h *Handler) jobRetries(w http.ResponseWriter, r *http.Request) {
	req, err := body[struct {
		Retries int `json:"retries"`
	}](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.engine.SetJobRetries(r.PathValue("id"), req.Retries); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 204, nil)
}

func (h *Handler) incidentList(w http.ResponseWriter, r *http.Request) {
	incs, err := h.engine.Incidents()
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, i := range incs {
		out = append(out, map[string]any{
			"id": i.ID, "incidentType": i.Type, "incidentMessage": i.Msg,
			"activityId": i.ActivityID, "processInstanceId": i.ProcInstID, "jobId": i.JobID,
		})
	}
	writeJSON(w, 200, out)
}

// ===== External task =====

type extFetchReq struct {
	WorkerId  string `json:"workerId"`
	MaxTasks  int    `json:"maxTasks"`
	AsyncResp bool   `json:"asyncResponseTimeout"`
	Topics    []struct {
		TopicName    string `json:"topicName"`
		LockDuration int64  `json:"lockDuration"` // 毫秒
	} `json:"topics"`
}

func (h *Handler) extFetchLock(w http.ResponseWriter, r *http.Request) {
	req, err := body[extFetchReq](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if req.WorkerId == "" || len(req.Topics) == 0 {
		writeJSON(w, 400, apiError{"缺少 workerId 或 topics"})
		return
	}
	longPoll := time.Duration(0)
	if req.AsyncResp {
		longPoll = 20 * time.Second
	}
	var all []map[string]any
	for _, t := range req.Topics {
		lock := time.Duration(t.LockDuration) * time.Millisecond
		if lock <= 0 {
			lock = 5 * time.Minute
		}
		tasks, err := h.engine.FetchAndLock(t.TopicName, req.WorkerId, lock, req.MaxTasks, longPoll)
		if err != nil {
			writeErr(w, err)
			return
		}
		for _, et := range tasks {
			all = append(all, map[string]any{
				"id": et.ID, "topicName": et.Topic, "workerId": et.WorkerID,
				"retries": et.Retries, "processInstanceId": et.ProcInst, "activityId": et.ActID,
				"variables": varsToJSON(et.Variables),
			})
		}
	}
	if all == nil {
		all = []map[string]any{}
	}
	writeJSON(w, 200, all)
}

func (h *Handler) extComplete(w http.ResponseWriter, r *http.Request) {
	req, err := body[struct {
		WorkerId  string `json:"workerId"`
		Variables varMap `json:"variables"`
	}](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.engine.CompleteExtTask(r.PathValue("id"), req.WorkerId, req.Variables.normalized()); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 204, nil)
}

func (h *Handler) extFailure(w http.ResponseWriter, r *http.Request) {
	req, err := body[struct {
		WorkerId     string `json:"workerId"`
		ErrorMessage string `json:"errorMessage"`
		Retries      int    `json:"retries"`
	}](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.engine.FailExtTask(r.PathValue("id"), req.WorkerId, req.ErrorMessage, req.Retries); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 204, nil)
}

// ===== History =====

func (h *Handler) hiProcInst(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	list, err := h.engine.HistoricProcessInstances(q.Get("state"), q.Get("processDefinitionKey"))
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, p := range list {
		out = append(out, map[string]any{
			"id": p.ID, "businessKey": p.BusinessKey, "processDefinitionKey": p.ProcDefKey,
			"startTime": p.StartTime.Format(time.RFC3339), "endTime": optTime(p.EndTime),
			"endActivityId": p.EndActID, "deleteReason": p.DeleteReason, "state": p.State,
		})
	}
	writeJSON(w, 200, out)
}

func (h *Handler) hiActInst(w http.ResponseWriter, r *http.Request) {
	list, err := h.engine.HistoricActivities(r.URL.Query().Get("processInstanceId"))
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, a := range list {
		out = append(out, map[string]any{
			"id": a.ID, "parentActivityInstanceId": a.ParentActInstID, "activityId": a.ActID,
			"activityName": a.ActName, "activityType": a.ActType, "processInstanceId": a.ProcInstID,
			"startTime": a.StartTime.Format(time.RFC3339), "endTime": optTime(a.EndTime),
			"durationInMillis": a.Duration, "state": a.ActInstState,
		})
	}
	writeJSON(w, 200, out)
}

func (h *Handler) hiTask(w http.ResponseWriter, r *http.Request) {
	list, err := h.engine.HistoricTasks(r.URL.Query().Get("processInstanceId"))
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, t := range list {
		out = append(out, map[string]any{
			"id": t.ID, "name": t.Name, "assignee": t.Assignee, "taskDefinitionKey": t.TaskDefKey,
			"processInstanceId": t.ProcInstID, "startTime": t.StartTime.Format(time.RFC3339),
			"endTime": optTime(t.EndTime), "durationInMillis": t.Duration, "deleteReason": t.DeleteReason,
		})
	}
	writeJSON(w, 200, out)
}

func (h *Handler) hiVar(w http.ResponseWriter, r *http.Request) {
	list, err := h.engine.HistoricVariables(r.URL.Query().Get("processInstanceId"))
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, v := range list {
		out = append(out, map[string]any{
			"id": v.ID, "name": v.Name, "type": v.VarType, "processInstanceId": v.ProcInstID,
			"state": v.State, "createTime": v.CreateTime.Format(time.RFC3339),
		})
	}
	writeJSON(w, 200, out)
}

func (h *Handler) hiJobLog(w http.ResponseWriter, r *http.Request) {
	list, err := h.engine.HistoricJobLogs()
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, j := range list {
		out = append(out, map[string]any{
			"id": j.ID, "jobId": j.JobID, "timestamp": j.Timestamp.Format(time.RFC3339),
			"state": j.State, "retries": j.JobRetries, "exceptionMessage": j.JobException,
		})
	}
	writeJSON(w, 200, out)
}

func (h *Handler) hiIncident(w http.ResponseWriter, r *http.Request) {
	list, err := h.engine.HistoricIncidents(r.URL.Query().Get("processInstanceId"))
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, i := range list {
		out = append(out, map[string]any{
			"id": i.ID, "incidentType": i.IncidentType, "incidentMessage": i.IncidentMsg,
			"processInstanceId": i.ProcInstID, "activityId": i.ActivityID,
			"createTime": i.CreateTime.Format(time.RFC3339), "endTime": optTime(i.EndTime),
		})
	}
	writeJSON(w, 200, out)
}

func optTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.Format(time.RFC3339)
}

var _ = strconv.Itoa
