// Package engine 组合根：对应 ProcessEngineConfigurationImpl + ProcessEngineImpl + 各 *Service。
package engine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"mini-camunda/internal/bpmn"
	"mini-camunda/internal/commandctx"
	"mini-camunda/internal/delegate"
	"mini-camunda/internal/interceptor"
	"mini-camunda/internal/jobexec"
	"mini-camunda/internal/kernel"
	"mini-camunda/internal/persistence"
)

// Configuration 对应 ProcessEngineConfiguration 的教学裁剪版。
type Configuration struct {
	DBPath           string // SQLite 文件路径；":memory:" 为内存库
	HistoryLevel     string // none | activity | audit | full
	Delegates        map[string]delegate.JavaDelegate
	EnableJobExecutor bool
	JobPollInterval  time.Duration
	JobMaxWorkers    int
}

func DefaultConfiguration() Configuration {
	return Configuration{
		DBPath:            ":memory:",
		HistoryLevel:      "full",
		Delegates:         map[string]delegate.JavaDelegate{},
		EnableJobExecutor: true,
		JobPollInterval:   500 * time.Millisecond,
		JobMaxWorkers:     4,
	}
}

// RegisterDelegate 注册 camunda:class 对应的实现（对应 ProcessEngineConfiguration.setJavaDelegates 风格）。
func (c *Configuration) RegisterDelegate(name string, d delegate.JavaDelegate) {
	c.Delegates[name] = d
}

// Engine 对应 ProcessEngine。
type Engine struct {
	cfg      Configuration
	ds       *persistence.DataSource
	executor *interceptor.Executor

	mu     sync.RWMutex
	defs   map[string]*bpmn.ProcessDefinition // procDefID → 解析后的定义
	latest map[string]string                  // key → 最新 procDefID

	extCh chan struct{} // 外部任务到达通知（长轮询）

	jobExec *jobexec.Executor

	closeOnce sync.Once
}

// NewProcessEngine 构建 + 启动（镜像 ProcessEngines.getDefaultProcessEngine()）。
func NewProcessEngine(cfg Configuration) (*Engine, error) {
	if cfg.HistoryLevel == "" {
		cfg.HistoryLevel = "full"
	}
	if cfg.Delegates == nil {
		cfg.Delegates = map[string]delegate.JavaDelegate{}
	}
	if cfg.JobPollInterval == 0 {
		cfg.JobPollInterval = 500 * time.Millisecond
	}
	if cfg.JobMaxWorkers <= 0 {
		cfg.JobMaxWorkers = 4
	}
	ds, err := persistence.OpenDataSource(cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("打开数据源失败: %w", err)
	}
	e := &Engine{
		cfg: cfg, ds: ds,
		defs: map[string]*bpmn.ProcessDefinition{}, latest: map[string]string{},
		extCh: make(chan struct{}, 1),
	}
	e.executor = interceptor.NewExecutor(ds, e, 3)
	if err := e.loadDeploymentCache(); err != nil {
		return nil, err
	}
	if cfg.EnableJobExecutor {
		je := jobexec.New("mini-job-executor", cfg.JobPollInterval, cfg.JobMaxWorkers,
			func() []string {
				jobs, err := e.AcquireJobs("mini-job-executor", cfg.JobMaxWorkers*2, 5*time.Minute)
				if err != nil {
					slog.Warn("job 获取失败", "err", err)
					return nil
				}
				ids := make([]string, 0, len(jobs))
				for _, j := range jobs {
					ids = append(ids, j.ID)
				}
				return ids
			},
			func(id string) error { return e.ExecuteJob(id) },
		)
		e.jobExec = je
		je.Start()
	}
	slog.Info("mini-camunda engine started", "db", cfg.DBPath, "historyLevel", cfg.HistoryLevel, "procDefs", len(e.defs))
	return e, nil
}

func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		if e.jobExec != nil {
			e.jobExec.Stop()
		}
	})
}

func (e *Engine) HistoryLevel() string { return e.cfg.HistoryLevel }

// ===== ID 生成（镜像 Camunda IdGenerator：随机串）=====

func (e *Engine) NextID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("id-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// procDefId 镜像 Camunda 格式 {key}:{version}:{random}。
func (e *Engine) procDefID(key string, version int) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s:%d:%s", key, version, hex.EncodeToString(b))
}

// ===== kernel.Deps 实现 =====

func (e *Engine) Delegate(name string) (delegate.JavaDelegate, bool) {
	d, ok := e.cfg.Delegates[name]
	return d, ok
}

func (e *Engine) ProcDef(id string) (*bpmn.ProcessDefinition, error) {
	e.mu.RLock()
	def, ok := e.defs[id]
	e.mu.RUnlock()
	if ok {
		return def, nil
	}
	// 未命中：从库里加载（可能是别的进程部署的）
	info, err := persistence.SelectProcDef(e.ds.DB(), id)
	if err != nil {
		return nil, fmt.Errorf("流程定义 %s 不存在", id)
	}
	bas, err := persistence.SelectByteArraysByDeployment(e.ds.DB(), info.DeploymentID)
	if err != nil {
		return nil, err
	}
	for _, ba := range bas {
		if ba.Name == info.ResourceName {
			def, err := bpmn.Parse(ba.Bytes, ba.Name)
			if err != nil {
				return nil, err
			}
			def.ID, def.Version, def.DeploymentID = info.ID, info.Version, info.DeploymentID
			e.mu.Lock()
			e.defs[info.ID] = def
			e.mu.Unlock()
			return def, nil
		}
	}
	return nil, fmt.Errorf("部署 %s 中找不到资源 %s", info.DeploymentID, info.ResourceName)
}

func (e *Engine) LatestProcDefByKey(key string) (*bpmn.ProcessDefinition, error) {
	e.mu.RLock()
	id, ok := e.latest[key]
	e.mu.RUnlock()
	if !ok {
		// 尝试从库加载最新版本
		infos, err := persistence.SelectProcDefs(e.ds.DB(), key)
		if err != nil || len(infos) == 0 {
			return nil, fmt.Errorf("找不到流程定义 key=%s", key)
		}
		latest := infos[len(infos)-1]
		e.mu.Lock()
		e.latest[key] = latest.ID
		e.mu.Unlock()
		return e.ProcDef(latest.ID)
	}
	return e.ProcDef(id)
}

func (e *Engine) AllLatestProcDefs() []*bpmn.ProcessDefinition {
	infos, err := persistence.SelectProcDefs(e.ds.DB(), "")
	if err != nil {
		return nil
	}
	var out []*bpmn.ProcessDefinition
	for _, info := range infos {
		if def, err := e.ProcDef(info.ID); err == nil {
			out = append(out, def)
		}
	}
	return out
}

// NotifyExtTask 外部任务创建时唤醒长轮询。
func (e *Engine) NotifyExtTask() {
	select {
	case e.extCh <- struct{}{}:
	default:
	}
}

// ===== 部署缓存 =====

// loadDeploymentCache 启动时把全部流程定义解析进内存（对应 DeploymentCache 预热）。
func (e *Engine) loadDeploymentCache() error {
	infos, err := persistence.SelectProcDefs(e.ds.DB(), "")
	if err != nil {
		return err
	}
	for _, info := range infos {
		if _, err := e.ProcDef(info.ID); err != nil {
			slog.Warn("启动加载流程定义失败", "id", info.ID, "err", err)
			continue
		}
		e.mu.Lock()
		if cur, ok := e.latest[info.Key]; !ok {
			e.latest[info.Key] = info.ID
		} else if curDef := e.defs[cur]; curDef != nil && curDef.Version < info.Version {
			e.latest[info.Key] = info.ID
		}
		e.mu.Unlock()
	}
	// 重建 timer-start 注册（引擎重启后定时启动事件仍然有效）
	defs := e.AllLatestProcDefs()
	return e.executor.Execute(&interceptor.CommandFunc{
		CmdName: "RebuildTimerStart",
		Fn: func(ctx *commandctx.CommandContext) error {
			k := kernel.New(ctx, e)
			for _, d := range defs {
				for _, start := range d.TimerStarts {
					if err := k.CreateTimerStartJob(d, start); err != nil {
						slog.Warn("重建定时启动失败", "def", d.Key, "err", err)
					}
				}
			}
			return nil
		},
	})
}

// cacheDef 部署成功后更新缓存。
func (e *Engine) cacheDef(def *bpmn.ProcessDefinition) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.defs[def.ID] = def
	if cur, ok := e.latest[def.Key]; !ok {
		e.latest[def.Key] = def.ID
	} else if curDef := e.defs[cur]; curDef == nil || curDef.Version < def.Version {
		e.latest[def.Key] = def.ID
	}
}

// RawDB 测试辅助：暴露底层 DB（不进公共 API 语义）。
func (e *Engine) RawDB() persistence.DBTX { return e.ds.DB() }
