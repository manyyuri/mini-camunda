// Package jobexec 镜像 org.camunda.bpm.engine.impl.jobexecutor：
// 轮询获取到期 Job → worker 池并发执行（排他 Job 在获取阶段保证同实例串行）。
package jobexec

import (
	"log/slog"
	"sync"
	"time"
)

// Executor Job 执行器。acquire 返回本批 Job id；execute 执行单个 Job。
type Executor struct {
	acquire func() []string
	execute func(id string) error
	tick    time.Duration
	workers int
	workerID string

	stopCh chan struct{}
	wg     sync.WaitGroup
	sem    chan struct{}
}

// New acquire 在锁语义下获取一批到期 Job（返回 id 列表）；execute 执行单个 Job。
func New(workerID string, tick time.Duration, workers int, acquire func() []string, execute func(string) error) *Executor {
	return &Executor{
		acquire: acquire, execute: execute, tick: tick, workers: workers, workerID: workerID,
		stopCh: make(chan struct{}), sem: make(chan struct{}, workers),
	}
}

func (x *Executor) Start() {
	x.wg.Add(1)
	go x.loop()
}

func (x *Executor) Stop() {
	close(x.stopCh)
	x.wg.Wait()
}

func (x *Executor) loop() {
	defer x.wg.Done()
	ticker := time.NewTicker(x.tick)
	defer ticker.Stop()
	for {
		select {
		case <-x.stopCh:
			return
		case <-ticker.C:
			x.runBatch()
		}
	}
}

func (x *Executor) runBatch() {
	ids := x.acquire()
	for _, id := range ids {
		id := id
		x.sem <- struct{}{}
		x.wg.Add(1)
		go func() {
			defer x.wg.Done()
			defer func() { <-x.sem }()
			if err := x.execute(id); err != nil {
				slog.Warn("job 执行失败（已转入重试/Incident 流程）", "job", id, "err", err)
			}
		}()
	}
}
