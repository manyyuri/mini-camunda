// mini-camunda CLI：镜像 camunda boot（serve）+ 部署/演示工具。
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"mini-camunda/internal/engine"
	"mini-camunda/internal/example"
	"mini-camunda/internal/rest"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		cmdServe(os.Args[2:])
	case "deploy":
		cmdDeploy(os.Args[2:])
	case "demo":
		cmdDemo(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Print(`mini-camunda —— Camunda 7.19 教学版流程引擎

用法:
  minicamunda serve  [-addr :8080] [-db minicam.db]      启动引擎 + REST API
  minicamunda deploy [-db minicam.db] <file.bpmn>...     部署 BPMN 文件
  minicamunda demo   [-db minicam.db]                    跑 loan-review 演示剧本
`)
}

func newConfig(dbPath string) engine.Configuration {
	cfg := engine.DefaultConfiguration()
	cfg.DBPath = dbPath
	// 教学 delegates：noop / fail / setVariable / risk-check / throw-review-error
	cfg.RegisterDelegate("noop", example.Noop())
	cfg.RegisterDelegate("fail", example.Fail())
	cfg.RegisterDelegate("setVariable", example.SetVariable())
	cfg.RegisterDelegate("risk-check", example.RiskCheck())
	cfg.RegisterDelegate("throw-review-error", example.ThrowBpmnError("REVIEW_FAILED", "复核失败"))
	return cfg
}

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "HTTP 监听地址")
	db := fs.String("db", "minicam.db", "SQLite 数据库路径（:memory: 为内存库）")
	_ = fs.Parse(args)

	e, err := engine.NewProcessEngine(newConfig(*db))
	must(err)
	defer e.Close()

	h := rest.New(e)
	fmt.Printf("mini-camunda listening on http://localhost%s/engine-rest/...\n", *addr)
	must(http.ListenAndServe(*addr, h))
}

func cmdDeploy(args []string) {
	fs := flag.NewFlagSet("deploy", flag.ExitOnError)
	db := fs.String("db", "minicam.db", "SQLite 数据库路径")
	_ = fs.Parse(args)
	files := fs.Args()
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "缺少 BPMN 文件参数")
		os.Exit(2)
	}
	e, err := engine.NewProcessEngine(func() engine.Configuration {
		c := newConfig(*db)
		c.EnableJobExecutor = false // 一次性命令不跑后台执行器
		return c
	}())
	must(err)
	defer e.Close()
	for _, f := range files {
		data, err := os.ReadFile(f)
		must(err)
		info, err := e.DeployResource(f, data)
		must(err)
		fmt.Printf("已部署: key=%s version=%d id=%s (%s)\n", info.Key, info.Version, info.ID, info.ResourceName)
	}
}

func cmdDemo(args []string) {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	db := fs.String("db", ":memory:", "SQLite 数据库路径")
	_ = fs.Parse(args)
	must(runDemo(*db))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}

var _ = time.Now
