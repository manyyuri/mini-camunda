// Package persistence 镜像 org.camunda.bpm.engine.impl.persistence.entity：
// 实体定义 + 手写 SQL Mapper + DataSource。
// 说明：不使用 ORM，函数命名对应 Camunda MyBatis statement 语义。
package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const timeFormat = time.RFC3339Nano // 全引擎统一 UTC 时间格式，字符串比较即时间比较

func FormatTime(t time.Time) string { return t.UTC().Format(timeFormat) }

func ParseTime(s string) (time.Time, error) { return time.Parse(timeFormat, s) }

// MustParseTime 解析失败返回零值（容错读取历史数据）。
func MustParseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := ParseTime(s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// DBTX 被 Mapper 使用的最小 DB 接口（*sql.DB 与 *Tx 都实现它）。
type DBTX interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// DataSource 对应 ProcessEngineConfiguration 的数据源。
// SQLite 单写者：写事务通过 writeMu 串行化 + BEGIN IMMEDIATE，杜绝 BUSY。
type DataSource struct {
	db      *sql.DB
	writeMu sync.Mutex
}

func OpenDataSource(path string) (*DataSource, error) {
	if path != ":memory:" && path != "" {
		if err := os.MkdirAll(filepathOf(path), 0o755); err != nil {
			return nil, err
		}
	}
	dsn := path
	if path == ":memory:" || path == "" {
		dsn = "file:minicam_mem?mode=memory&cache=shared"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if path == ":memory:" || path == "" {
		// 共享内存库：先建表再允许多连接复用
		if err := initMemDSN(dsn); err != nil {
			return nil, err
		}
	}
	db.SetMaxOpenConns(8)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	ds := &DataSource{db: db}
	if err := ds.migrate(); err != nil {
		return nil, err
	}
	// 镜像 Camunda 启动时写属性表
	_ = ds.SetProperty("engine.name", "mini-camunda")
	return ds, nil
}

func initMemDSN(dsn string) error {
	c, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer c.Close()
	data, err := schemaSQL()
	if err != nil {
		return err
	}
	_, err = c.Exec(string(data))
	return err
}

func filepathOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}

func (ds *DataSource) migrate() error {
	data, err := schemaSQL()
	if err != nil {
		return err
	}
	if _, err := ds.db.Exec(string(data)); err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}
	return nil
}

func (ds *DataSource) SetProperty(name, value string) error {
	_, err := ds.db.Exec(`INSERT INTO ACT_GE_PROPERTY(NAME_, VALUE_) VALUES(?,?)
		ON CONFLICT(NAME_) DO UPDATE SET VALUE_=excluded.VALUE_`, name, value)
	return err
}

func (ds *DataSource) GetProperty(name string) string {
	var v string
	_ = ds.db.QueryRow(`SELECT VALUE_ FROM ACT_GE_PROPERTY WHERE NAME_=?`, name).Scan(&v)
	return v
}

func (ds *DataSource) SetPropertyIfAbsent(name, value string) {
	_, _ = ds.db.Exec(`INSERT INTO ACT_GE_PROPERTY(NAME_, VALUE_) VALUES(?,?) ON CONFLICT(NAME_) DO NOTHING`, name, value)
}

func (ds *DataSource) DB() *sql.DB { return ds.db }

// Tx 手写事务：独占连接 + BEGIN IMMEDIATE；Commit/Rollback 后释放写锁。
type Tx struct {
	conn *sql.Conn
	ds   *DataSource
}

// BeginTx 开写事务。
func (ds *DataSource) BeginTx() (*Tx, error) {
	ds.writeMu.Lock()
	conn, err := ds.db.Conn(context.Background())
	if err != nil {
		ds.writeMu.Unlock()
		return nil, err
	}
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		conn.Close()
		ds.writeMu.Unlock()
		return nil, err
	}
	return &Tx{conn: conn, ds: ds}, nil
}

func (t *Tx) Exec(q string, args ...any) (sql.Result, error) {
	return t.conn.ExecContext(context.Background(), q, args...)
}
func (t *Tx) Query(q string, args ...any) (*sql.Rows, error) {
	return t.conn.QueryContext(context.Background(), q, args...)
}
func (t *Tx) QueryRow(q string, args ...any) *sql.Row {
	return t.conn.QueryRowContext(context.Background(), q, args...)
}

func (t *Tx) done(finalSQL string) error {
	var firstErr error
	if _, err := t.conn.ExecContext(context.Background(), finalSQL); err != nil {
		firstErr = err
	}
	if err := t.conn.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	t.ds.writeMu.Unlock()
	return firstErr
}

func (t *Tx) Commit() error   { return t.done("COMMIT") }
func (t *Tx) Rollback() error { return t.done("ROLLBACK") }
