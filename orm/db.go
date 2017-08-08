package orm

import (
	"database/sql"
	"errors"
	"time"
)

var (
	ErrTxHasBegin = errors.New("[database.conn] transaction has begin")
	ErrTxDone     = errors.New("[database.conn] transaction not begin")
	ErrNoRows     = sql.ErrNoRows
)

var Debug = false

type DB interface {
	Begin() error
	Commit() error
	Rollback() error
	NewSql() Sql
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Prepare(query string) (*sql.Stmt, error)
	Close() error
	Closed() bool
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
}

type sqlQuerier interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Prepare(query string) (*sql.Stmt, error)
}

type odb struct {
	db      *sql.DB
	tx      *sql.Tx
	isClose bool
}

func NewDB(driver, dsn string) (DB, error) {
	db, err := sql.Open(driver, dsn)

	if err != nil {
		return nil, err
	}

	return &odb{db: db, isClose: false}, nil
}

func (o *odb) sqlQuerier() sqlQuerier {
	if o.tx == nil {
		return o.db
	}
	return o.tx
}

func (o *odb) QueryRow(sql string, args ...interface{}) *sql.Row {
	return o.sqlQuerier().QueryRow(sql, args...)
}

func (o *odb) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	return o.sqlQuerier().Query(sql, args...)
}

func (o *odb) Exec(sql string, args ...interface{}) (sql.Result, error) {
	return o.sqlQuerier().Exec(sql, args...)
}

func (o *odb) Prepare(query string) (*sql.Stmt, error) {
	return o.sqlQuerier().Prepare(query)
}

func (o *odb) NewSql() Sql {
	return &rawSql{
		conn: o,
	}
}

func (o *odb) Begin() error {
	if o.tx != nil {
		return ErrTxHasBegin
	}

	tx, err := o.db.Begin()
	if err == nil {
		o.tx = tx
	}
	return err
}

func (o *odb) Commit() error {
	if o.tx == nil {
		return ErrTxDone
	}
	err := o.tx.Commit()
	if err == nil {
		o.tx = nil
	} else if err == sql.ErrTxDone {
		return ErrTxDone
	}
	return err
}

func (o *odb) Rollback() error {
	if o.tx == nil {
		return ErrTxDone
	}
	err := o.tx.Rollback()
	if err == nil {
		o.tx = nil
	} else if err == sql.ErrTxDone {
		return ErrTxDone
	}
	return err
}

func (o *odb) Close() error {
	if o.isClose {
		return nil
	}
	err := o.db.Close()
	o.isClose = true
	return err
}

func (o *odb) Closed() bool {
	return o.isClose
}

func (o *odb) SetConnMaxLifetime(d time.Duration) {
	o.db.SetConnMaxLifetime(d)
}
func (o *odb) SetMaxIdleConns(n int) {
	o.db.SetMaxIdleConns(n)
}
func (o *odb) SetMaxOpenConns(n int) {
	o.db.SetMaxOpenConns(n)
}
func (o *odb) Stats() sql.DBStats {
	return o.db.Stats()
}
