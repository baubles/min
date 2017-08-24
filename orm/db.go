package orm

import (
	"database/sql"
	"time"
)

var (
	ErrTxDone = sql.ErrTxDone
	ErrNoRows = sql.ErrNoRows
)

var Debug = false

type Tx interface {
	SqlQuerier

	NewSql() Sql
	Commit() error
	Rollback() error
}

type DB interface {
	SqlQuerier

	NewSql() Sql
	Begin() (Tx, error)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
}

type SqlQuerier interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Prepare(query string) (*sql.Stmt, error)
}

type odb struct {
	*sql.DB
}

func NewDB(driver, dsn string) (DB, error) {
	db, err := sql.Open(driver, dsn)

	if err != nil {
		return nil, err
	}

	return &odb{db}, nil
}

func (o *odb) NewSql() Sql {
	return &rawSql{
		conn: o,
	}
}

func (o *odb) Begin() (Tx, error) {
	tx, err := o.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &otx{tx}, err
}

type otx struct {
	*sql.Tx
}

func (o *otx) NewSql() Sql {
	return &rawSql{
		conn: o,
	}
}
