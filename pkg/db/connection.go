package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
)

type PgConnection struct {
	Conn      *pgx.Conn
	log       logrus.FieldLogger
	connPid   uint32
	pgVersion int
}

func NewConnection(ctx context.Context, connStr string, log logrus.FieldLogger) (*PgConnection, error) {
	if conn, err := pgx.Connect(ctx, connStr); err != nil {
		return nil, err
	} else {
		return NewConnectionWithConn(ctx, conn, log)
	}
}

func NewConnectionWithConn(ctx context.Context, conn *pgx.Conn, log logrus.FieldLogger) (*PgConnection, error) {
	if _, err := conn.Exec(ctx, `set application_name to "pg-defrag";`); err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `set lc_messages TO 'C';`); err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `set client_min_messages to warning;`); err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `set statement_timeout to 0;`); err != nil {
		return nil, err
	}

	pid := conn.PgConn().PID()

	res := conn.QueryRow(ctx, `select pg_backend_pid();`)
	if err := res.Scan(&pid); err != nil {
		return nil, fmt.Errorf("failed to get backend pid: %w", err)
	}

	pgVersion := 0

	res = conn.QueryRow(ctx, `select current_setting('server_version_num')::int;`)
	if err := res.Scan(&pgVersion); err != nil {
		return nil, fmt.Errorf("failed to get pg version: %w", err)
	}

	return &PgConnection{
		Conn:      conn,
		log:       log,
		connPid:   pid,
		pgVersion: pgVersion}, nil

}

func (pg *PgConnection) Close(ctx context.Context) {
	if err := pg.Conn.Close(ctx); err != nil {
		pg.log.Errorf("Error closing connection: %v", err)
	}
}

func (pg *PgConnection) GetPID() uint32 {
	return pg.connPid
}

func (pg *PgConnection) GetPgStatTupleSchema(ctx context.Context) (string, error) {
	qry := `
SELECT nspname FROM pg_catalog.pg_proc
JOIN pg_catalog.pg_namespace AS n ON pronamespace = n.oid
WHERE proname = 'pgstattuple' LIMIT 1;
`
	var nspname string
	err := pg.Conn.QueryRow(ctx, qry).Scan(&nspname)
	return nspname, err
}

func (pg *PgConnection) SetSessionReplicaRole(ctx context.Context) error {
	qry := `set session_replication_role to replica;`
	_, err := pg.Conn.Exec(ctx, qry)
	return err
}
