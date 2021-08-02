package db

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

type PgConnection struct {
	Conn *pgx.Conn
	log  logrus.FieldLogger
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

	return &PgConnection{Conn: conn, log: log}, nil

}

func (pg *PgConnection) Close(ctx context.Context) {
	if err := pg.Conn.Close(ctx); err != nil {
		pg.log.Errorf("Error closing connection: %v", err)
	}
}

func (pg *PgConnection) GetPID() uint32 {
	return pg.Conn.PgConn().PID()
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
