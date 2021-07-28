package db

import (
	"context"
	"fmt"
)

func (pg *PgConnection) Vacuum(ctx context.Context, schema, table string) error {
	qry := fmt.Sprintf("vacuum analyze %s.%s", schema, table)
	_, err := pg.Conn.Exec(ctx, qry)
	return err
}

func (pg *PgConnection) Analyze(ctx context.Context, schema, table string) error {
	qry := fmt.Sprintf("analyze %s.%s", schema, table)
	_, err := pg.Conn.Exec(ctx, qry)
	return err
}
