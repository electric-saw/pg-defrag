package db

import (
	"context"
	"fmt"
)

func (pg *PgConnection) TryAdvisoryLock(ctx context.Context, schema, table string) (bool, error) {

	var lock int

	err := pg.Conn.QueryRow(ctx, fmt.Sprintf(`
	SELECT pg_try_advisory_lock(
		'pg_catalog.pg_class'::regclass::integer,
		(quote_ident('%s')||'.'||quote_ident('%s'))::regclass::integer)::integer;`, schema, table)).Scan(&lock)

	if err != nil {
		return false, err
	}
	return lock == 1, nil
}
