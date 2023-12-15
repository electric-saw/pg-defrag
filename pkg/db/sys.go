package db

import (
	"context"
	"fmt"
)

func (pg *PgConnection) TryAdvisoryLock(ctx context.Context, schema, table string) (bool, error) {

	var isLocked int

	err := pg.Conn.QueryRow(ctx, fmt.Sprintf(`
	SELECT pg_try_advisory_lock(
		'pg_catalog.pg_class'::regclass::integer,
		(quote_ident('%s')||'.'||quote_ident('%s'))::regclass::integer)::integer;`, schema, table)).Scan(&isLocked)

	if err != nil {
		return false, err
	}
	return isLocked == 0, nil
}

func (pg *PgConnection) GetVersionNum() int {
	return pg.pgVersion
}
