package db

import (
	"context"
	"fmt"
)

func (pg *PgConnection) HasTrigger(ctx context.Context, schema, table string) (bool, error) {
	ident := fmt.Sprintf("%s.%s", QuoteIdentifier(schema), QuoteIdentifier(table))
	qry := fmt.Sprintf(`
		SELECT count(1) FROM pg_catalog.pg_trigger
		WHERE
			tgrelid = '%s'::regclass AND
			tgenabled IN ('A', 'R') AND
			(tgtype & 16)::boolean
	`, ident)

	var n int
	err := pg.Conn.QueryRow(ctx, qry).Scan(&n)
	return n > 0, err
}
