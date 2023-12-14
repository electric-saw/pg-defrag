package db

import (
	"context"
	"fmt"
	"strings"
)

func (pg *PgConnection) Vacuum(ctx context.Context, schema, table string, analyze bool) error {
	opts := []string{}

	if analyze {
		opts = append(opts, "analyze")
	}

	if pg.GetVersionNum() >= 120000 {
		opts = append(opts, "index_cleanup on")
	}

	optsStr := " "

	if len(opts) > 0 {
		optsStr = fmt.Sprintf("(%s)", strings.Join(opts, ", "))
	}

	qry := fmt.Sprintf("vacuum %s %s.%s", optsStr, QuoteIdentifier(schema), QuoteIdentifier(table))
	_, err := pg.Conn.Exec(ctx, qry)
	return err
}

func (pg *PgConnection) Analyze(ctx context.Context, schema, table string) error {
	qry := fmt.Sprintf("analyze %s.%s", QuoteIdentifier(schema), QuoteIdentifier(table))
	_, err := pg.Conn.Exec(ctx, qry)
	return err
}
