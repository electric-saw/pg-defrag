package db

import (
	"context"
	"fmt"

	"github.com/georgysavva/scany/pgxscan"
)

type PgSizeStats struct {
	Size           int64
	TotalSize      int64
	PageCount      int64
	TotalPageCount int64
}

type PgBloatStats struct {
	EffectivePageCount int64
	FreePercent        float64
	FreeSpace          int64
}

func (pg *PgConnection) GetPgSizeStats(ctx context.Context, schema, table string) (*PgSizeStats, error) {
	qry := `
	SELECT
		size,
		total_size,
		ceil(size / bs) AS page_count,
		ceil(total_size / bs) AS total_page_count
	FROM (
		SELECT
			current_setting('block_size')::integer AS bs,
			pg_catalog.pg_relation_size(quote_ident($1)||'.'||quote_ident($2)) AS size,
			pg_catalog.pg_total_relation_size(quote_ident($1)||'.'||quote_ident($2)) AS total_size
	) AS sq
	`
	var stats PgSizeStats

	err := pgxscan.Get(ctx, pg.Conn, &stats, qry, schema, table)
	return &stats, err
}

func (pg *PgConnection) GetBloatStats(ctx context.Context, schema, table string) (*PgBloatStats, error) {
	pgStatTupleSchema, err := pg.GetPgStatTupleSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to get pg_stat_tuple schema: %v", err)
	}

	qry := fmt.Sprintf(`
	"SELECT
    ceil((size - free_space - dead_tuple_len) * 100 / fillfactor / bs) AS effective_page_count,
            greatest(round(
                (100 * (1 - (100 - free_percent - dead_tuple_percent) / fillfactor))::numeric, 2
            ),0) AS free_percent,
            greatest(ceil(size - (size - free_space - dead_tuple_len) * 100 / fillfactor), 0) AS free_space
    FROM (
    SELECT
        current_setting('block_size')::integer AS bs,
        pg_catalog.pg_relation_size(pg_catalog.pg_class.oid) AS size,
        coalesce(
            (
                SELECT (
                    regexp_matches(
                        reloptions::text, E'.*fillfactor=(\\\\d+).*'))[1]),
            '100')::real AS fillfactor,
        pgst.*
    FROM pg_catalog.pg_class
    CROSS JOIN
        %s.pgstattuple(
            (quote_ident($1) || '.' || quote_ident($2))) AS pgst
    WHERE pg_catalog.pg_class.oid = (quote_ident($1) || '.' || quote_ident($2))::regclass
    ) AS sq`, pgStatTupleSchema)

	var stats PgBloatStats
	err = pgxscan.Get(ctx, pg.Conn, &stats, qry, schema, table)
	return &stats, err
}
