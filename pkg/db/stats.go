package db

import (
	"context"
	"fmt"

	"github.com/georgysavva/scany/v2/pgxscan"
)

type PgSizeStats struct {
	Size           int64
	TotalSize      int64
	PageCount      int64
	TotalPageCount int64
}

type PgBloatStats struct {
	EffectivePageCount int64   `db:"effective_page_count"`
	FreePercent        float64 `db:"free_percent"`
	FreeSpace          int64   `db:"free_space"`
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
	select
    ceil((size - free_space - dead_tuple_len) * 100 / fillfactor / bs) as effective_page_count,
            greatest(round(
                (100 * (1 - (100 - free_percent - dead_tuple_percent) / fillfactor))::numeric, 2
            ),0) as free_percent,
            greatest(ceil(size - (size - free_space - dead_tuple_len) * 100 / fillfactor), 0) as free_space
    from (
    select
        current_setting('block_size')::integer as bs,
        pg_catalog.pg_relation_size(pg_catalog.pg_class.oid) as size,
        coalesce(
            (
                select (
                    regexp_matches(
                        reloptions::text, e'.*fillfactor=(\\\\d+).*'))[1]),
            '100')::real as fillfactor,
        pgst.*
    from pg_catalog.pg_class
    cross join
        %q.pgstattuple(
            (quote_ident($1) || '.' || quote_ident($2))) as pgst
    where pg_catalog.pg_class.oid = (quote_ident($1) || '.' || quote_ident($2))::regclass
    ) as sq limit 1;`, pgStatTupleSchema)

	var stats PgBloatStats
	err = pgxscan.Get(ctx, pg.Conn, &stats, qry, schema, table)
	return &stats, err
}

func (s *PgSizeStats) Copy() *PgSizeStats {
	return &PgSizeStats{
		Size:           s.Size,
		TotalSize:      s.TotalSize,
		PageCount:      s.PageCount,
		TotalPageCount: s.TotalPageCount,
	}
}
