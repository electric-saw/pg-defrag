package db

import (
	"context"
	"fmt"

	"github.com/electric-saw/pg-defrag/pkg/params"
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

	qry := fmt.Sprintf(qryTableBloatPgstattuple, pgStatTupleSchema)

	var stats PgBloatStats
	err = pgxscan.Get(ctx, pg.Conn, &stats, qry, schema, table)
	if err != nil {
		return nil, fmt.Errorf("Failed to get bloat stats: %v", err)
	}

	if params.BLOAT_METRIC_SOURCE == params.BLOAT_METRIC_STATISTICAL {
		qry = qryTableBloatStatistical
		err = pgxscan.Get(ctx, pg.Conn, &stats, qry, schema, table)
		if err != nil {
			return nil, fmt.Errorf("Failed to get bloat stats: %v", err)
		}
	}
	return &stats, nil
}

func (s *PgSizeStats) Copy() *PgSizeStats {
	return &PgSizeStats{
		Size:           s.Size,
		TotalSize:      s.TotalSize,
		PageCount:      s.PageCount,
		TotalPageCount: s.TotalPageCount,
	}
}
