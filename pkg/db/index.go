package db

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"github.com/electric-saw/pg-defrag/pkg/params"
	"github.com/electric-saw/pg-defrag/pkg/utils"
	"github.com/georgysavva/scany/pgxscan"
)

type IndexDefinition struct {
	IndexName    string  `db:"indexname"`
	Tablespace   string  `db:"tablespace"`
	IndexDef     string  `db:"indexdef"`
	IndexMethod  string  `db:"indmethod"`
	ConName      *string `db:"conname"`
	ConTypeDef   *string `db:"contypedef"`
	Allowed      bool    `db:"allowed"`
	IsFunctional bool    `db:"is_functional"`
	IsDeferrable *bool   `db:"is_deferrable"`
	IsDeferred   *bool   `db:"is_deferred"`
	IdxSize      int64   `db:"idxsize"`
}

type IndexStats struct {
	Size      int64 `db:"size"`
	PageCount int64 `db:"page_count"`
}

type IndexBloatStats struct {
	FreePerctent float64 `db:"free_percent"`
	FreeSpace    int64   `db:"free_space"`
}

func (pg *PgConnection) ReindexTable(ctx context.Context, schema, table string, force bool) (bool, int, error) {
	var isReindexed bool = true
	var lockAttemp = 0

	if indexDataList, err := pg.GetIndexList(ctx, schema, table); err != nil {
		return false, 0, fmt.Errorf("failed on get index data: %w", err)
	} else {
		for _, index := range indexDataList {
			select {
			case <-ctx.Done():
				return isReindexed, lockAttemp, nil
			default:
				initialIndexStats, err := pg.GetIndexSizeStatistics(context.Background(), schema, index.IndexName)
				if err != nil {
					return false, 0, fmt.Errorf("failed on get index size statistics: %w", err)
				}

				if initialIndexStats.PageCount <= 1 {
					pg.log.Infof("skipping reindex: %s.%s, empty or 1 page only", schema, index.IndexName)
					continue
				}

				indexBloatStats, err := pg.getIndexBloatStats(context.Background(), schema, index.IndexName)
				if err != nil {
					return false, 0, fmt.Errorf("failed on get index bloat stats: %w", err)
				}

				if !force {
					if index.IndexMethod != "btree" {
						pg.log.Warnf("skipping reindex: %s.%s, not btree, reindexing is up to you.", schema, index.IndexName)
						pg.log.Warnf("reindex queries: %s.%s, initial size %d pages (%s)", schema, index.IndexName, initialIndexStats.PageCount, utils.Humanize(initialIndexStats.Size))
						if index.Allowed {
							pg.log.Warnf("%s; --%s", pg.getReindexQuery(&index), pg.Conn.Config().Database)
							pg.log.Warnf("%s; --%s", pg.getAlterIndexQuery(schema, table, &index), pg.Conn.Config().Database)
						} else {
							pg.log.Warnf("%s; --%s", pg.getStraightReindexQuery(schema, &index), pg.Conn.Config().Database)
						}
						continue
					}

					if initialIndexStats.PageCount < params.MINIMAL_COMPACT_PAGES {
						pg.log.Infof("skipping reindex: %s.%s, too few pages (%d and the minimum is %d)", schema, index.IndexName, initialIndexStats.PageCount, params.MINIMAL_COMPACT_PAGES)
						continue
					}

					if indexBloatStats.FreePerctent < params.MINIMAL_COMPACT_PERCENT {
						pg.log.Infof("skipping reindex: %s.%s, %f%s free space is below required %f%s", schema, index.IndexName, indexBloatStats.FreePerctent, "%", params.MINIMAL_COMPACT_PERCENT, "%")
						continue
					}

				}

				pg.log.Warnf("reindex queries: %s.%s, initial size %d pages (%s), will be reduced by %f%% (%s)",
					schema,
					index.IndexName,
					initialIndexStats.PageCount,
					utils.Humanize(initialIndexStats.Size),
					indexBloatStats.FreePerctent,
					utils.Humanize(indexBloatStats.FreeSpace))

				if !index.Allowed {
					pg.log.Infof("skip reindex: %s.%s, can not reindex without heavy locks because of its dependencies, reindexing is up to you.", schema, index.IndexName)
					pg.log.Warnf("%s; --%s", pg.getReindexQuery(&index), pg.Conn.Config().Database)
					pg.log.Warnf("%s; --%s", pg.getAlterIndexQuery(schema, table, &index), pg.Conn.Config().Database)
					continue
				}

				startedAt := time.Now()

				err = pg.Reindex(context.Background(), &index)
				if err != nil {
					pg.log.Warnf("skipped reindex: %s.%s, %s", schema, index.IndexName, err)

					// Cleanup index

					if exists, _ := pg.IndexExists(context.Background(), schema, pg.getTempIndexName()); exists {
						pg.log.Infof("removing index: %s.%s", schema, pg.getTempIndexName())
						_ = pg.dropTempIndex(context.Background(), schema)
					}
					continue
				}

				if index.IsFunctional {
					analyzeStartedAt := time.Now()
					if err := pg.Analyze(context.Background(), schema, table); err != nil {
						pg.log.Warnf("failed on analyze: %s.%s, %s", schema, index.IndexName, err.Error())
					} else {
						pg.log.Infof("auto analyze functional index, duration %s", time.Since(analyzeStartedAt))
					}
				}

				for lockAttemp < 3 {
					if err := pg.AlterIndex(context.Background(), schema, table, &index); err != nil {
						if strings.Contains(err.Error(), "statement timeout") {
							lockAttemp++
							pg.log.Warnf("failed on alter index: %s.%s, %s", schema, index.IndexName, err)
							time.Sleep(10 * time.Second)
							pg.log.Infof("retry alter index: %s.%s, %d/3", schema, index.IndexName, lockAttemp)
							continue
						} else {
							pg.log.Errorf("failed on alter index: %s.%s, %s", schema, index.IndexName, err)
							return false, lockAttemp, err
						}
					} else {
						break
					}
				}

				if lockAttemp < 3 {
					newStats, err := pg.GetIndexSizeStatistics(context.Background(), schema, index.IndexName)
					if err != nil {
						return isReindexed, lockAttemp, fmt.Errorf("failed on get index size statistics: %w", err)
					}

					freePct := 100 * (1 - float64(newStats.Size)/float64(initialIndexStats.Size))
					freeSpace := (initialIndexStats.Size - newStats.Size)
					pg.log.Warnf("reindex: %s.%s, initial size %d (%s), has been reduced by %f%% (%s), duration %s, attemps %d",
						schema,
						index.IndexName,
						initialIndexStats.PageCount,
						utils.Humanize(initialIndexStats.Size),
						freePct,
						utils.Humanize(freeSpace),
						time.Since(startedAt),
						lockAttemp)
				} else {
					if err := pg.dropTempIndex(context.Background(), schema); err != nil {
						pg.log.Errorf("unable to drop temporary index: %s.%s, %s", schema, pg.getTempIndexName(), err)
						return false, lockAttemp, err
					}

					pg.log.Warnf("reindex: %s.%s, lock has not been acquired, initial size %d pages (%s)",
						schema,
						index.IndexName,
						initialIndexStats.PageCount,
						utils.Humanize(initialIndexStats.Size))

					isReindexed = false
				}

				pg.log.Debugf("reindex queries: %s.%s, initial size %d pages (%s), will be reduced by %f%% (%s), duration %s",
					schema,
					index.IndexName,
					initialIndexStats.PageCount,
					utils.Humanize(initialIndexStats.Size),
					indexBloatStats.FreePerctent,
					utils.Humanize(indexBloatStats.FreeSpace),
					time.Since(startedAt))

				pg.log.Debugf("%s; --%s", pg.getReindexQuery(&index), pg.Conn.Config().Database)
				pg.log.Debugf("%s; --%s", pg.getAlterIndexQuery(schema, table, &index), pg.Conn.Config().Database)

			}
		}
	}
	return isReindexed, lockAttemp, nil
}

func (pg *PgConnection) getTempIndexName() string {
	return fmt.Sprintf("pg_defrag_index_%d", pg.GetPID())
}

func (pg *PgConnection) Reindex(ctx context.Context, index *IndexDefinition) error {
	_, err := pg.Conn.Exec(ctx, pg.getReindexQuery(index))
	return err
}

func (pg *PgConnection) AlterIndex(ctx context.Context, schema, table string, index *IndexDefinition) error {
	for _, sql := range strings.Split(pg.getAlterIndexQuery(schema, table, index), ";") {
		_, err := pg.Conn.Exec(ctx, fmt.Sprintf("%s;", strings.TrimSpace(sql)))
		if err != nil {
			return err
		}
	}
	return nil
}

func (pg *PgConnection) GetIndexList(ctx context.Context, schema, table string) ([]IndexDefinition, error) {
	qry := `
	SELECT
    indexname, tablespace, indexdef,
    regexp_replace(indexdef, E'.* USING (\\w+) .*', E'\\1') AS indmethod,
    conname,
    CASE
        WHEN contype = 'p' THEN 'PRIMARY KEY'
        WHEN contype = 'u' THEN 'UNIQUE'
        ELSE NULL END AS contypedef,
    (
        SELECT
            bool_and(
                deptype IN ('n', 'a', 'i') AND
                NOT (refobjid = indexoid AND deptype = 'n') AND
                NOT (
                    objid = indexoid AND deptype = 'i' AND
                    (version < array[9,1] OR contype NOT IN ('p', 'u'))))
        FROM pg_catalog.pg_depend
        LEFT JOIN pg_catalog.pg_constraint ON
            pg_catalog.pg_constraint.oid = refobjid
        WHERE
            (objid = indexoid AND classid = pgclassid) OR
            (refobjid = indexoid AND refclassid = pgclassid)
    )::integer = 1 AS allowed,
    (
        SELECT string_to_array(indkey::text, ' ')::int2[] operator(pg_catalog.@>) array[0::int2]
        FROM pg_catalog.pg_index
        WHERE indexrelid = indexoid
    )::integer = 1 as is_functional,
    condeferrable as is_deferrable,
    condeferred as is_deferred,
    pg_catalog.pg_relation_size(indexoid) as idxsize
FROM (
    SELECT
        indexname, COALESCE(tablespace, (SELECT spcname AS tablespace FROM pg_catalog.pg_tablespace WHERE oid = (SELECT dattablespace
            FROM pg_catalog.pg_database
            WHERE
                datname = current_database() AND
                spcname != current_setting('default_tablespace')))) AS tablespace, indexdef,
        (
            quote_ident(schemaname) || '.' ||
            quote_ident(indexname))::regclass AS indexoid,
        'pg_catalog.pg_class'::regclass AS pgclassid,
        string_to_array(
            regexp_replace(
                version(), E'.*PostgreSQL (\\d+\\.\\d+).*', E'\\1'),
            '.')::integer[] AS version
    FROM pg_catalog.pg_indexes
      WHERE
       schemaname = quote_ident($1) AND
       tablename = quote_ident($2)
) AS sq
LEFT JOIN pg_catalog.pg_constraint ON
    conindid = indexoid AND contype IN ('p', 'u')
ORDER BY idxsize;
	`

	result := []IndexDefinition{}

	err := pgxscan.Select(ctx, pg.Conn, &result, qry, schema, table)
	return result, err
}

func (pg *PgConnection) GetIndexSizeStatistics(ctx context.Context, schema, index string) (IndexStats, error) {
	qry := `
    SELECT size, ceil(size / bs) AS page_count
    FROM (
        SELECT
            pg_catalog.pg_relation_size((quote_ident($1) || '.' || quote_ident($2))::regclass) AS size,
            current_setting('block_size')::real AS bs
    ) AS sq
    `
	var result IndexStats
	err := pgxscan.Get(ctx, pg.Conn, &result, qry, schema, index)
	return result, err

}

func (pg *PgConnection) getReindexQuery(index *IndexDefinition) string {
	var qry string = index.IndexDef
	var re = regexp.MustCompile(`(?m)INDEX (\S+)`)
	var re2 = regexp.MustCompile(`(?m)( WHERE .*|$)`)

	qry = re.ReplaceAllString(qry, fmt.Sprintf("INDEX CONCURRENTLY %s", pg.getTempIndexName()))
	if index.Tablespace != "" {
		qry = re2.ReplaceAllString(qry, fmt.Sprintf(" TABLESPACE %s $1", index.Tablespace))
	}
	return qry
}

func (pg *PgConnection) IndexExists(ctx context.Context, schema, index string) (bool, error) {
	qry := `
	SELECT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_indexes
		WHERE schemaname = quote_ident($1) AND indexname = quote_ident($2)
	)
	`
	var result bool
	err := pgxscan.Get(ctx, pg.Conn, &result, qry, schema, index)
	return result, err
}

func (pg *PgConnection) dropTempIndex(ctx context.Context, schema string) error {
	_, err := pg.Conn.Exec(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY %s;", pg.getTempIndexName()))
	return err
}

func (pg *PgConnection) getAlterIndexQuery(schema, table string, index *IndexDefinition) string {
	if index.ConName != nil {
		constraintName := QuoteIdentifier(*index.ConName)
		constraintOptions := fmt.Sprintf("%s using index %s", *index.ConTypeDef, pg.getTempIndexName())

		if index.IsDeferrable != nil && *index.IsDeferrable {
			constraintOptions = fmt.Sprintf(" %s deferrable", constraintOptions)
		}

		if index.IsDeferred != nil && *index.IsDeferred {
			constraintOptions = fmt.Sprintf(" %s initially deferred", constraintOptions)
		}

		return fmt.Sprintf(`
        begin; set local statement_timeout = 0;
        alter table %s.%s drop constraint %s;
        alter table %s.%s add constraint %s %s;
        end;`, schema, table, constraintName, schema, table, constraintName, constraintOptions)
	} else {
		randIndex := fmt.Sprintf("tmp_%d", rand.Intn(1000000000))
		return fmt.Sprintf(`
        begin; set local statement_timeout = 0;
        alter index %s.%s rename to %s;
        alter index %s.%s rename to %s;
        end;
        drop index concurrently %s.%s;
        `, schema, index.IndexName, randIndex, schema, pg.getTempIndexName(), index.IndexName, schema, randIndex)

	}

}

func (pg *PgConnection) getStraightReindexQuery(schema string, index *IndexDefinition) string {
	return fmt.Sprintf("reindex index ('%s.%s')",
		QuoteIdentifier(schema),
		QuoteIdentifier(index.IndexName))
}

func (pg *PgConnection) getIndexBloatStats(ctx context.Context, schema, index string) (*IndexBloatStats, error) {
	if pgstatstupleSchema, err := pg.GetPgStatTupleSchema(ctx); err != nil {
		return nil, fmt.Errorf("error getting pgstattuple schema: %w", err)
	} else {
		qry := fmt.Sprintf(`
		SELECT
			CASE
				WHEN avg_leaf_density = 'NaN' THEN 0
				ELSE
					round(
						(100 * (1 - avg_leaf_density / fillfactor))::numeric, 2
					)
				END AS free_percent,
			CASE
				WHEN avg_leaf_density = 'NaN' THEN 0
				ELSE
					ceil(
						index_size * (1 - avg_leaf_density / fillfactor)
					)
				END AS free_space
		FROM (
			SELECT
				coalesce(
					(
						SELECT (
							regexp_matches(
								reloptions::text, E'.*fillfactor=(\\\\d+).*'))[1]),
					'90')::real AS fillfactor,
				pgsi.*
			FROM pg_catalog.pg_class
			CROSS JOIN %s.pgstatindex(
				quote_ident($1) || '.' || quote_ident($2)) AS pgsi
			WHERE pg_catalog.pg_class.oid = (quote_ident($1) || '.' || quote_ident($2))::regclass
		) AS oq`, pgstatstupleSchema)

		var result IndexBloatStats
		err := pgxscan.Get(ctx, pg.Conn, &result, qry, schema, index)
		return &result, err
	}
}

// QuoteIdentifier quotes an identifier for use in SQL statements.
func QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("\"%s\"", identifier)
}
