package db

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/electric-saw/pg-defrag/pkg/params"
	"github.com/electric-saw/pg-defrag/pkg/utils"
	"github.com/georgysavva/scany/v2/pgxscan"
)

const (
	reindexRetryMaxCount = 3
	reindexRetryPause    = 5 * time.Second
)

type IndexDefinition struct {
	Schema               string  `db:"schemaname"`
	Table                string  `db:"tablename"`
	IndexName            string  `db:"indexname"`
	Tablespace           string  `db:"tablespace"`
	IndexDef             string  `db:"indexdef"`
	IndexMethod          string  `db:"indmethod"`
	ConName              *string `db:"conname"`
	ConTypeDef           *string `db:"contypedef"`
	ReplaceIndexPossible bool    `db:"replace_index_possible"`
	IsFunctional         bool    `db:"is_functional"`
	IsDeferrable         *bool   `db:"is_deferrable"`
	IsDeferred           *bool   `db:"is_deferred"`
	IsExcludeConstraint  *bool   `db:"is_exclude_constraint"`
	IdxSize              int64   `db:"idxsize"`
}

type IndexStats struct {
	Size      int64 `db:"size"`
	PageCount int64 `db:"page_count"`
}

type IndexBloatStats struct {
	FreePerctent float64 `db:"free_percent"`
	FreeSpace    int64   `db:"free_space"`
}

func (pg *PgConnection) ReindexTable(ctx context.Context, schema, table string, force, noReindex bool) (bool, error) {
	var isReindexed bool = true

	indexDataList, err := pg.GetIndexList(ctx, schema, table)
	if err != nil {
		return false, fmt.Errorf("failed on get index data: %w", err)
	}

	useReindexConcurrently := pg.pgVersion >= 120000

	for _, index := range indexDataList {
		select {
		case <-ctx.Done():
			return isReindexed, nil
		default:
			initialIndexStats, err := pg.GetIndexSizeStatistics(context.Background(), schema, index.IndexName)
			if err != nil {
				return false, fmt.Errorf("failed on get index size statistics: %w", err)
			}

			if initialIndexStats.PageCount <= 1 {
				pg.log.Infof("skipping reindex: %s.%s, empty or 1 page only", schema, index.IndexName)
				continue
			}

			if index.IsExcludeConstraint != nil && *index.IsExcludeConstraint {
				pg.log.Infof("skipping reindex: %s.%s, exclude constraint", schema, index.IndexName)
				continue
			}

			indexBloatStats, err := pg.getIndexBloatStats(context.Background(), schema, index.IndexName)
			if err != nil {
				return false, fmt.Errorf("failed on get index bloat stats: %w", err)
			}

			if !force {
				if index.IndexMethod != "btree" {
					pg.log.Warnf("skipping reindex: %s.%s, not btree, reindexing is up to you.", schema, index.IndexName)
					pg.log.Warnf("reindex queries: %s.%s, initial size %d pages (%s)", schema, index.IndexName, initialIndexStats.PageCount, utils.Humanize(initialIndexStats.Size))
					if index.ReplaceIndexPossible {
						pg.log.Warnf("%s; --%s", pg.getReindexQuery(&index), pg.Conn.Config().Database)
						pg.log.Warnf("%s; --%s", pg.getAlterIndexQuery(&index), pg.Conn.Config().Database)
					} else {
						pg.log.Warnf("%s; --%s", pg.getStraightReindexQuery(&index), pg.Conn.Config().Database)
					}
					continue
				}

				if initialIndexStats.PageCount < params.MINIMAL_COMPACT_PAGES {
					pg.log.Infof("skipping reindex: %s.%s, too few pages (has %d and the minimum is %d)", schema, index.IndexName, initialIndexStats.PageCount, params.MINIMAL_COMPACT_PAGES)
					continue
				}

				if indexBloatStats.FreePerctent < params.MINIMAL_COMPACT_PERCENT {
					pg.log.Infof("skipping reindex: %s.%s, %.2f%s free space is below required %.2f%s", schema, index.IndexName, indexBloatStats.FreePerctent, "%", params.MINIMAL_COMPACT_PERCENT, "%")
					continue
				}
			}

			pg.log.Warnf("reindex: %s.%s, initial size %d pages (%s), will be reduced by %.2f%% (%s)",
				schema,
				index.IndexName,
				initialIndexStats.PageCount,
				utils.Humanize(initialIndexStats.Size),
				indexBloatStats.FreePerctent,
				utils.Humanize(indexBloatStats.FreeSpace))

			if !index.ReplaceIndexPossible && !useReindexConcurrently {
				pg.log.Infof("skipping reindex: %s.%s, can not reindex without heavy locks because of its dependencies, reindexing is up to you.", index.Schema, index.IndexName)
				pg.log.Warnf("reindex queries: %s.%s, initial size %d pages (%s), will be reduced by %d%% (%s)",
					index.Schema,
					index.IndexName,
					initialIndexStats.PageCount,
					utils.Humanize(initialIndexStats.Size),
					indexBloatStats.FreePerctent,
					utils.Humanize(indexBloatStats.FreeSpace))

				pg.log.Warnf("%s; --%s", pg.getReindexQuery(&index), pg.Conn.Config().Database)
				pg.log.Warnf("%s; --%s", pg.getAlterIndexQuery(&index), pg.Conn.Config().Database)

				continue
			}

			if !noReindex {
				if useReindexConcurrently {
					if err := pg.ReindexIndexConcurrently(ctx, &index, &initialIndexStats); err != nil {
						pg.log.Errorf("failed on reindex concurrently: %s", err)
						isReindexed = false
					}
				} else {
					if err := pg.ReindexIndexOldReplace(ctx, &index); err != nil {
						pg.log.Errorf("failed on reindex old replace: %w", err)
						isReindexed = false
					}
				}
			} else {
				pg.log.Warnf("Reindex is disabled, skipping reindex: %s.%s", index.Schema, index.IndexName)
				pg.log.Warnf("reindex queries: %s.%s, initial size %d pages (%s), will be reduced by %d%% (%s)",
					index.Schema,
					index.IndexName,
					initialIndexStats.PageCount,
					utils.Humanize(initialIndexStats.Size),
					indexBloatStats.FreePerctent,
					utils.Humanize(indexBloatStats.FreeSpace))

				pg.log.Warnf("%s; --%s", pg.getReindexQuery(&index), pg.Conn.Config().Database)
				pg.log.Warnf("%s; --%s", pg.getAlterIndexQuery(&index), pg.Conn.Config().Database)
			}
		}
	}

	return isReindexed, nil
}

func (pg *PgConnection) getTempIndexName() string {
	return fmt.Sprintf("pg_defrag_index_%d", pg.GetPID())
}

func (pg *PgConnection) Reindex(ctx context.Context, index *IndexDefinition) error {
	_, err := pg.Conn.Exec(ctx, pg.getReindexQuery(index))
	if err != nil {
		return fmt.Errorf("failed on reindex: %w", err)
	}

	return nil
}

func (pg *PgConnection) ReindexIndexConcurrently(ctx context.Context, index *IndexDefinition, initialIndexSizeStats *IndexStats) error {

	start := time.Now()

	_, err := pg.Conn.Exec(ctx, fmt.Sprintf("reindex index concurrently %s.%s", QuoteIdentifier(index.Schema), QuoteIdentifier(index.IndexName)))
	if err != nil {
		return fmt.Errorf("failed on reindex concurrently: %w", err)
	}

	newIndexSizeStats, err := pg.GetIndexSizeStatistics(ctx, index.Schema, index.IndexName)
	if err != nil {
		return fmt.Errorf("failed on get index size statistics: %w", err)
	}

	freePct := 100 * (1 - float64(newIndexSizeStats.Size)/float64(initialIndexSizeStats.Size))
	freeSpace := (initialIndexSizeStats.Size - newIndexSizeStats.Size)

	pg.log.Infof("reindex concurrently: %s.%s, initial size %d (%s), has been reduced by %.2f%% (%s), duration %s",
		index.Schema,
		index.IndexName,
		initialIndexSizeStats.PageCount,
		utils.Humanize(initialIndexSizeStats.Size),
		freePct,
		utils.Humanize(freeSpace),
		time.Since(start))

	return nil
}

func (pg *PgConnection) DropTemporaryIndex(ctx context.Context, schema string) error {
	_, err := pg.Conn.Exec(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY %s.%s;", schema, pg.getTempIndexName()))
	return err
}

func (pg *PgConnection) ReindexIndexOldReplace(ctx context.Context, index *IndexDefinition) error {
	start := time.Now()

	if err := pg.Reindex(ctx, index); err != nil {
		pg.log.Infof("Skipping reindex %s: %s", index.IndexName, err)

		if err := pg.DropTemporaryIndex(ctx, index.Schema); err != nil {
			return fmt.Errorf("unable to drop temporary index: %s.%s, %s", index.Schema, pg.getTempIndexName(), err)
		}

		return err
	}

	reindexDuration := time.Since(start)

	if index.IsFunctional {
		analyzeTime := time.Now()
		if err := pg.Analyze(ctx, index.Schema, index.Table); err != nil {
			pg.log.Errorf("Autoanalyze functional index %s.%s failed: %s", index.Schema, index.IndexName, err)

			if err := pg.DropTemporaryIndex(ctx, index.Schema); err != nil {
				pg.log.Errorf("Unable to drop temporary index: %s.%s, %s", index.Schema, pg.getTempIndexName(), err)
			}

			return err
		}

		pg.log.Infof("Autoanalyze functional index %s.%s, duration %s", index.Schema, index.IndexName, time.Since(analyzeTime))
	}

	lockedAtlerAttempt := 0
	for lockedAtlerAttempt < reindexRetryMaxCount {
		if err := pg.AlterIndex(ctx, index); err != nil {
			if strings.Contains(err.Error(), "statement timeout") {
				lockedAtlerAttempt++
				pg.log.Warnf("failed on alter index: %s.%s, %s", index.Schema, index.IndexName, err)
				time.Sleep(reindexRetryPause)
				pg.log.Infof("retry alter index: %s.%s, %d/%d", index.Schema, index.IndexName, lockedAtlerAttempt, reindexRetryMaxCount)
				continue
			} else {
				pg.log.Errorf("failed on alter index: %s.%s, %s", index.Schema, index.IndexName, err)
				return err
			}
		}

		reindexDuration = time.Since(start)
		break
	}

	if lockedAtlerAttempt < reindexRetryMaxCount {
		newStats, err := pg.GetIndexSizeStatistics(ctx, index.Schema, index.IndexName)
		if err != nil {
			return fmt.Errorf("failed on get index size statistics: %w", err)
		}

		freePct := 100 * (1 - float64(newStats.Size)/float64(index.IdxSize))
		freeSpace := (index.IdxSize - newStats.Size)

		pg.log.Infof("reindex: %s.%s, initial size %d (%s), has been reduced by %.2f%% (%s), duration %s, attempts %d",
			index.Schema,
			index.IndexName,
			index.IdxSize,
			utils.Humanize(index.IdxSize),
			freePct,
			utils.Humanize(freeSpace),
			reindexDuration,
			lockedAtlerAttempt)
	} else {
		if err := pg.DropTemporaryIndex(ctx, index.Schema); err != nil {
			pg.log.Errorf("unable to drop temporary index: %s.%s, %s", index.Schema, pg.getTempIndexName(), err)
			return err
		}

		pg.log.Warnf("reindex: %s.%s, lock has not been acquired, initial size %d pages (%s)",
			index.Schema,
			index.IndexName,
			index.IdxSize,
			utils.Humanize(index.IdxSize))
	}

	return nil
}

func (pg *PgConnection) AlterIndex(ctx context.Context, index *IndexDefinition) error {
	for _, sql := range strings.Split(pg.getAlterIndexQuery(index), ";") {
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
    schemaname,
    tablename,
    indexname,
    tablespace,
    indexdef,
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
    )::integer = 1 AS replace_index_possible,
    (
        SELECT string_to_array(indkey::text, ' ')::int2[] operator(pg_catalog.@>) array[0::int2]
        FROM pg_catalog.pg_index
        WHERE indexrelid = indexoid
    )::integer = 1 as is_functional,
    condeferrable as is_deferrable,
    condeferred as is_deferred,
    (contype = 'x') as is_exclude_constraint,
    pg_catalog.pg_relation_size(indexoid) as idxsize
FROM (
    SELECT
        schemaname, tablename,
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
	_, err := pg.Conn.Exec(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY %s.%s;", schema, pg.getTempIndexName()))
	return err
}

func (pg *PgConnection) getAlterIndexQuery(index *IndexDefinition) string {
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
        begin;
            set local statement_timeout = 0;
            alter table %s.%s drop constraint %s;
            alter table %s.%s add constraint %s %s;
        end;`, index.Schema, index.Table, constraintName, index.Schema, index.Table, constraintName, constraintOptions)
	} else {
		randIndex := fmt.Sprintf("tmp_%d", rand.Intn(1000000000))
		return fmt.Sprintf(`
        begin;
            set local statement_timeout = 0;
            alter index %s.%s rename to %s;
            alter index %s.%s rename to %s;
        end;
        drop index concurrently %s.%s;
        `,
			index.Schema, index.IndexName,
			randIndex,
			index.Schema, pg.getTempIndexName(),
			index.IndexName,
			index.Schema, randIndex)

	}

}

func (pg *PgConnection) getStraightReindexQuery(index *IndexDefinition) string {
	return fmt.Sprintf("reindex index ('%s.%s')",
		QuoteIdentifier(index.Schema),
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

func QuoteIdentifier(identifier string) string {
	return strconv.Quote(identifier)
}
