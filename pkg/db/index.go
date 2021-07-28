package db

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/electric-saw/pg-defrag/pkg/params"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/sirupsen/logrus"
)

type IndexDefinition struct {
	IndexName    string
	Tablespace   string
	IndexDef     string
	IndexMethod  string
	ConName      string
	ConTypeDef   string
	Allowed      bool
	IsFunctional bool
	IsDeferrable bool
	IsDeferred   bool
	IdxSize      int64
}

type IndexStats struct {
	Size      int64
	PageCount int64
}

type IndexBloatStats struct {
	FreePerctent int
	FreeSpace    int64
}

func (pg *PgConnection) ReindexTable(ctx context.Context, schema, table string, force bool) (bool, int, error) {
	var isReindexed bool = false
	var lockAttemp = 0

	if indexDataList, err := pg.GetIndexList(ctx, schema, table); err != nil {
		return false, 0, fmt.Errorf("failed on get index data: %w", err)
	} else {
		for _, index := range indexDataList {
			initialIndexStats, err := pg.GetIndexSizeStatistics(ctx, schema, index.IndexName)
			if err != nil {
				return false, 0, fmt.Errorf("failed on get index size statistics: %w", err)
			}

			if initialIndexStats.PageCount <= 1 {
				pg.log.Infof("skipping reindex: %s.%s, empty or 1 page only", schema, index.IndexName)
				continue
			}

			indexBloatStats, err := pg.getIndexBloatStats(ctx, schema, index.IndexName)
			if err != nil {
				return false, 0, fmt.Errorf("failed on get index bloat stats: %w", err)
			}

			if !force {
				if index.IndexMethod != "btree" {
					pg.log.Infof("skipping reindex: %s.%s, not btree, reindexing is up to you.", schema, index.IndexName)
					pg.log.Warnf("reindex queries: %s.%s, initial size %d pages (%s)", schema, index.IndexName, initialIndexStats.PageCount, humanize.Bytes(uint64(initialIndexStats.Size)))
					if index.Allowed {
						pg.log.Warnf("%s; --%s", pg.getReindexQuery(index), pg.Conn.Config().Database)
						pg.log.Warnf("%s; --%s", pg.getAlterIndexQuery(schema, table, index), pg.Conn.Config().Database)
					} else {
						pg.log.Warnf("%s; --%s", pg.getStraightReindexQuery(schema, index), pg.Conn.Config().Database)
					}
					continue
				}

				if initialIndexStats.PageCount < params.MINIMAL_COMPACT_PAGES {
					pg.log.Infof("skipping reindex: %s.%s, too few pages (%d and the minimum is %d)", schema, index.IndexName, initialIndexStats.PageCount, params.MINIMAL_COMPACT_PAGES)
					continue
				}

				if indexBloatStats.FreePerctent > params.MINIMAL_COMPACT_PERCENT {
					pg.log.Infof("skipping reindex: %s.%s, %d%% free space is below required %d%%", schema, index.IndexName, indexBloatStats.FreePerctent, params.MINIMAL_COMPACT_PERCENT)
					continue
				}

			}

			if !index.Allowed {
				pg.log.Infof("skip reindex: %s.%s, can not reindex without heavy locks because of its dependencies, reindexing is up to you.", schema, index.IndexName)
				pg.log.Warnf("reindex queries: %s.%s, initial size %d pages (%s), will be reduced by %d%% (%s)",
					schema,
					index.IndexName,
					initialIndexStats.PageCount,
					humanize.Bytes(uint64(initialIndexStats.Size)),
					indexBloatStats.FreePerctent,
					humanize.Bytes(uint64(indexBloatStats.FreeSpace)))
				pg.log.Warnf("%s; --%s", pg.getReindexQuery(index), pg.Conn.Config().Database)
				pg.log.Warnf("%s; --%s", pg.getAlterIndexQuery(schema, table, index), pg.Conn.Config().Database)
				continue
			}

			startedAt := time.Now()

			err = pg.Reindex(ctx, index)
			if err != nil {
				pg.log.Warnf("skipped reindex: %s.%s, %s", schema, index.IndexName, err)
				continue
			}

			if index.IsFunctional {
				analyzeStartedAt := time.Now()
				if err := pg.Analyze(ctx, schema, table); err != nil {
					pg.log.Warnf("failed on analyze: %s.%s, %s", schema, index.IndexName, err.Error())
				} else {
					pg.log.Infof("auto analyze functional index, duration %s", time.Since(analyzeStartedAt))
				}
			}

			for lockAttemp < 3 {
				if err := pg.AlterIndex(ctx, schema, table, index); err != nil {
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
				newStats, err := pg.GetIndexSizeStatistics(ctx, schema, index.IndexName)
				if err != nil {
					return isReindexed, lockAttemp, fmt.Errorf("failed on get index size statistics: %w", err)
				}

				freePct := 100 * (1 - float64(newStats.Size)/float64(initialIndexStats.Size))
				freeSpace := (initialIndexStats.Size - newStats.Size)
				pg.log.Warnf("reindex: %s.%s, initial size %d (%s), has been reduced by %f%% (%s), duration %s, attemps %d",
					schema,
					index.IndexName,
					initialIndexStats.PageCount,
					humanize.Bytes(uint64(initialIndexStats.Size)),
					freePct,
					humanize.Bytes(uint64(freeSpace)),
					time.Since(startedAt),
					lockAttemp)
				isReindexed = true
			} else {
				if err := pg.dropTempIndex(ctx, schema); err != nil {
					pg.log.Errorf("unable to drop temporary index: %s.pgcompact_index_%d, %s", schema, pg.GetPID(), err)
					return isReindexed, lockAttemp, err
				}

				pg.log.Warnf("reindex: %s.%s, lock has not been acquired, initial size %d pages (%s)",
					schema,
					index.IndexName,
					initialIndexStats.PageCount,
					humanize.Bytes(uint64(initialIndexStats.Size)))

			}

			if pg.log.IsLevelEnabled(logrus.DebugLevel) {
				pg.log.Debugf("reindex queries: %s.%s, initial size %d pages (%s), will be reduced by %d%% (%s), duration %s",
					schema,
					index.IndexName,
					initialIndexStats.PageCount,
					humanize.Bytes(uint64(initialIndexStats.Size)),
					indexBloatStats.FreePerctent,
					humanize.Bytes(uint64(indexBloatStats.FreeSpace)),
					time.Since(startedAt))

				pg.log.Warnf("%s; --%s", pg.getReindexQuery(index), pg.Conn.Config().Database)
				pg.log.Warnf("%s; --%s", pg.getAlterIndexQuery(schema, table, index), pg.Conn.Config().Database)
			}

		}
	}
	return isReindexed, lockAttemp, nil
}

func (pg *PgConnection) Reindex(ctx context.Context, index *IndexDefinition) error {
	_, err := pg.Conn.Exec(ctx, pg.getReindexQuery(index))
	return err
}

func (pg *PgConnection) AlterIndex(ctx context.Context, schema, table string, index *IndexDefinition) error {
	for _, sql := range strings.Split(pg.getAlterIndexQuery(schema, table, index), ";") {
		_, err := pg.Conn.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pg *PgConnection) GetIndexList(ctx context.Context, schema, table string) ([]*IndexDefinition, error) {
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
    )::integer AS allowed,
    (
        SELECT string_to_array(indkey::text, ' ')::int2[] operator(pg_catalog.@>) array[0::int2]
        FROM pg_catalog.pg_index
        WHERE indexrelid = indexoid
    )::integer as is_functional,
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

	var result []*IndexDefinition

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
	err := pgxscan.Select(ctx, pg.Conn, &result, qry, schema, index)
	return result, err

}

func (pg *PgConnection) getReindexQuery(index *IndexDefinition) string {
	var qry string = index.IndexDef
	var re = regexp.MustCompile(`(?m)INDEX (\S+)`)
	var re2 = regexp.MustCompile(`(?m)( WHERE .*|$)`)

	qry = re.ReplaceAllString(qry, fmt.Sprintf("INDEX CONCURRENTLY pgcompact_index_%d", pg.GetPID()))
	if index.Tablespace != "" {
		qry = re2.ReplaceAllString(qry, fmt.Sprintf(" TABLESPACE %s $1", index.Tablespace))
	}
	return qry
}

func (pg *PgConnection) dropTempIndex(ctx context.Context, schema string) error {
	_, err := pg.Conn.Exec(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY pgcompact_index_%d", pg.GetPID()))
	return err
}

func (pg *PgConnection) getAlterIndexQuery(schema, table string, index *IndexDefinition) string {

	if len(index.ConName) > 0 {
		constraintName := QuoteIdentifier(index.ConName)
		constraintOptions := fmt.Sprintf("%s using index pgcompact_index_%d", index.ConTypeDef, pg.GetPID())

		if index.IsDeferrable {
			constraintOptions = fmt.Sprintf(" %s deferrable", constraintOptions)
		}

		if index.IsDeferred {
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
        alter index %s.pgcompact_index_%d rename to %s;
        end;
        drop index concurrently %s.%s;
        `, schema, index.IndexName, randIndex, schema, pg.GetPID(), randIndex, schema, randIndex)

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
