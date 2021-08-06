package db

import (
	"context"
	"fmt"

	"github.com/electric-saw/pg-defrag/pkg/params"
)

func (pg *PgConnection) CreateCleanPageFunction(ctx context.Context) (string, error) {
	qry := `
	CREATE OR REPLACE FUNCTION public.pg_defrag_clean_pages_%d(
		i_table_ident text,
		i_column_ident text,
		i_to_page integer,
		i_page_offset integer,
		i_max_tupples_per_page integer)
	RETURNS integer
	LANGUAGE plpgsql AS $$
	DECLARE
		_from_page integer := i_to_page - i_page_offset + 1;
		_min_ctid tid;
		_max_ctid tid;
		_ctid_list tid[];
		_next_ctid_list tid[];
		_ctid tid;
		_loop integer;
		_result_page integer;
		_update_query text :=
			'UPDATE ONLY ' || i_table_ident ||
			' SET ' || i_column_ident || ' = ' || i_column_ident ||
			' WHERE ctid = ANY($1) RETURNING ctid';
	BEGIN
		-- Check page argument values
		IF NOT (
			i_page_offset IS NOT NULL AND i_page_offset >= 1 AND
			i_to_page IS NOT NULL AND i_to_page >= 1 AND
			i_to_page >= i_page_offset)
		THEN
			RAISE EXCEPTION 'Wrong page arguments specified.';
		END IF;
		-- Check that session_replication_role is set to replica to
		-- prevent triggers firing
		IF NOT (
			SELECT setting = 'replica'
			FROM pg_catalog.pg_settings
			WHERE name = 'session_replication_role')
		THEN
			RAISE EXCEPTION 'The session_replication_role must be set to replica.';
		END IF;
		-- Define minimal and maximal ctid values of the range
		_min_ctid := (_from_page, 1)::text::tid;
		_max_ctid := (i_to_page, i_max_tupples_per_page)::text::tid;
		-- Build a list of possible ctid values of the range
		SELECT array_agg((pi, ti)::text::tid)
		INTO _ctid_list
		FROM generate_series(_from_page, i_to_page) AS pi
		CROSS JOIN generate_series(1, i_max_tupples_per_page) AS ti;
		<<_outer_loop>>
		FOR _loop IN 1..i_max_tupples_per_page LOOP
			_next_ctid_list := array[]::tid[];
			-- Update all the tuples in the range
			FOR _ctid IN EXECUTE _update_query USING _ctid_list
			LOOP
				IF _ctid > _max_ctid THEN
					_result_page := -1;
					EXIT _outer_loop;
				ELSIF _ctid >= _min_ctid THEN
					-- The tuple is still in the range, more updates are needed
					_next_ctid_list := _next_ctid_list || _ctid;
				END IF;
			END LOOP;
			_ctid_list := _next_ctid_list;
			-- Finish processing if there are no tupples in the range left
			IF coalesce(array_length(_ctid_list, 1), 0) = 0 THEN
				_result_page := _from_page - 1;
				EXIT _outer_loop;
			END IF;
		END LOOP;
		-- No result
		IF _loop = i_max_tupples_per_page AND _result_page IS NULL THEN
			RAISE EXCEPTION
				'Maximal loops count has been reached with no result.';
		END IF;
		RETURN _result_page;
	END $$;
`
	_, err := pg.Conn.Exec(ctx, fmt.Sprintf(qry, pg.GetPID()))
	return fmt.Sprintf("pg_defrag_clean_pages_%d", pg.GetPID()), err
}

func (pg *PgConnection) DropCleanPageFunction(ctx context.Context) error {
	_, err := pg.Conn.Exec(ctx, fmt.Sprintf("drop function pg_defrag_clean_pages_%d", pg.GetPID()))
	return err
}

func (pg *PgConnection) CleanPages(ctx context.Context, schema, table string, column string, toPage, pagesPerRound, maxTupplesPerPage int64) (int64, error) {
	qry := fmt.Sprintf("SELECT pg_defrag_clean_pages_%d($1, $2, $3, $4, $5);", pg.GetPID())

	var newToPage int64
	err := pg.Conn.QueryRow(ctx, qry,
		fmt.Sprintf("%s.%s", schema, table),
		column,
		toPage,
		pagesPerRound,
		maxTupplesPerPage).
		Scan(&newToPage)

	return newToPage, err
}

func (pg *PgConnection) GetUpdateColumn(ctx context.Context, schema, table string) (string, error) {
	qry := `
	SELECT quote_ident(attname)
		FROM pg_catalog.pg_attribute
		WHERE
		attnum > 0 AND -- neither system
		NOT attisdropped AND -- nor dropped
		attrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass
		ORDER BY
		-- Variable legth attributes have lower priority because of the chance
		-- of being toasted
		(attlen = -1),
		-- Preferably not indexed attributes
		(
			attnum::text IN (
				SELECT regexp_split_to_table(indkey::text, ' ')
				FROM pg_catalog.pg_index
				WHERE indrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass)),
		-- Preferably smaller attributes
		attlen,
		attnum
		LIMIT 1;
	`

	var column string
	err := pg.Conn.QueryRow(ctx, qry, schema, table).Scan(&column)
	return column, err
}

func (pg *PgConnection) GetMaxTuplesPerPage(ctx context.Context, schema, table string) (int64, error) {
	qry := fmt.Sprintf(`
	SELECT ceil(current_setting('block_size')::real / sum(attlen))
	FROM pg_catalog.pg_attribute
	WHERE
		attrelid = '%s.%s'::regclass AND attnum < 0;
`, QuoteIdentifier(schema), QuoteIdentifier(table))

	var maxTupplesPerPage int64
	err := pg.Conn.QueryRow(ctx, qry).Scan(&maxTupplesPerPage)
	return maxTupplesPerPage, err
}

func (pg *PgConnection) GetPagesPerRound(pageCount, toPage int64) int64 {
	var realPagesPerRound int64
	var pagesPerRound int64

	if pageCount > params.PAGES_PER_ROUND_DIVISOR {
		realPagesPerRound = pageCount
	} else {
		realPagesPerRound = params.PAGES_PER_ROUND_DIVISOR
	}

	if realPagesPerRound < params.MAX_PAGES_PER_ROUND {
		pagesPerRound = realPagesPerRound
	} else {
		pagesPerRound = params.MAX_PAGES_PER_ROUND
	}

	if pagesPerRound >= toPage {
		pagesPerRound = toPage
	}
	return pagesPerRound
}

func (pg *PgConnection) GetPagesBeforeVacuum(pageCount, expectedPageCount int64) int64 {
	var pages int64

	if (pageCount / params.PAGES_BEFORE_VACUUM_LOWER_DIVISOR) < params.PAGES_BEFORE_VACUUM_LOWER_THRESHOLD {
		pages = pageCount / params.PAGES_BEFORE_VACUUM_LOWER_DIVISOR
	} else {
		pages = pageCount / params.PAGES_BEFORE_VACUUM_LOWER_THRESHOLD
	}

	if !(pages > (expectedPageCount / params.PAGES_BEFORE_VACUUM_UPPER_DIVISOR)) {
		pages = expectedPageCount / params.PAGES_BEFORE_VACUUM_UPPER_DIVISOR
	}
	return pages
}
