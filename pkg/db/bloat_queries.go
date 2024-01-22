package db

const (
	qryTableBloatPgstattuple = `
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
    ) as sq limit 1;`

	qryTableBloatStatistical = `
SELECT  CASE WHEN tblpages - est_tblpages_ff > 0
    THEN (tblpages-est_tblpages_ff)*bs
    ELSE 0
  END AS free_space,
  CASE WHEN tblpages > 0 AND tblpages - est_tblpages_ff > 0
    THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float
    ELSE 0
  END AS free_percent
FROM (
  SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
    ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
    tblpages, fillfactor, bs, tblid, schemaname, tblname, heappages, toastpages
  FROM (
    SELECT
      ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
        - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
        - CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END
      ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
      toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, fillfactor
    FROM (
      SELECT
        tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
        tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
        coalesce(toast.reltuples, 0) AS toasttuples,
        coalesce(substring(
          array_to_string(tbl.reloptions, ' ')
          FROM 'fillfactor=([0-9]+)')::smallint, 100) AS fillfactor,
        current_setting('block_size')::numeric AS bs,
        CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
        24 AS page_hdr,
        23 + CASE WHEN MAX(coalesce(s.null_frac,0)) > 0 THEN ( 7 + count(s.attname) ) / 8 ELSE 0::int END
           + CASE WHEN bool_or(att.attname = 'oid' and att.attnum < 0) THEN 4 ELSE 0 END AS tpl_hdr_size,
        sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 0) ) AS tpl_data_size
      FROM pg_attribute AS att
        JOIN pg_class AS tbl ON att.attrelid = tbl.oid
        JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
        LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname
          AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
        LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
      WHERE NOT att.attisdropped
        AND tbl.relkind in ('r','m')
        AND ns.nspname = $1
        AND tbl.relname = $2
      GROUP BY 1,2,3,4,5,6,7,8,9,10
      ORDER BY 2,3
    ) AS s
  ) AS s2
) AS s3;
`

	qryIndexBloatPgstattuple = `
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
        CROSS JOIN %q.pgstatindex(
            quote_ident($1) || '.' || quote_ident($2)) AS pgsi
        WHERE pg_catalog.pg_class.oid = (quote_ident($1) || '.' || quote_ident($2))::regclass
    ) AS oq`

	qryIndexBloatStatistical = `
    SELECT CASE WHEN relpages > est_pages_ff
      THEN bs*(relpages-est_pages_ff)
      ELSE 0
    END AS free_space,
    100 * (relpages-est_pages_ff)::float / relpages AS free_percent
  FROM (
    SELECT coalesce(1 +
           ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0 -- ItemIdData size + computed avg size of a tuple (nulldatahdrwidth)
        ) AS est_pages,
        coalesce(1 +
           ceil(reltuples/floor((bs-pageopqdata-pagehdr)*fillfactor/(100*(4+nulldatahdrwidth)::float))), 0
        ) AS est_pages_ff,
        bs, nspname, tblname, idxname, relpages, fillfactor
    FROM (
        SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, idxoid, fillfactor,
              ( index_tuple_hdr_bm +
                  maxalign - CASE -- Add padding to the index tuple header to align on MAXALIGN
                    WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
                    ELSE index_tuple_hdr_bm%maxalign
                  END
                + nulldatawidth + maxalign - CASE -- Add padding to the data to align on MAXALIGN
                    WHEN nulldatawidth = 0 THEN 0
                    WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
                    ELSE nulldatawidth::integer%maxalign
                  END
              )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata
        FROM (
            SELECT n.nspname, i.tblname, i.idxname, i.reltuples, i.relpages,
                i.idxoid, i.fillfactor, current_setting('block_size')::numeric AS bs,
                CASE -- MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?)
                  WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
                  ELSE 4
                END AS maxalign,
                /* per page header, fixed size: 20 for 7.X, 24 for others */
                24 AS pagehdr,
                /* per page btree opaque data */
                16 AS pageopqdata,
                /* per tuple header: add IndexAttributeBitMapData if some cols are null-able */
                CASE WHEN max(coalesce(s.null_frac,0)) = 0
                    THEN 8 -- IndexTupleData size
                    ELSE 8 + (( 32 + 8 - 1 ) / 8) -- IndexTupleData size + IndexAttributeBitMapData size ( max num filed per index + 8 - 1 /8)
                END AS index_tuple_hdr_bm,
                /* data len: we remove null values save space using it fractionnal part from stats */
                sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth
            FROM (
                SELECT ct.relname AS tblname, ct.relnamespace, ic.idxname, ic.attpos, ic.indkey, ic.indkey[ic.attpos], ic.reltuples, ic.relpages, ic.tbloid, ic.idxoid, ic.fillfactor,
                    coalesce(a1.attnum, a2.attnum) AS attnum, coalesce(a1.attname, a2.attname) AS attname, coalesce(a1.atttypid, a2.atttypid) AS atttypid,
                    CASE WHEN a1.attnum IS NULL
                    THEN ic.idxname
                    ELSE ct.relname
                    END AS attrelname
                FROM (
                    SELECT idxname, reltuples, relpages, tbloid, idxoid, fillfactor, indkey,
                        pg_catalog.generate_series(1,indnatts) AS attpos
                    FROM (
                        SELECT ci.relname AS idxname, ci.reltuples, ci.relpages, i.indrelid AS tbloid,
                            i.indexrelid AS idxoid,
                            coalesce(substring(
                                array_to_string(ci.reloptions, ' ')
                                from 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor,
                            i.indnatts,
                            pg_catalog.string_to_array(pg_catalog.textin(
                                pg_catalog.int2vectorout(i.indkey)),' ')::int[] AS indkey
                        FROM pg_catalog.pg_index i
                        JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
                        WHERE ci.relam=(SELECT oid FROM pg_am WHERE amname = 'btree')
                        AND ci.relpages > 0
                        AND ci.relnamespace = ($1)::regnamespace
                        AND ci.relname = $2
                    ) AS idx_data
                ) AS ic
                JOIN pg_catalog.pg_class ct ON ct.oid = ic.tbloid
                LEFT JOIN pg_catalog.pg_attribute a1 ON
                    ic.indkey[ic.attpos] <> 0
                    AND a1.attrelid = ic.tbloid
                    AND a1.attnum = ic.indkey[ic.attpos]
                LEFT JOIN pg_catalog.pg_attribute a2 ON
                    ic.indkey[ic.attpos] = 0
                    AND a2.attrelid = ic.idxoid
                    AND a2.attnum = ic.attpos
              ) i
              JOIN pg_catalog.pg_namespace n ON n.oid = i.relnamespace
              JOIN pg_catalog.pg_stats s ON s.schemaname = n.nspname
                                        AND s.tablename = i.attrelname
                                        AND s.attname = i.attname
              GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        ) AS rows_data_stats
    ) AS rows_hdr_pdg_stats
  ) AS relation_stats;
    `
)
