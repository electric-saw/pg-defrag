package defrag

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/electric-saw/pg-defrag/pkg/db"
	"github.com/electric-saw/pg-defrag/pkg/params"
	"github.com/electric-saw/pg-defrag/pkg/sys"
	"github.com/electric-saw/pg-defrag/pkg/utils"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

type Logger interface {
	logrus.FieldLogger
	IsLevelEnabled(logrus.Level) bool
}

type JobInfo struct {
	Schema string
	Table  string
}

type Process struct {
	InitialVacuum  bool
	InitialReindex bool
	NoReindex      bool
	Force          bool
	RoutineVacuum  bool
	Jobs           []JobInfo
	log            Logger
	pg             *db.PgConnection
}

type TableInfo struct {
	InitialStats *db.PgSizeStats
	Stats        *db.PgSizeStats
}

func NewProcessor(connStr string, log Logger) (*Process, error) {
	conn, err := db.NewConnection(context.Background(), connStr, log)
	if err != nil {
		return nil, fmt.Errorf("can't create connection: %v", err)
	}

	process := &Process{
		log: log,
		pg:  conn,
	}

	return process, nil
}

func (p *Process) RunReindexTable(ctx context.Context, schema, table string) (bool, int, error) {
	if len(schema) == 0 {
		supposedSchema, err := p.pg.GetSchemaOfTable(ctx, table)
		if err != nil {
			return false, 0, fmt.Errorf("can't get schema %w", err)
		}
		schema = supposedSchema
	}

	return p.pg.ReindexTable(ctx, schema, table, false)
}

func (p *Process) Run(ctx context.Context) (bool, error) {
	if err := p.setLowPiority(p.pg); err != nil {
		return false, err
	}

	if err := p.pg.SetSessionReplicaRole(ctx); err != nil {
		p.log.Errorf("can't set session replica role: %v", err)
	}

	if fName, err := p.pg.CreateCleanPageFunction(ctx); err != nil {
		p.log.Errorf("can't create clean page function: %v", err)

		return false, err
	} else {
		p.log.Infof("clean page function created: %s", fName)
		defer func() {
			if err := p.pg.DropCleanPageFunction(context.Background()); err != nil {
				p.log.Errorf("can't drop clean page function: %v", err)
			}
		}()
	}

	for _, job := range p.Jobs {
		if len(job.Schema) == 0 {
			supposedSchema, err := p.pg.GetSchemaOfTable(ctx, job.Table)
			if err != nil {
				return false, fmt.Errorf("can't get schema %w", err)
			}
			job.Schema = supposedSchema
		}

		if cleaned, err := p.process(ctx, job.Schema, job.Table); err != nil {
			return false, err
		} else if !cleaned {
			return cleaned, nil
		}

	}

	return true, nil
}

func (p *Process) setLowPiority(pg *db.PgConnection) error {
	pid := pg.GetPID()
	if pid == 0 || pg.Conn.Config().User == "postgres" {
		if err := sys.SetIOPriorityPID(int(pid), sys.IOPRIO_CLASS_IDLE); err != nil {
			p.log.Error(err)
			p.log.Warnf("can´t apply ionice on pid %d, run 'ionice -c 3 -p %d'", pid, pid)
		}
	} else {
		p.log.Warnf("can´t apply ionice on pid %d, run o database host 'ionice -c 3 -p %d'", pid, pid)
	}
	return nil
}

func (p *Process) process(ctx context.Context, schema, table string) (bool, error) {
	p.log.Infof("defragmenting table %s.%s", schema, table)
	isLocked := false
	tableInfo := &TableInfo{}
	isSkipped := false

	p.log.Infof("try advisory lock")
	if locked, err := p.pg.TryAdvisoryLock(ctx, schema, table); err != nil {
		return false, fmt.Errorf("can't try advisory lock: %v", err)
	} else if locked {
		p.log.Infof("another instance is working with table %s.%s", schema, table)
		isLocked = locked
	} else {
		isLocked = locked
		p.log.Infof("table %s.%s is unlocked", schema, table)
	}

	if stats, err := p.pg.GetPgSizeStats(ctx, schema, table); err != nil {
		return false, fmt.Errorf("can't get pg size stats: %v", err)
	} else {
		tableInfo.Stats = stats
		if tableInfo.InitialStats == nil {
			tableInfo.InitialStats = stats
		}
	}

	if p.InitialVacuum {
		start := time.Now()
		if err := p.pg.Vacuum(ctx, schema, table); err != nil {
			return false, fmt.Errorf("can't do initial vacuum table %s.%s: %v", schema, table, err)
		}
		elapsed := time.Since(start)

		if stats, err := p.pg.GetPgSizeStats(ctx, schema, table); err != nil {
			return false, fmt.Errorf("can't get pg size stats: %v", err)
		} else {
			tableInfo.Stats = stats
		}
		p.log.Infof("initial vacuum: %d pages left on table %s.%s took %s", tableInfo.Stats.PageCount, schema, table, elapsed)
	}

	if tableInfo.Stats.PageCount <= 1 {
		p.log.Infof("table %s.%s has no pages, skipping defragmentation", schema, table)
		isSkipped = true
	}

	isReindexed := false
	attempt := 0
	if p.InitialReindex && !p.NoReindex {
		if ok, reindexAttempt, err := p.pg.ReindexTable(ctx, schema, table, p.Force); err != nil {
			return false, fmt.Errorf("can't do initial reindex table %s.%s: %v", schema, table, err)
		} else {
			isReindexed = ok
			attempt = reindexAttempt
		}
	}

	bloatStats, err := p.pg.GetBloatStats(ctx, schema, table)
	if err != nil {
		return false, fmt.Errorf("can't get bloat stats: %v", err)
	}

	if bloatStats.EffectivePageCount > 0 {
		p.log.Infof("bloat statistics with pgstattuple")
	} else {
		analyzeTime := time.Now()
		if err := p.pg.Analyze(ctx, schema, table); err != nil {
			return false, fmt.Errorf("can't do analyze table %s.%s: %v", schema, table, err)
		} else {
			p.log.Infof("analyze table %s.%s took %s", schema, table, time.Since(analyzeTime))
		}

		bloatStats, err = p.pg.GetBloatStats(ctx, schema, table)
		if err != nil {
			return false, fmt.Errorf("can't get bloat stats: %v", err)
		}

	}

	if !isLocked && !isSkipped {
		canBeCompacted := bloatStats.FreePercent > 0 && tableInfo.Stats.PageCount > bloatStats.EffectivePageCount
		if canBeCompacted {
			p.log.Infof("statistics: %d pages (%d pages including toasts and indexes), it is expected that ~%0.3f%% (%d pages) can be compacted with the estimated space saving being %s.",
				tableInfo.Stats.PageCount,
				tableInfo.Stats.TotalPageCount,
				bloatStats.FreePercent,
				(tableInfo.Stats.PageCount - bloatStats.EffectivePageCount),
				utils.Humanize(bloatStats.FreeSpace))
		} else {
			p.log.Infof("statistics: %d pages (%d pages including toasts and indexes)",
				tableInfo.Stats.PageCount,
				tableInfo.Stats.TotalPageCount)
		}

		if exists, err := p.pg.HasTrigger(ctx, schema, table); err != nil {
			return false, fmt.Errorf("can't check trigger exists: %v", err)
		} else if exists {
			p.log.Warnf("table %s.%s has 'always' or 'replica' triggers , skipping defragmentation", schema, table)
			isSkipped = true
		}

		if !p.Force {
			if tableInfo.Stats.PageCount < params.MINIMAL_COMPACT_PAGES {
				p.log.Warnf("table %s.%s has less than %d (actual is %d) pages, skipping defragmentation",
					schema,
					table,
					params.MINIMAL_COMPACT_PAGES,
					tableInfo.Stats.PageCount)
				isSkipped = true
			}

			if bloatStats.FreePercent < params.MINIMAL_COMPACT_PERCENT {
				p.log.Warnf("table %s.%s has less than %0.d%% space to compact (actualis %0.3f%%), skipping defragmentation",
					schema,
					table,
					params.MINIMAL_COMPACT_PERCENT,
					bloatStats.FreePercent)
				isSkipped = true
			}
		}
	}

	if !isLocked && !isSkipped {
		vacuumPageCount := int64(0)
		initialSizeStats := tableInfo.Stats
		toPage := tableInfo.Stats.PageCount - 1
		progressReportTime := time.Now()
		cleanPagesTotalDurarion := int64(0)
		lastLoop := tableInfo.Stats.PageCount - 1

		expectedPageCount := tableInfo.Stats.PageCount
		columnName, err := p.pg.GetUpdateColumn(ctx, schema, table)
		if err != nil {
			return false, fmt.Errorf("can't get update column of table %s.%s: %v", schema, table, err)
		}

		pagesPerRound := p.pg.GetPagesPerRound(tableInfo.InitialStats.PageCount, toPage)
		pagesBeforeVacuum := p.pg.GetPagesBeforeVacuum(tableInfo.Stats.PageCount, expectedPageCount)

		maxTuplesPerPage, err := p.pg.GetMaxTuplesPerPage(ctx, schema, table)
		if err != nil {
			return false, fmt.Errorf("can't get max tuples per page: %v", err)
		}

		p.log.Infof("update by column: %s", columnName)
		p.log.Infof("set page/round: %d", pagesPerRound)
		p.log.Infof("pages pages/vacuum: %d", pagesBeforeVacuum)

		var lastToPage int64
		var loop int64
		for loop = tableInfo.Stats.PageCount; loop > 0; loop-- {
			tx, err := p.pg.Conn.Begin(ctx)
			if err != nil {
				return false, fmt.Errorf("can't begin transaction: %v", err)
			}

			// startTime := time.Now()
			lastToPage = toPage

			toPage, err = p.pg.CleanPages(ctx, schema, table, columnName, lastToPage, pagesPerRound, maxTuplesPerPage)
			if err != nil {
				if err := tx.Rollback(ctx); err != nil {
					return false, fmt.Errorf("can't rollback transaction: %v", err)
				}
				switch {
				case strings.Contains(err.Error(), "deadlock detected"):
					p.log.Errorf("deadlock detected, retrying")
				}
				if strings.Contains(err.Error(), "deadlock detected") {
					p.log.Error("detected deadlock during cleaning")
					continue
				} else if strings.Contains(err.Error(), "cannot extract system attribute") {
					p.log.Error("system attribute extraction error has occurred, processing stopped")
					break
				} else if errors.Is(err, pgx.ErrNoRows) {
					p.log.Errorf("incorrect result of cleaning: column_name %s, to_page %d, pages_per_round %d, max_tupples_per_page %d",
						columnName, lastToPage, pagesPerRound, maxTuplesPerPage)
				} else {
					return false, fmt.Errorf("can't clean pages: %v", err)
				}
			} else {
				if err := tx.Commit(ctx); err != nil {
					return false, fmt.Errorf("can't commit transaction: %v", err)
				}
				if toPage == -1 {
					toPage = lastToPage
					break
				}
			}

			// sleepTime := 2 * time.Second /* TODO: makeit configurable (time.Since(startTime)) */
			// if sleepTime > 0 {
			// 	time.Sleep(sleepTime)
			// }

			// TODO: after round statements?

			if time.Since(progressReportTime) >= params.PROGRESS_REPORT_PERIOD {
				var pct int64 = 1
				if bloatStats.EffectivePageCount > 0 {
					pct = (100 * (initialSizeStats.PageCount - toPage - 1) / (tableInfo.Stats.PageCount - bloatStats.EffectivePageCount))
				}

				cleaned := (tableInfo.Stats.PageCount - toPage - 1)

				p.log.Infof("progress: %d%%, %d pages cleaned, %d pages left", pct, cleaned, (tableInfo.InitialStats.PageCount-bloatStats.EffectivePageCount)-cleaned)

				progressReportTime = time.Now()
			}

			expectedPageCount -= pagesPerRound
			vacuumPageCount += (lastToPage - toPage)

			if p.RoutineVacuum && vacuumPageCount >= pagesBeforeVacuum {
				dur := cleanPagesTotalDurarion / (lastLoop - loop)
				var avgDur float64
				if dur > 0 {
					avgDur = float64(dur)
				} else {
					avgDur = 0.0001
				}
				p.log.Warnf("cleaning in average: %.1f pages/second (%d seconds per %d pages).",
					float64(pagesPerRound)/avgDur, dur, pagesPerRound)

				cleanPagesTotalDurarion = 0
				lastLoop = loop

				vacuumTime := time.Now()
				if err := p.pg.Vacuum(ctx, schema, table); err != nil {
					return false, fmt.Errorf("can't vacuum table %s.%s: %v", schema, table, err)
				}
				vacuumTimeTotal := time.Since(vacuumTime)

				newStats, err := p.pg.GetPgSizeStats(ctx, schema, table)
				if err != nil {
					return false, fmt.Errorf("can't get table stats after vacuum: %v", err)
				}
				tableInfo.Stats = newStats

				if tableInfo.Stats.PageCount > (toPage + 1) {
					p.log.Warnf("vacuum routine: can not clean %d pages, %d pages left, duration %s",
						tableInfo.Stats.PageCount-toPage-1,
						tableInfo.Stats.PageCount,
						vacuumTimeTotal)
				} else {
					p.log.Infof("vacuum routine: %d pages left, duration %s", tableInfo.Stats.PageCount, vacuumTimeTotal)
				}

				vacuumPageCount = 0

				lastPagesBeforeVacuum := pagesBeforeVacuum
				pagesBeforeVacuum = p.pg.GetPagesBeforeVacuum(tableInfo.Stats.PageCount, expectedPageCount)
				if pagesBeforeVacuum != lastPagesBeforeVacuum {
					p.log.Warnf("set pages/vacuum: %d", pagesBeforeVacuum)
				}
			}

			if toPage >= tableInfo.Stats.PageCount {
				toPage = tableInfo.Stats.PageCount - 1
			}

			if toPage <= 1 {
				toPage = 0
				break
			}

			lastPagesPerRound := pagesPerRound
			pagesPerRound = p.pg.GetPagesPerRound(tableInfo.Stats.PageCount, expectedPageCount)

			if pagesPerRound != lastPagesPerRound {
				p.log.Warnf("set pages/round: %d", pagesPerRound)
			}
		}
		if loop == 0 {
			p.log.Warn("max loop reached")
		}

		if toPage > 0 {
			vacuumTime := time.Now()
			if err := p.pg.Vacuum(ctx, schema, table); err != nil {
				return false, fmt.Errorf("can't vacuum table %s.%s: %v", schema, table, err)
			}
			vacuumTimeTotal := time.Since(vacuumTime)

			newStats, err := p.pg.GetPgSizeStats(ctx, schema, table)
			if err != nil {
				return false, fmt.Errorf("can't get table stats after vacuum: %v", err)
			}
			tableInfo.Stats = newStats

			if tableInfo.Stats.PageCount > (toPage + pagesPerRound) {
				p.log.Warnf("vacuum final: can not clean %d pages, %d pages left, duration %s",
					tableInfo.Stats.PageCount-toPage-1,
					tableInfo.Stats.PageCount,
					vacuumTimeTotal)
			} else {
				p.log.Infof("vacuum final: %d pages left, duration %s", tableInfo.Stats.PageCount, vacuumTimeTotal)
			}
		}

		analyzeTime := time.Now()
		if err := p.pg.Analyze(ctx, schema, table); err != nil {
			return false, fmt.Errorf("can't analyze table %s.%s: %v", schema, table, err)
		}
		analyzeTimeTotal := time.Since(analyzeTime)
		p.log.Infof("analyze final: duration %s", analyzeTimeTotal)

		getStatTime := time.Now()
		newBloatStats, err := p.pg.GetBloatStats(ctx, schema, table)
		if err != nil {
			return false, fmt.Errorf("can't get table stats after analyze: %v", err)
		}
		bloatStats = newBloatStats

		p.log.Infof("bloat statistics with pgstattuple: duration %s", time.Since(getStatTime))
	}

	willBeSkipped := (!isLocked &&
		(isSkipped || tableInfo.Stats.PageCount < params.MINIMAL_COMPACT_PAGES || bloatStats.FreePercent < params.MINIMAL_COMPACT_PERCENT))

	if !isLocked && (!p.NoReindex && (!isSkipped || attempt == 3) || (!isReindexed && isSkipped && attempt < 3)) {
		if ok, _, err := p.pg.ReindexTable(ctx, schema, table, p.Force); err != nil {
			return false, fmt.Errorf("can't reindex table %s.%s: %v", schema, table, err)
		} else {
			isReindexed = ok || isReindexed
		}

		if !p.NoReindex {
			newTableStats, err := p.pg.GetPgSizeStats(ctx, schema, table)
			if err != nil {
				return false, fmt.Errorf("can't get table info after reindex: %v", err)
			}
			tableInfo.Stats = newTableStats
		}
	}

	if !isLocked && !(isSkipped && !isReindexed) {
		completed := (willBeSkipped || !isSkipped) && isReindexed
		if completed {
			p.log.Info("processing completed")
		} else {
			p.log.Info("incomplete processing")
		}

		if bloatStats.FreePercent > 0 && tableInfo.Stats.PageCount > bloatStats.EffectivePageCount && !completed {
			p.log.Warnf("processing results: %d pages (%d pages including toasts and indexes), size has been reduced by %s (%s including toasts and indexes) in total. This attempt has been initially expected to compact ~%f%% more space (%d pages, %s)",
				tableInfo.Stats.PageCount,
				tableInfo.Stats.TotalPageCount,
				utils.Humanize(tableInfo.InitialStats.Size-tableInfo.Stats.Size),
				utils.Humanize(tableInfo.InitialStats.TotalSize-tableInfo.Stats.TotalSize),
				bloatStats.FreePercent,
				tableInfo.Stats.PageCount-bloatStats.EffectivePageCount,
				utils.Humanize(bloatStats.FreeSpace),
			)
		} else {
			p.log.Warnf("processing results: %d pages left (%d pages including toasts and indexes), size reduced by %s (%s including toasts and indexes) in total.",
				tableInfo.Stats.PageCount,
				tableInfo.Stats.TotalPageCount,
				utils.Humanize(tableInfo.InitialStats.Size-tableInfo.Stats.Size),
				utils.Humanize(tableInfo.InitialStats.TotalSize-tableInfo.Stats.TotalSize),
			)
		}
	}

	return (!isLocked || isSkipped || willBeSkipped) && (isReindexed || !p.NoReindex), nil
}

func (p *Process) Close() {
	p.pg.Close(context.Background())
}
