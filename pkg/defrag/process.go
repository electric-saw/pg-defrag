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
	"github.com/jackc/pgx/v5"
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
	NoInitialVacuum bool
	InitialReindex  bool
	NoReindex       bool
	Force           bool
	RoutineVacuum   bool
	Jobs            []JobInfo
	log             Logger
	Pg              *db.PgConnection
}

type TableInfo struct {
	BaseStats *db.PgSizeStats
	Stats     *db.PgSizeStats
}

func NewProcessor(connStr string, log Logger) (*Process, error) {
	conn, err := db.NewConnection(context.Background(), connStr, log)
	if err != nil {
		return nil, fmt.Errorf("can't create connection: %v", err)
	}

	process := &Process{
		log:             log,
		Pg:              conn,
		NoInitialVacuum: false,
		InitialReindex:  false,
		NoReindex:       false,
		Force:           false,
		RoutineVacuum:   true,
	}

	return process, nil
}

func (p *Process) RunReindexTable(ctx context.Context, schema, table string) (bool, error) {
	if len(schema) == 0 {
		supposedSchema, err := p.Pg.GetSchemaOfTable(ctx, table)
		if err != nil {
			return false, fmt.Errorf("can't get schema %w", err)
		}
		schema = supposedSchema
	}

	return p.Pg.ReindexTable(ctx, schema, table, false, false)
}

func (p *Process) Run(ctx context.Context) (bool, error) {
	if err := p.setLowPiority(p.Pg); err != nil {
		return false, err
	}

	if err := p.Pg.SetSessionReplicaRole(ctx); err != nil {
		p.log.Errorf("can't set session replica role: %v", err)
	}

	if err := p.Pg.CreateCleanPageFunction(ctx); err != nil {
		p.log.Errorf("can't create clean page function: %v", err)

		return false, err
	}

	defer func() {
		if err := p.Pg.DropCleanPageFunction(context.Background()); err != nil {
			p.log.Errorf("can't drop clean page function: %v", err)
		}
	}()

	p.log.Infof("clean page function created")

	if err := p.Pg.CreatePgStatTupleExtension(ctx); err != nil {
		p.log.Errorf("can't create pgstattuple extension: %v", err)

		return false, err
	}

	p.log.Infof("pgstattuple extension ensured")

	for _, job := range p.Jobs {
		if len(job.Schema) == 0 {
			supposedSchema, err := p.Pg.GetSchemaOfTable(ctx, job.Table)
			if err != nil {
				return false, fmt.Errorf("can't get schema %w", err)
			}
			job.Schema = supposedSchema
		}

		if cleaned, err := p.process(ctx, job.Schema, job.Table, 0, new(TableInfo)); err != nil {
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
			p.log.Warnf("can't apply ionice on pid %d, run 'ionice -c 3 -p %d'", pid, pid)
		}
	} else {
		p.log.Warnf("can't apply ionice on pid %d, run o database host 'ionice -c 3 -p %d'", pid, pid)
	}
	return nil
}

//gocyclo:ignore
func (p *Process) process(ctx context.Context, schema, table string, attepmt int, tableInfo *TableInfo) (bool, error) {
	p.log.Infof("defragmenting table %s.%s", schema, table)
	isSkipped := false
	isLocked := false
	var err error

	if ok, err := p.Pg.TryAdvisoryLock(ctx, schema, table); err != nil {
		return false, fmt.Errorf("can't try advisory lock: %v", err)
	} else {
		isLocked = ok
	}

	if isLocked {
		p.log.Infof("Table %s.%s is locked, skipping defragmentation", schema, table)
		return false, nil
	}

	if stats, err := p.Pg.GetPgSizeStats(ctx, schema, table); err != nil {
		return false, fmt.Errorf("can't get Pg size stats: %v", err)
	} else {
		tableInfo.Stats = stats
		if tableInfo.BaseStats == nil {
			tableInfo.BaseStats = stats
		}
	}

	if !isLocked && !p.NoInitialVacuum {
		start := time.Now()

		if err := p.Pg.Vacuum(ctx, schema, table, false); err != nil {
			return false, fmt.Errorf("can't do initial vacuum table %s.%s: %v", schema, table, err)
		}

		elapsed := time.Since(start)

		if stats, err := p.Pg.GetPgSizeStats(ctx, schema, table); err != nil {
			return false, fmt.Errorf("can't get Pg size stats: %v", err)
		} else {
			tableInfo.Stats = stats
		}

		p.log.Infof("vacuum initial: %d pages left on table %s.%s took %s", tableInfo.Stats.PageCount, schema, table, elapsed)

		if tableInfo.Stats.PageCount <= 1 {
			p.log.Infof("Skipping defragmentation of table %s.%s, it has no pages", schema, table)
			isSkipped = true
		}
	}

	isReindexed := false
	var bloatStats *db.PgBloatStats

	if !isLocked && !isSkipped {
		if p.InitialReindex && !p.NoReindex && attepmt == 0 {
			isReindexed, err = p.Pg.ReindexTable(ctx, schema, table, p.Force, p.NoReindex)
			if err != nil {
				return false, fmt.Errorf("can't do initial reindex table %s.%s: %v", schema, table, err)
			}
		}

		getStatsTime := time.Now()

		bloatStats, err = p.Pg.GetBloatStats(ctx, schema, table)
		if err != nil {
			return false, fmt.Errorf("can't get bloat stats: %v", err)
		}

		if bloatStats.EffectivePageCount > 0 {
			p.log.Infof("bloat statistics with pgstattuple: duration %s", time.Since(getStatsTime))
		} else {
			analyzeTime := time.Now()
			if err := p.Pg.Analyze(ctx, schema, table); err != nil {
				return false, fmt.Errorf("can't do analyze table %s.%s: %v", schema, table, err)
			} else {
				p.log.Infof("analyze table %s.%s took %s", schema, table, time.Since(analyzeTime))
			}

			getStatsTime = time.Now()
			bloatStats, err = p.Pg.GetBloatStats(ctx, schema, table)
			if err != nil {
				return false, fmt.Errorf("can't get bloat stats: %v", err)
			}

			if bloatStats.EffectivePageCount > 0 {
				p.log.Infof("bloat statistics with pgstattuple: duration %s", time.Since(getStatsTime))
			} else {
				p.log.Info("Can't get bloat statistics with pgstattuple, skipping defragmentation")
				isSkipped = true
			}
		}
	}

	if !isLocked && !isSkipped {

		canBeCompacted := bloatStats.FreePercent > 0 && tableInfo.Stats.PageCount > bloatStats.EffectivePageCount

		if canBeCompacted {
			p.log.Infof("Statistics: %d pages (%d pages including toasts and indexes), it is expected that ~%0.3f%% (%d pages) can be compacted with the estimated space saving being %s.",
				tableInfo.Stats.PageCount,
				tableInfo.Stats.TotalPageCount,
				bloatStats.FreePercent,
				(tableInfo.Stats.PageCount - bloatStats.EffectivePageCount),
				utils.Humanize(bloatStats.FreeSpace))
		} else {
			p.log.Warnf("Statistics: %d pages (%d pages including toasts and indexes)",
				tableInfo.Stats.PageCount,
				tableInfo.Stats.TotalPageCount)
		}

		if exists, err := p.Pg.HasTriggers(ctx, schema, table); err != nil {
			return false, fmt.Errorf("can't check trigger exists: %v", err)
		} else if exists {
			p.log.Warnf("Table %s.%s has 'always' or 'replica' triggers , skipping defragmentation", schema, table)
			isSkipped = true
		}

		if !p.Force {
			if tableInfo.Stats.PageCount < params.MINIMAL_COMPACT_PAGES {
				p.log.Warnf("Table %s.%s has less than %d (actual is %d) pages, skipping defragmentation",
					schema,
					table,
					params.MINIMAL_COMPACT_PAGES,
					tableInfo.Stats.PageCount)
				isSkipped = true
			}

			if bloatStats.FreePercent < params.MINIMAL_COMPACT_PERCENT*100 {
				p.log.Warnf("Table %s.%s has less than %0.f%% space to compact (actual is %0.3f%%), skipping defragmentation",
					schema,
					table,
					params.MINIMAL_COMPACT_PERCENT*100,
					bloatStats.FreePercent)
				isSkipped = true
			}
		}
	}

	if !isLocked && !isSkipped {
		if p.Force {
			p.log.Warn("Force mode is enabled, defragmentation will be performed")
		}

		vacuumPageCount := int64(0)
		initialSizeStats := tableInfo.Stats.Copy()
		toPage := tableInfo.Stats.PageCount - 1
		progressReportTime := time.Now()
		cleanPagesTotalDuration := time.Duration(0)
		lastLoop := tableInfo.Stats.PageCount - 1

		expectedPageCount := tableInfo.Stats.PageCount
		columnName, err := p.Pg.GetUpdateColumn(ctx, schema, table)
		if err != nil {
			return false, fmt.Errorf("can't get update column of table %s.%s: %v", schema, table, err)
		}

		pagesPerRound := p.Pg.GetPagesPerRound(tableInfo.BaseStats.PageCount, toPage)
		pagesBeforeVacuum := p.Pg.GetPagesBeforeVacuum(tableInfo.Stats.PageCount, expectedPageCount)

		maxTuplesPerPage, err := p.Pg.GetMaxTuplesPerPage(ctx, schema, table)
		if err != nil {
			return false, fmt.Errorf("can't get max tuples per page: %v", err)
		}

		p.log.Infof("update by column: %s", columnName)
		p.log.Infof("set page/round: %d", pagesPerRound)
		p.log.Infof("pages pages/vacuum: %d", pagesBeforeVacuum)

		var startTime time.Time
		var lastToPage int64
		var loop int64
	pageLoop:
		for loop = tableInfo.Stats.PageCount; loop > 0; loop-- {
			startTime = time.Now()

			tx, err := p.Pg.Conn.Begin(ctx)
			if err != nil {
				return false, fmt.Errorf("can't begin transaction: %v", err)
			}

			lastToPage = toPage

			toPage, err = p.Pg.CleanPages(ctx, schema, table, columnName, lastToPage, pagesPerRound, maxTuplesPerPage)
			cleanPagesTotalDuration += time.Since(startTime)

			if err != nil {
				if errRollback := tx.Rollback(ctx); errRollback != nil {
					p.log.Errorf("Rollbacking because of error: %v", err)
					return false, fmt.Errorf("can't rollback transaction: %v", errRollback)
				}
				switch {
				case strings.Contains(err.Error(), "deadlock detected"):
					p.log.Error("detected deadlock during cleaning")
					continue pageLoop
				case strings.Contains(err.Error(), "cannot extract system attribute"):
					p.log.Error("system attribute extraction error has occurred, processing stopped")
					break pageLoop
				case errors.Is(err, pgx.ErrNoRows):
					p.log.Errorf("incorrect result of cleaning: column_name %s, to_page %d, pages_per_round %d, max_tupples_per_page %d",
						columnName, lastToPage, pagesPerRound, maxTuplesPerPage)
				default:
					return false, fmt.Errorf("can't clean pages: %v", err)
				}
			} else {
				if toPage == -1 {
					if err := tx.Rollback(context.Background()); err != nil {
						p.log.Errorf("can't rollback cleaning transaction: %v", err)
					}
					toPage = lastToPage
					break pageLoop
				}

				if err := tx.Commit(context.Background()); err != nil {
					return false, fmt.Errorf("can't commit cleaning transaction: %v", err)
				}
			}

			sleepTime := sleepTimeCalculate(time.Since(startTime))

			if sleepTime > 0 {
				time.Sleep(sleepTime)
			}

			if time.Since(progressReportTime) >= params.PROGRESS_REPORT_PERIOD && lastToPage != toPage {
				progressPercentage := float64(-1)

				if bloatStats.EffectivePageCount > 0 {
					progressPercentage = float64(100) * (float64(initialSizeStats.PageCount-toPage-1) / float64(tableInfo.BaseStats.PageCount-bloatStats.EffectivePageCount))
				}
				p.log.Infof("Progress: %.2f%%, %d pages cleaned, %d pages left",
					progressPercentage,
					tableInfo.Stats.PageCount-toPage,
					(tableInfo.BaseStats.PageCount-bloatStats.EffectivePageCount)-(initialSizeStats.PageCount-toPage-1))
				progressReportTime = time.Now()
			}

			expectedPageCount -= pagesPerRound
			vacuumPageCount += (lastToPage - toPage)

			if p.RoutineVacuum && vacuumPageCount >= pagesBeforeVacuum {
				duration := cleanPagesTotalDuration.Seconds() / (float64(lastLoop) - float64(loop))

				avgDuration := 0.0001
				if duration > 0 {
					avgDuration = duration
				}

				p.log.Warnf("Cleaning in average: %.1f pages/second (%.1f seconds per %d pages).",
					float64(pagesPerRound)/avgDuration,
					avgDuration,
					pagesPerRound)

				cleanPagesTotalDuration = time.Duration(0)
				lastLoop = loop

				vacuumStart := time.Now()

				if err := p.Pg.Vacuum(ctx, schema, table, false); err != nil {
					return false, fmt.Errorf("can't vacuum table %s.%s: %v", schema, table, err)
				}

				vacuumTime := time.Since(vacuumStart)

				if tableInfo.Stats.PageCount > toPage+1 {
					p.log.Infof("Vacuum routine: can not clean %d pages, %d pages left, duration %s",
						tableInfo.Stats.PageCount-toPage-1,
						tableInfo.Stats.PageCount,
						vacuumTime)
				} else {
					p.log.Infof("Vacuum routine: %d pages left, duration %s", tableInfo.Stats.PageCount, vacuumTime)
				}

				vacuumPageCount = 0

				lastPagesBeforeVacuum := pagesBeforeVacuum
				pagesBeforeVacuum = p.Pg.GetPagesBeforeVacuum(tableInfo.Stats.PageCount, expectedPageCount)
				if pagesBeforeVacuum != lastPagesBeforeVacuum {
					p.log.Warnf("Set pages/vacuum: %d", pagesBeforeVacuum)
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

			pagesPerRound = p.Pg.GetPagesPerRound(tableInfo.Stats.PageCount, toPage)

			if pagesPerRound != lastPagesPerRound {
				p.log.Warnf("Set pages/round: %d", pagesPerRound)
			}
		}

		if loop == 0 {
			p.log.Warnf("Max loop reached")
		}

		if toPage > 0 {
			vacuumStart := time.Now()
			time.Sleep(1 * time.Second)
			if err := p.Pg.Vacuum(ctx, schema, table, false); err != nil {
				return false, fmt.Errorf("can't vacuum table %s.%s: %v", schema, table, err)
			}

			vacuumTime := time.Since(vacuumStart)

			if stats, err := p.Pg.GetPgSizeStats(ctx, schema, table); err != nil {
				return false, fmt.Errorf("can't get table info after vacuum: %v", err)
			} else {
				tableInfo.Stats = stats
			}

			if tableInfo.Stats.PageCount > toPage+pagesPerRound {
				p.log.Infof("Vacuum final: cannot clean %d pages, %d pages left, duration %s",
					tableInfo.Stats.PageCount-toPage-pagesPerRound,
					tableInfo.Stats.PageCount,
					vacuumTime)
			} else {
				p.log.Infof("Vacuum final: %d pages left, duration %s",
					tableInfo.Stats.PageCount,
					vacuumTime)
			}
		}

		analyzeStart := time.Now()
		if err := p.Pg.Analyze(ctx, schema, table); err != nil {
			return false, fmt.Errorf("can't analyze table %s.%s: %v", schema, table, err)
		}

		analyzeTime := time.Since(analyzeStart)

		p.log.Infof("Analyze final: duration %s", analyzeTime)

		getStatsStart := time.Now()

		bloatStats, err = p.Pg.GetBloatStats(ctx, schema, table)
		if err != nil {
			return false, fmt.Errorf("can't get table stats after analyze: %v", err)
		}

		p.log.Infof("Bloat statistics with pgstattuple: duration %s", time.Since(getStatsStart))
	}

	willBeSkipped := !isLocked && (isLocked || tableInfo.Stats.PageCount < params.MINIMAL_COMPACT_PAGES || bloatStats.FreePercent < params.MINIMAL_COMPACT_PERCENT*100)

	if !isLocked && (!p.NoReindex && (!isSkipped || attepmt >= params.MAX_RETRY_COUNT || (!isReindexed && !isSkipped && attepmt == 0))) {
		if ok, err := p.Pg.ReindexTable(ctx, schema, table, p.Force, p.NoReindex); err != nil {
			p.log.Errorf("can't reindex table %s.%s: %v", schema, table, err)
			isReindexed = ok || isReindexed
		} else {
			isReindexed = ok || isReindexed
		}

		if !p.NoReindex {
			stats, err := p.Pg.GetPgSizeStats(ctx, schema, table)
			if err != nil {
				return false, fmt.Errorf("can't get table info after reindex: %v", err)
			}

			tableInfo.Stats = stats
		}
	}

	if !isLocked && !(isSkipped && !isReindexed) {
		// TODO  && (defined $is_reindexed ? $is_reindexed : 1));
		complete := ((willBeSkipped || isSkipped) && isReindexed)
		if complete {
			p.log.Info("Processing completed")
		} else {
			p.log.Info("Incomplete processing")
		}

		if bloatStats.FreePercent > 0 && tableInfo.Stats.PageCount > bloatStats.EffectivePageCount && !complete {
			p.log.Infof("Processing results: %d pages (%d pages including toasts and indexes), size has been reduced by %s (%s including toasts and indexes) in total. This attempt has been initially expected to compact ~%.2f%% more space (%d pages, %s)",
				tableInfo.Stats.PageCount,
				tableInfo.Stats.TotalPageCount,
				utils.Humanize(tableInfo.BaseStats.Size-tableInfo.Stats.Size),
				utils.Humanize(tableInfo.BaseStats.TotalSize-tableInfo.Stats.TotalSize),
				bloatStats.FreePercent,
				tableInfo.Stats.PageCount-bloatStats.EffectivePageCount,
				utils.Humanize(bloatStats.FreeSpace),
			)
		} else {
			p.log.Infof("Processing results: %d pages left (%d pages including toasts and indexes), size reduced by %s (%s including toasts and indexes) in total.",
				tableInfo.Stats.PageCount,
				tableInfo.Stats.TotalPageCount,
				utils.Humanize(tableInfo.BaseStats.Size-tableInfo.Stats.Size),
				utils.Humanize(tableInfo.BaseStats.TotalSize-tableInfo.Stats.TotalSize),
			)
		}
	}

	return (!isLocked || isSkipped || willBeSkipped) && (isReindexed || !p.NoReindex), nil
}

func (p *Process) Close() {
	p.Pg.Close(context.Background())
}

func sleepTimeCalculate(d time.Duration) time.Duration {
	dur := time.Duration(float64(d) * params.DELAY_RATIO)
	if dur > params.MAX_DELAY {
		dur = params.MAX_DELAY
	}

	return dur
}
