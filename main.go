package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/electric-saw/pg-defrag/pkg/defrag"
	"github.com/electric-saw/pg-defrag/pkg/params"
	"github.com/pterm/pterm"
	"github.com/sirupsen/logrus"
)

func main() {
	p, err := defrag.NewProcessor(os.Getenv("PG_CONNECTION_STRING"),
		logrus.New())
	if err != nil {
		logrus.Fatal(err)
	}

	p.Jobs = jobList(p)
	p.NoInitialVacuum = true

	params.PROGRESS_REPORT_PERIOD = 5 * time.Second

	defer p.Close()

	if cleaned, err := p.Run(context.Background()); err != nil {
		logrus.Panic(err)
	} else {
		fmt.Printf("cleaned: %v", cleaned)
	}
}

func jobList(p *defrag.Process) []defrag.JobInfo {
	jobs := []defrag.JobInfo{}

	tablesEnv := os.Getenv("PG_TABLES")

	tablesToDefrag := []string{}

	if tablesEnv == "" || tablesEnv == "*" {
		res, err := p.Pg.Conn.Query(
			context.Background(),
			"SELECT schemaname || '.' || relname FROM pg_stat_user_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')",
		)
		if err != nil {
			logrus.Fatal(err)
		}

		for res.Next() {
			var table string
			if err := res.Scan(&table); err != nil {
				logrus.Fatal(err)
			}
			if !slices.Contains([]string{
				"_timescaledb_catalog.metadata",
				"_timescaledb_catalog.chunk",
			}, table) {
				tablesToDefrag = append(tablesToDefrag, table)
			}
		}

		slices.Sort(tablesToDefrag)

		tablesToDefrag, _ = pterm.
			DefaultInteractiveMultiselect.
			WithOptions(tablesToDefrag).
			WithDefaultText("Select tables to defrag").
			WithFilter(true).
			// WithKeyConfirm(keys.Enter).
			// WithKeySelect(keys.Space).
			WithCheckmark(&pterm.Checkmark{Checked: pterm.Green("+"), Unchecked: pterm.Red("-")}).
			Show()

		if len(tablesToDefrag) == 0 {
			logrus.Info("No tables selected, exiting")
			os.Exit(0)
		}
	} else {
		tablesToDefrag = strings.Split(tablesEnv, ",")
	}

	for _, name := range tablesToDefrag {
		splitedName := strings.Split(name, ".")
		if len(splitedName) == 2 {
			jobs = append(jobs, defrag.JobInfo{
				Schema: splitedName[0],
				Table:  splitedName[1],
			})
		} else {
			jobs = append(jobs, defrag.JobInfo{
				Table: splitedName[0],
			})
		}
	}

	return jobs
}
