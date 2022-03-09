package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/electric-saw/pg-defrag/pkg/defrag"
	"github.com/electric-saw/pg-defrag/pkg/params"
	"github.com/sirupsen/logrus"
)

func main() {

	p, err := defrag.NewProcessor(os.Getenv("PG_CONNECTION_STRING"),
		logrus.New())
	if err != nil {
		logrus.Fatal(err)
	}

	p.Jobs = jobList()
	p.NoReindex = true
	p.RoutineVacuum = true
	p.InitialVacuum = true
	p.Force = true
	params.MINIMAL_COMPACT_PERCENT = 0.10

	params.MINIMAL_COMPACT_PAGES = 5
	params.MINIMAL_COMPACT_PERCENT = .25
	params.PAGES_PER_ROUND_DIVISOR = 100
	params.MAX_PAGES_PER_ROUND = 100
	params.PAGES_BEFORE_VACUUM_LOWER_DIVISOR = 16
	params.PAGES_BEFORE_VACUUM_LOWER_THRESHOLD = 10000
	params.PAGES_BEFORE_VACUUM_UPPER_DIVISOR = 32
	params.PROGRESS_REPORT_PERIOD = 5 * time.Second

	defer p.Close()

	if cleaned, err := p.Run(context.Background()); err != nil {
		logrus.Panic(err)
	} else {
		fmt.Printf("cleaned: %v", cleaned)
	}
}

func jobList() []defrag.JobInfo {
	jobs := []defrag.JobInfo{}
	for _, name := range strings.Split(os.Getenv("PG_TABLES"), ",") {
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
