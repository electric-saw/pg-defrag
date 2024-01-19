package params

import (
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

type (
	maxPagesPerRoundFunc func(int64) int64
	BloatMetricType      int
)

const (
	BLOAT_METRIC_PGSTATTUPLE BloatMetricType = iota
	BLOAT_METRIC_STATISTICAL

	BLOAT_METRIC_DEFAULT = BLOAT_METRIC_PGSTATTUPLE
)

var (
	MINIMAL_COMPACT_PAGES               int64                = envOrDefaultInt64("MINIMAL_COMPACT_PAGES", 10)
	MINIMAL_COMPACT_PERCENT             float64              = envOrDefaultfloat64("MINIMAL_COMPACT_PERCENT", .20)
	PAGES_PER_ROUND_DIVISOR             int64                = envOrDefaultInt64("PAGES_PER_ROUND_DIVISOR", 1000)
	MAX_PAGES_PER_ROUND                 int64                = envOrDefaultInt64("MAX_PAGES_PER_ROUND", -1)
	MAX_PAGES_PER_ROUND_FUNC            maxPagesPerRoundFunc = maxPagesPerRoundCalc
	PAGES_BEFORE_VACUUM_LOWER_DIVISOR   int64                = envOrDefaultInt64("PAGES_BEFORE_VACUUM_LOWER_DIVISOR", 16)
	PAGES_BEFORE_VACUUM_LOWER_THRESHOLD int64                = envOrDefaultInt64("PAGES_BEFORE_VACUUM_LOWER_THRESHOLD", 1000)
	PAGES_BEFORE_VACUUM_UPPER_DIVISOR   int64                = envOrDefaultInt64("PAGES_BEFORE_VACUUM_UPPER_DIVISOR", 50)
	PROGRESS_REPORT_PERIOD              time.Duration        = 60 * time.Second
	DELAY_RATIO                         float64              = envOrDefaultfloat64("DELAY_RATIO", 2)
	MAX_DELAY                           time.Duration        = time.Second
	BLOAT_METRIC_SOURCE                                      = BLOAT_METRIC_DEFAULT

	MAX_RETRY_COUNT int = 10
)

func envOrDefaultInt64(env string, def int64) int64 {
	raw := os.Getenv(env)
	if raw == "" {
		return def
	}

	rawInt, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		logrus.Warnf("ENV %s: Could not parse %s as int64, using default value %d", env, raw, def)
		return def
	}

	return rawInt
}

func envOrDefaultfloat64(env string, def float64) float64 {
	raw := os.Getenv(env)
	if raw == "" {
		return def
	}

	rawInt, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		logrus.Warnf("ENV %s: Could not parse %s as float64, using default value %f", env, raw, def)
		return def
	}

	return rawInt
}

func maxPagesPerRoundCalc(pageCount int64) int64 {
	const MIN_PAGES_PER_ROUND = 10
	const MAX_PAGES_PER_ROUND_LIMIT = 500

	if MAX_PAGES_PER_ROUND > 0 {
		return MAX_PAGES_PER_ROUND
	}

	maxPagesPerRound := int64(float64(pageCount) / float64(PAGES_PER_ROUND_DIVISOR))

	if maxPagesPerRound > pageCount {
		maxPagesPerRound = pageCount
	}

	if maxPagesPerRound > MAX_PAGES_PER_ROUND_LIMIT {
		maxPagesPerRound = maxPagesPerRoundCalc(maxPagesPerRound)
	}

	if maxPagesPerRound < MIN_PAGES_PER_ROUND && pageCount >= MIN_PAGES_PER_ROUND {
		maxPagesPerRound = MIN_PAGES_PER_ROUND
	}

	if maxPagesPerRound < 1 {
		maxPagesPerRound = 1
	}

	return maxPagesPerRound
}
