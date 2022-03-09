package params

import "time"

var (
	MINIMAL_COMPACT_PAGES               int64         = 5
	MINIMAL_COMPACT_PERCENT             float64       = .20
	PAGES_PER_ROUND_DIVISOR             int64         = 1000
	MAX_PAGES_PER_ROUND                 int64         = 10
	PAGES_BEFORE_VACUUM_LOWER_DIVISOR   int64         = 16
	PAGES_BEFORE_VACUUM_LOWER_THRESHOLD int64         = 1000
	PAGES_BEFORE_VACUUM_UPPER_DIVISOR   int64         = 50
	PROGRESS_REPORT_PERIOD              time.Duration = 60 * time.Second
)
