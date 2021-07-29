package params

import "time"

const (
	MINIMAL_COMPACT_PAGES               = 10
	MINIMAL_COMPACT_PERCENT             = 20
	PAGES_PER_ROUND_DIVISOR             = 1000
	MAX_PAGES_PER_ROUND                 = 10
	PAGES_BEFORE_VACUUM_LOWER_DIVISOR   = 16
	PAGES_BEFORE_VACUUM_LOWER_THRESHOLD = 1000
	PAGES_BEFORE_VACUUM_UPPER_DIVISOR   = 50
	PROGRESS_REPORT_PERIOD              = 60 * time.Second
	LOG_ALWAYS                          = 1000
	LOG_ERROR                           = 2
	LOG_WARNING                         = 1
	LOG_NOTICE                          = 0
)
