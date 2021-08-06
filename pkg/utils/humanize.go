package utils

import (
	"math"

	"github.com/dustin/go-humanize"
)

func Humanize(size int64) string {
	abs := math.Abs(float64(size))

	return humanize.Bytes(uint64(abs))

}
