//go:build !linux

package sys

const (
	IOPRIO_WHO_PROCESS = 1
	IOPRIO_WHO_PGRP    = 2
	IOPRIO_WHO_USER    = 3

	IOPRIO_CLASS_NONE = 0
	IOPRIO_CLASS_RT   = 1
	IOPRIO_CLASS_BE   = 2
	IOPRIO_CLASS_IDLE = 3

	IOPRIO_PRIO_MASK   = ((uint32(1) << IOPRIO_CLASS_SHIFT) - 1)
	IOPRIO_CLASS_SHIFT = uint32(13)
)

func SetIOPriorityPID(pid int, priority uint32) error {
	// SetIOPriorityPID is not implemented.
	return nil
}
