//go:build linux

package sys

import "syscall"

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
	priority = ((priority) << IOPRIO_CLASS_SHIFT) | (0 & IOPRIO_PRIO_MASK)
	_, _, err := syscall.Syscall(syscall.SYS_IOPRIO_SET, uintptr(IOPRIO_WHO_PROCESS), uintptr(pid), uintptr(priority))
	if err != 0 {
		return err
	}
	return nil
}
