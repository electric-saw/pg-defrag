package main

import (
	"os"
	"strconv"

	"github.com/electric-saw/pg-defrag/pkg/sys"
)

func main() {
	pid, e := strconv.Atoi(os.Args[1])
	if e != nil {
		panic(e)
	}

	if e := sys.SetPriorityPID(pid, 15); e != nil {
		panic(e.Error())
	}

	if e := sys.SetIOPriorityPID(pid, 3); e != nil {
		panic(e.Error())
	}
}
