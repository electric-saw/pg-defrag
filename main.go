package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/electric-saw/pg-defrag/pkg/defrag"
	"github.com/sirupsen/logrus"
)

func main() {

	p, err := defrag.NewProcessor(os.Getenv("PG_CONNECTION_STRING"),
		logrus.New())
	if err != nil {
		logrus.Fatal(err)
	}

	p.Tables = strings.Split(os.Getenv("PG_TABLES"), ",")
	p.InitialReindex = false // test: ok
	p.InitialVacuum = false  // test: ok
	p.RoutineVacuum = false
	p.Force = true

	defer p.Close()

	if cleaned, err := p.Run(context.Background()); err != nil {
		logrus.Panic(err)
	} else {
		fmt.Printf("cleaned: %v", cleaned)
	}

}
