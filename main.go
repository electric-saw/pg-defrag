package main

import (
	"context"
	"fmt"

	"github.com/electric-saw/pg-defrag/pkg/defrag"
	"github.com/sirupsen/logrus"
)

func main() {

	p, err := defrag.NewProcessor("localhost:5432", logrus.New())
	if err != nil {
		logrus.Fatal(err)
	}

	p.Tables = append(p.Tables, "route", "leg")

	defer p.Close()

	if cleaned, err := p.Run(context.Background()); err != nil {
		logrus.Panic(err)
	} else {
		fmt.Printf("cleaned: %v", cleaned)
	}

}
