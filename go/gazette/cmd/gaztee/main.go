package main

import (
	"flag"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/envflag"
	"github.com/LiveRamp/gazette/pkg/envflagfactory"
	"github.com/LiveRamp/gazette/pkg/gazette"
	"github.com/LiveRamp/gazette/pkg/journal"
)

func main() {
	var gazetteEndpoint = envflagfactory.NewGazetteServiceEndpoint()

	log.SetOutput(os.Stderr)
	envflag.CommandLine.Parse()
	flag.Parse()

	client, err := gazette.NewClient(*gazetteEndpoint)
	if err != nil {
		log.WithField("err", err).Fatal("failed to connect to gazette")
	}

	if len(flag.Args()) == 0 {
		log.Fatal("journal name required")
	}

	name := journal.Name(flag.Arg(0))
	if err := client.Create(name); err != nil && err != journal.ErrExists {
		log.WithField("err", err).Fatal("failed to create journal")
	}

	var writeService = gazette.NewWriteService(client)
	writeService.Start()
	defer writeService.Stop() // Flush writes on exit

	if _, err := writeService.ReadFrom(name, os.Stdin); err != nil {
		log.WithField("err", err).Fatal("failed to append to journal")
	}
}
