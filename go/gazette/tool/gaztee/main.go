package main

import (
	"flag"
	"os"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/envflag"
	"github.com/pippio/gazette/envflagfactory"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
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
