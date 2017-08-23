package main

import (
	"flag"
	"io"
	"os"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/envflag"
	"github.com/pippio/gazette/envflagfactory"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
	_ "github.com/pippio/graph" // Used for querying topic partitions.
	"github.com/pippio/topics"
)

var (
	begin = flag.Int64("begin", 0,
		"Start tailing at given offset. Not valid in topic mode.")
	bytes     = flag.Int64("n", 1048576, "Number of bytes to tail.")
	follow    = flag.Bool("f", false, "Continue tailing until interrupt.")
	topicMode = flag.Bool("topic", false,
		"Argument refers to a topic, and <bytes> should be read from each one.")
	gazetteEndpoint = envflagfactory.NewGazetteServiceEndpoint()
)

func main() {
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

	var name = journal.Name(flag.Arg(0))
	if *topicMode {
		if *begin > 0 {
			log.Fatal("cannot use -topic and -begin together")
		}

		var t *topic.Description
		var ok bool
		if t, ok = topics.ByName[flag.Arg(0)]; !ok {
			log.WithField("name", flag.Arg(0)).Fatal("no such topic")
		}

		for _, j := range t.Partitions() {
			processJournal(client, j)
		}
	} else {
		processJournal(client, name)
	}
}

func processJournal(client *gazette.Client, name journal.Name) {
	// Make a HEAD request to get the writehead.
	var args = journal.ReadArgs{Journal: name}
	var result journal.ReadResult
	if result, _ = client.Head(args); result.Error != nil {
		log.WithField("err", result.Error).Fatal("failed to HEAD journal")
	}

	var offset int64
	if offset = *begin; offset == 0 {
		offset = result.WriteHead - *bytes
	}

	var mark = journal.Mark{
		Journal: journal.Name(name),
		Offset:  offset,
	}
	if mark.Offset < 0 {
		log.WithField("head", result.WriteHead).Warn(
			"journal is smaller than bytes requested, reading entire journal")
		mark.Offset = 0
	}

	var reader io.Reader = journal.NewRetryReader(mark, client)
	log.WithFields(log.Fields{
		"name": name,
		"mark": mark,
	}).Info("now reading partition")

	if !*follow {
		reader = io.LimitReader(reader, *bytes)
	}

	if _, err := io.Copy(os.Stdout, reader); err != nil {
		log.WithField("err", result.Error).Fatal("failed to read journal data")
	}
}
