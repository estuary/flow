package main

import (
	"bufio"
	"context"
	"flag"
	"io"
	"math/rand"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/LiveRamp/gazette/examples/stream-sum"
	"github.com/LiveRamp/gazette/pkg/gazette"
	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/metrics"
	"github.com/LiveRamp/gazette/pkg/topic"
)

var configFile = flag.String("config", "/path/to/config", "Path to required configuration file.")

type Config struct {
	Chunker struct {
		NumStreams      int // Total number of streams to create. -1 for infinite.
		ChunksPerStream int // Number of stream chunks.
	}
	Gazette struct{ Endpoint string } // Gazette endpoint to use.
}

func main() {
	prometheus.MustRegister(metrics.GazetteClientCollectors()...)
	prometheus.MustRegister(metrics.GazetteConsumerCollectors()...)
	flag.Parse()

	viper.SetConfigFile(*configFile)
	if err := viper.ReadInConfig(); err != nil {
		log.WithField("err", err).Fatal("failed to read config")
	}

	// Allow environment variables to override file configuration.
	// Treat variable underscores as nested-path specifiers.
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		log.WithField("err", err).Fatal("failed to unmarshal")
	}

	client, err := gazette.NewClient(config.Gazette.Endpoint)
	if err != nil {
		log.WithField("err", err).Fatal("failed to init gazette client")
	}

	var writeService = gazette.NewWriteService(client)
	writeService.Start()
	defer writeService.Stop() // Flush writes on exit.

	var pub = topic.NewPublisher(writeService)

	// Issue an empty write (a write barrier) to ensure the "sums" Journal is created
	// and to determine its current log head to read from, prior to emitting any chunks.
	var barrier *journal.AsyncAppend
	if barrier, err = writeService.Write(journal.Name(stream_sum.Sums.Name), nil); err != nil {
		log.WithField("err", err).Fatal("failed to determine write head")
	}
	<-barrier.Ready

	var ctx, cancelCtx = context.WithCancel(context.Background())
	defer cancelCtx()

	var rr = journal.NewRetryReaderContext(ctx, journal.Mark{
		Journal: journal.Name(stream_sum.Sums.Name),
		Offset:  barrier.WriteHead,
	}, client)

	var chunkCh = make(chan stream_sum.Chunk)
	var verifyCh = make(chan stream_sum.Sum)
	var actualCh = make(chan stream_sum.Sum)

	go pumpSums(bufio.NewReader(rr), actualCh)
	go generate(config.Chunker.NumStreams, config.Chunker.ChunksPerStream, verifyCh, chunkCh)

	verify(func(chunk stream_sum.Chunk) {
		if _, err := pub.Publish(&chunk, stream_sum.Chunks); err != nil {
			log.WithField("err", err).Fatal("failed to Publish")
		}
	}, chunkCh, verifyCh, actualCh)
}

func generate(numStreams, chunksPerStream int, doneCh chan<- stream_sum.Sum, outCh chan<- stream_sum.Chunk) {
	var streams []stream_sum.Sum

	defer close(outCh)
	defer close(doneCh)

	for {
		// Select an existing stream (if i < len(streams)) or new stream (if i == len(streams)).
		var N = len(streams)

		if numStreams != 0 {
			N += 1
		} else if N == 0 {
			return
		}

		var i = rand.Intn(N)

		// Create a new stream.
		if i == len(streams) {
			streams = append(streams, stream_sum.Sum{ID: stream_sum.NewStreamID()})
			numStreams--
		}

		var data []byte
		if streams[i].SeqNo < chunksPerStream {
			data = stream_sum.FillPRNG(make([]byte, 32))
		}

		var chunk = stream_sum.Chunk{
			ID:    streams[i].ID,
			SeqNo: streams[i].SeqNo + 1,
			Data:  data,
		}

		done, err := streams[i].Update(chunk)
		if err != nil {
			panic(err) // Update fails only on invalid SeqNo.
		}
		outCh <- chunk

		if done {
			doneCh <- streams[i]
			copy(streams[i:], streams[i+1:])
			streams = streams[:len(streams)-1]
		}
	}
}

func verify(emit func(stream_sum.Chunk), chunkCh <-chan stream_sum.Chunk, verifyCh, actualCh <-chan stream_sum.Sum) {
	for chunkCh != nil || verifyCh != nil {
		// Emit chunks as they're generated, and verify as they complete. If a
		// stream is ready for verification, we block and wait for it's computed
		// sum prior to emitting any further chunks. This precludes races were
		// expected and actual stream sums potentially arrive in different orders
		// (since we can't emit a chunk which would complete a next stream before
		// we've verified the last one).

		select {
		case chunk, ok := <-chunkCh:
			if !ok {
				chunkCh = nil
				continue
			}

			emit(chunk)

		case expect, ok := <-verifyCh:
			if !ok {
				verifyCh = nil
				continue
			}

			for actual := range actualCh {
				if actual.ID == expect.ID {
					if actual != expect {
						log.WithFields(log.Fields{"actual": actual, "expect": expect}).Fatal("mis-matched sum!")
					} else {
						log.WithFields(log.Fields{"id": actual.ID}).Info("verified sum")
					}
					break
				}
			}
		}
	}
}

func pumpSums(br *bufio.Reader, actualCh chan<- stream_sum.Sum) {
	for {
		var sum stream_sum.Sum

		if b, err := stream_sum.Sums.Framing.Unpack(br); err == context.Canceled || err == io.EOF {
			return
		} else if err != nil {
			log.WithField("err", err).Fatal("unpacking frame")
		} else if err = stream_sum.Sums.Unmarshal(b, &sum); err == topic.ErrDesyncDetected {
			log.WithField("err", err).Error("failed to Unmarshal Sum")
		} else {
			actualCh <- sum
		}
	}
}
