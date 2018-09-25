package main

import (
	"bufio"
	"context"
	"io"
	"math/rand"
	"net/http"

	"github.com/LiveRamp/gazette/v2/examples/stream-sum"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	"github.com/LiveRamp/gazette/v2/pkg/metrics"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const iniFilename = "chunker.ini"

var Config = new(struct {
	Chunker struct {
		mbp.ZoneConfig
		Streams int `long:"streams" default:"-1" description:"Total number of streams to create. <0 for infinite"`
		Chunks  int `long:"chunks" default:"100" description:"Number of chunks per stream"`
	} `group:"Chunker" namespace:"chunker" env-namespace:"CHUNKER"`

	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
})

type runChunker struct{}

func (runChunker) Execute(args []string) error {
	defer mbp.InitDiagnosticsAndRecover(Config.Diagnostics)()
	mbp.InitLog(Config.Log)
	pb.RegisterGRPCDispatcher(Config.Chunker.Zone)
	go http.ListenAndServe(":8080", nil) // Serve diagnostics.

	log.WithField("config", Config).Info("starting chunker")
	prometheus.MustRegister(metrics.GazetteClientCollectors()...)

	var ctx = context.Background()
	var rjc = Config.Broker.RoutedJournalClient(ctx)
	var as = client.NewAppendService(ctx, rjc)

	var chunksMapping, err = stream_sum.NewChunkMapping(ctx, rjc)
	mbp.Must(err, "failed to build chunks mapping")

	// Issue an empty write (a write barrier) to determine the current log head of the
	// "sums" Journal, prior to emitting any chunks.
	var barrier = as.StartAppend(stream_sum.SumsJournal)
	barrier.Release()
	<-barrier.Done()

	var rr = client.NewRetryReader(ctx, rjc, pb.ReadRequest{
		Journal:    barrier.Response().Commit.Journal,
		Offset:     barrier.Response().Commit.End,
		Block:      true,
		DoNotProxy: !rjc.IsNoopRouter(),
	})

	var chunkCh = make(chan stream_sum.Chunk)
	var verifyCh = make(chan stream_sum.Sum)
	var actualCh = make(chan stream_sum.Sum)

	go pumpSums(bufio.NewReader(rr), actualCh)
	go generate(Config.Chunker.Streams, Config.Chunker.Chunks, verifyCh, chunkCh)

	verify(func(chunk stream_sum.Chunk) {
		if _, err := message.Publish(as, chunksMapping, &chunk); err != nil {
			log.WithField("err", err).Fatal("failed to Publish")
		}
	}, chunkCh, verifyCh, actualCh)
	return nil
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

		if b, err := message.JSONFraming.Unpack(br); err == context.Canceled || err == io.EOF {
			return
		} else if err != nil {
			log.WithField("err", err).Fatal("unpacking frame")
		} else if err = message.JSONFraming.Unmarshal(b, &sum); err != nil {
			log.WithField("err", err).Fatal("failed to Unmarshal Sum")
		} else {
			actualCh <- sum
		}
	}
}

func main() {
	var parser = flags.NewParser(Config, flags.Default)

	parser.AddCommand("run", "Run chunker", `
run chunker with the provided configuration. chunker will emit streams
of pseudo-random stream chunks. Upon completing each stream, it will confirm that
the expected sum is read from `+stream_sum.SumsJournal.String(), &runChunker{})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}
