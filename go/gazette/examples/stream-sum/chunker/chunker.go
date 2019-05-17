package main

import (
	"context"
	"net/http"

	"github.com/gazette/gazette/v2/examples/stream-sum"
	mbp "github.com/gazette/gazette/v2/mainboilerplate"
	"github.com/gazette/gazette/v2/metrics"
	pb "github.com/gazette/gazette/v2/protocol"
	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const iniFilename = "chunker.ini"

var cfg = new(stream_sum.ChunkerConfig)

type cmdRun struct{}

func (cmdRun) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cfg.Diagnostics)()
	mbp.InitLog(cfg.Log)
	pb.RegisterGRPCDispatcher(cfg.Chunker.Zone)
	go func() { mbp.Must(http.ListenAndServe(":8080", nil), "serving diagnostics") }()

	log.WithField("config", cfg).Info("starting chunker")
	prometheus.MustRegister(metrics.GazetteClientCollectors()...)

	mbp.Must(stream_sum.GenerateAndVerifyStreams(context.Background(), cfg), "chunker failed")
	return nil
}

func main() {
	var parser = flags.NewParser(cfg, flags.Default)

	_, _ = parser.AddCommand("run", "Run chunker", `
run chunker with the provided configuration. chunker will emit streams
of pseudo-random stream chunks. Upon completing each stream, it will confirm that
the expected sum is read from `+stream_sum.FinalSumsJournal.String(), &cmdRun{})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}
