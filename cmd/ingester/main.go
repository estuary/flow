package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

const iniFilename = "flow.ini"

// Config is the top-level configuration object of a Flow ingester.
var Config = new(struct {
	Ingest struct {
		mbp.ServiceConfig
		Catalog pb.Endpoint `long:"catalog" required:"true" description:"Catalog URL"`
	} `group:"Ingest" namespace:"ingest" env-namespace:"INGEST"`

	Etcd struct {
		mbp.EtcdConfig
		JournalsPrefix string `long:"journals" env:"JOURNALS" default:"/gazette/cluster/items" description:"Etcd base prefix for broker journals"`
	} `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`

	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
})

type cmdServe struct{}

type publisherAndClock struct {
	*message.Clock
	*message.Publisher
}

func (cmdServe) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(Config.Diagnostics)()
	mbp.InitLog(Config.Log)

	log.WithFields(log.Fields{
		"config":    Config,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("ingester configuration")
	pb.RegisterGRPCDispatcher(Config.Ingest.Zone)

	catalog, err := flow.NewCatalog(Config.Ingest.Catalog.URL().String(), os.TempDir())
	mbp.Must(err, "opening catalog")
	collections, err := catalog.LoadCapturedCollections()
	mbp.Must(err, "loading captured collection specifications")

	// Bind our server listener, grabbing a random available port if Port is zero.
	srv, err := server.New("", Config.Ingest.Port)
	mbp.Must(err, "building Server instance")

	if Config.Broker.Cache.Size <= 0 {
		log.Warn("--broker.cache.size is disabled; consider setting > 0")
	}

	var (
		etcd     = Config.Etcd.MustDial()
		rjc      = Config.Broker.MustRoutedJournalClient(context.Background())
		ajc      = client.NewAppendService(context.Background(), rjc)
		pub      = message.NewPublisher(ajc, nil)
		tasks    = task.NewGroup(context.Background())
		signalCh = make(chan os.Signal, 1)
		mu       sync.Mutex
	)

	journals, err := flow.NewJournalsKeySpace(context.Background(), etcd, Config.Etcd.JournalsPrefix)
	mbp.Must(err, "failed to load Gazette journals")
	delegate, err := flow.NewWorkerHost("combine", "--catalog", catalog.LocalPath())
	mbp.Must(err, "failed to start flow-worker")

	var mapper = flow.Mapper{
		Ctx:           tasks.Context(),
		JournalClient: rjc,
		Journals:      journals,
	}
	var index = make(map[pf.Collection]*pf.CollectionSpec)
	for c := range collections {
		index[collections[c].Name] = &collections[c]
	}

	srv.HTTPMux.HandleFunc("/ingest", func(w http.ResponseWriter, req *http.Request) {
		if ct := req.Header.Get("Content-Type"); ct != "application/json" {
			http.Error(w,
				fmt.Sprintf("unsupported Content-Type %q (expected 'application/json'')", ct), http.StatusBadRequest)
			return
		}

		var err error
		var body map[pf.Collection][]json.RawMessage
		var rpcs []*flow.Combine

		if err = json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for collection, docs := range body {
			var spec, ok = index[collection]
			if !ok {
				http.Error(w, fmt.Sprintf("%q is not a captured collection", collection), http.StatusBadRequest)
				return
			}

			var rpc, err = flow.NewCombine(req.Context(), delegate.Conn, spec)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			for _, doc := range docs {
				if err := rpc.Add(doc); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
			if err = rpc.Flush(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			rpcs = append(rpcs, rpc)
		}

		var journalAppends = make(map[pb.Journal]*client.AsyncAppend)

		mu.Lock()
		for _, rpc := range rpcs {
			if err = rpc.Finish(func(icr pf.IndexedCombineResponse) error {
				if aa, err := pub.PublishCommitted(mapper.Map, icr); err != nil {
					return err
				} else {
					journalAppends[aa.Request().Journal] = aa
					return nil
				}
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				mu.Unlock()
				return
			}
		}
		mu.Unlock()

		// Collect and marshal append offsets in the response.
		var offsets = make(pb.Offsets, len(journalAppends))
		for journal, aa := range journalAppends {
			offsets[journal] = aa.Request().Offset
		}
		_ = json.NewEncoder(w).Encode(offsets)
	})

	srv.QueueTasks(tasks)

	// Install signal handler & start broker tasks.
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	tasks.Queue("watch signalCh", func() error {
		select {
		case sig := <-signalCh:
			log.WithField("signal", sig).Info("caught signal")
			tasks.Cancel()
			return nil
		case <-tasks.Context().Done():
			return nil
		}
	})
	tasks.GoRun()

	return nil
}

func main() {
	var parser = flags.NewParser(Config, flags.Default)

	_, _ = parser.AddCommand("serve", "Serve as Flow ingester", `
Serve a Flow ingester with the provided configuration, until signaled to
exit (via SIGTERM).
`, &cmdServe{})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}
