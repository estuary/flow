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
		Catalog string `long:"catalog" required:"true" description:"Catalog URL or local path"`
	} `group:"Ingest" namespace:"ingest" env-namespace:"INGEST"`

	Flow struct {
		BrokerRoot string `long:"broker-root" env:"BROKER_ROOT" default:"/gazette/cluster" description:"Broker Etcd base prefix"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`

	Etcd        mbp.EtcdConfig        `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`
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

	catalog, err := flow.NewCatalog(Config.Ingest.Catalog, os.TempDir())
	mbp.Must(err, "opening catalog")
	collections, err := catalog.LoadCapturedCollections()
	mbp.Must(err, "loading captured collection specifications")

	for _, collection := range collections {
		log.WithField("name", collection.Name).Info("serving captured collection")
	}
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

	journals, err := flow.NewJournalsKeySpace(context.Background(), etcd, Config.Flow.BrokerRoot)
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
			if err == nil {
				err = rpc.Open(flow.FieldPointersForMapper(spec))
			}
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

		// Block on each append, collect write offsets, and marshal into the response.
		var offsets = make(pb.Offsets, len(journalAppends))
		for journal, aa := range journalAppends {
			if err := aa.Err(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			offsets[journal] = aa.Response().Commit.End
		}
		w.Header().Add("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(offsets)
	})

	srv.QueueTasks(tasks)

	tasks.Queue("journals.Watch", func() error {
		if err := journals.Watch(tasks.Context(), etcd); err != context.Canceled {
			return err
		}
		return nil
	})

	// Install signal handler & start broker tasks.
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	tasks.Queue("watch signalCh", func() error {
		select {
		case sig := <-signalCh:
			log.WithField("signal", sig).Info("caught signal")

			tasks.Cancel()
			srv.BoundedGracefulStop()
			return delegate.Stop()

		case <-tasks.Context().Done():
			return nil
		}
	})
	tasks.GoRun()

	// Block until all tasks complete. Assert none returned an error.
	mbp.Must(tasks.Wait(), "ingester task failed")
	log.Info("goodbye")

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
