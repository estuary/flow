package main

import (
	"bytes"
	"errors"
	"flag"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"time"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	"github.com/gorilla/mux"
	"golang.org/x/net/context"
	"golang.org/x/net/trace"
	"google.golang.org/api/gensupport"

	"github.com/pippio/gazette/cloudstore"
	"github.com/pippio/gazette/envflag"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/keepalive"
	"github.com/pippio/varz"
)

var (
	spoolDirectory = flag.String("spoolDir", "/var/tmp/gazette",
		"Local directory for journal spools")

	replicaCount = flag.Int("replicaCount", 2, "Number of required journal replicas")
)

// In order for a brokered Journal to be handed off, it must have regular
// transactions which synchronize all replicas. There's no guarantee that
// clients will perform Journal writes with sufficient frequency, so we issue
// a periodic no-op Append operation to each Journal being brokered locally.
const brokerPulseInterval = 10 * time.Second

func main() {
	var etcdEndpoint = envflag.NewEtcdServiceEndpoint()
	var cloudFSUrl = envflag.NewCloudFSURL()

	envflag.Parse()
	defer varz.Initialize("gazetted").Cleanup()
	gensupport.RegisterHook(traceRequests)

	var localRoute string
	if ip, err := routableIP(); err != nil {
		log.WithField("err", err).Fatal("failed to acquire routable IP")
	} else {
		localRoute = url.QueryEscape("http://" + ip.String() + ":8081")
	}

	log.WithFields(log.Fields{
		"spoolDir":     *spoolDirectory,
		"replicaCount": *replicaCount,
		"etcdEndpoint": *etcdEndpoint,
		"localRoute":   localRoute,
		"releaseTag":   *varz.ReleaseTag,
	}).Info("flag configuration")

	// Fail fast if spool directory cannot be created.
	if err := os.MkdirAll(filepath.Dir(*spoolDirectory), 0700); err != nil {
		log.WithField("err", err).Fatal("failed to create spool directory")
	}

	etcdClient, err := etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *etcdEndpoint}})
	if err != nil {
		log.WithField("err", err).Fatal("failed to init etcd client")
	}
	keysAPI := etcd.NewKeysAPI(etcdClient)

	cfs, err := cloudstore.NewFileSystem(nil, *cloudFSUrl)
	if err != nil {
		log.WithField("err", err).Fatal("failed to initialize cloudstore")
	}
	listener, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.WithField("err", err).Fatal("failed to bind listener")
	}

	persister := gazette.NewPersister(*spoolDirectory, cfs, keysAPI, localRoute)
	persister.StartPersisting()

	for _, fragment := range journal.LocalFragments(*spoolDirectory, "") {
		log.WithField("path", fragment.ContentPath()).Warning("recovering fragment")
		persister.Persist(fragment)
	}

	var router = gazette.NewRouter(
		func(n journal.Name) gazette.JournalReplica {
			return journal.NewReplica(n, *spoolDirectory, persister, cfs)
		},
	)

	// Run regular broker commit "pulses".
	go func() {
		for _ = range time.Tick(brokerPulseInterval) {
			var emptyBuffer bytes.Buffer
			var journals = router.BrokeredJournals()
			var resultCh = make(chan journal.AppendResult, len(journals))

			for _, name := range journals {
				router.Append(journal.AppendOp{
					AppendArgs: journal.AppendArgs{
						Journal: name,
						Content: &emptyBuffer,
					},
					Result: resultCh,
				})
			}
			for _ = range journals {
				<-resultCh
			}
		}
	}()

	var m = mux.NewRouter()
	gazette.NewCreateAPI(keysAPI, *replicaCount).Register(m)
	gazette.NewReadAPI(router, cfs).Register(m)
	gazette.NewReplicateAPI(router).Register(m)
	gazette.NewWriteAPI(router).Register(m)

	go func() {
		err := http.Serve(keepalive.TCPListener{listener.(*net.TCPListener)}, m)

		if _, ok := err.(net.Error); ok {
			return // Don't log on listener.Close.
		}
		log.WithField("err", err).Error("http.Serve failed")
	}()

	var runner = gazette.NewRunner(etcdClient, localRoute, *replicaCount, router)
	if err := runner.Run(); err != nil {
		log.WithField("err", err).Error("runner.Run() failed")
	}
	listener.Close()

	persister.Stop()
	log.Info("service stop complete")
}

// Support for net/trace tracing of package gensupport HTTP requests,
// which are used by the GCS client.
type requestStringer http.Request

func (s *requestStringer) String() string {
	var b, err = httputil.DumpRequest((*http.Request)(s), false)
	if err != nil {
		panic(err) // Shouldn't happen as body isn't written.
	}
	return string(b)
}

type responseStringer http.Response

func (s *responseStringer) String() string {
	var b, err = httputil.DumpResponse((*http.Response)(s), false)
	if err != nil {
		panic(err) // Shouldn't happen as body isn't written.
	}
	return string(b)
}

func traceRequests(ctx context.Context, req *http.Request) func(resp *http.Response) {
	var tr = trace.New("gensupport", req.Method)
	tr.LazyLog((*requestStringer)(req), false)

	return func(resp *http.Response) {
		defer tr.Finish()

		if resp == nil {
			tr.LazyPrintf("<nil Response>")
			tr.SetError()
			return
		}

		tr.LazyLog((*responseStringer)(resp), false)
		if resp.StatusCode >= 400 {
			tr.SetError()
		}
	}
}

// routableIP returns an externally-routable IP address.
func routableIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		if ip, ok := addr.(*net.IPNet); ok && !ip.IP.IsLoopback() {
			return ip.IP, nil
		}
	}
	return nil, errors.New("no non-loopback IP")
}
