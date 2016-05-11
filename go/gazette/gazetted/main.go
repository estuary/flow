package main

import (
	"bytes"
	"flag"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	"github.com/gorilla/mux"
	"golang.org/x/net/context"

	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/api-server/endpoints"
	"github.com/pippio/api-server/varz"
	"github.com/pippio/consensus"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/keepalive"
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
	varz.Initialize("gazetted")

	var localRoute string
	if ip, err := endpoints.RoutableIP(); err != nil {
		log.WithField("err", err).Fatal("failed to acquire routable IP")
	} else {
		localRoute = url.QueryEscape("http://" + ip.String() + ":8081")
	}

	log.WithFields(log.Fields{
		"spoolDir":     *spoolDirectory,
		"replicaCount": *replicaCount,
		"etcdEndpoint": *endpoints.EtcdEndpoint,
		"localRoute":   localRoute,
		"releaseTag":   *varz.ReleaseTag,
	}).Info("flag configuration")

	// Fail fast if spool directory cannot be created.
	if err := os.MkdirAll(filepath.Dir(*spoolDirectory), 0700); err != nil {
		log.WithField("err", err).Fatal("failed to create spool directory")
	}

	etcdClient, err := etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *endpoints.EtcdEndpoint}})
	if err != nil {
		log.WithField("err", err).Fatal("failed to init etcd client")
	}
	keysAPI := etcd.NewKeysAPI(etcdClient)

	properties, err := keysAPI.Get(context.Background(), "/properties",
		&etcd.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		log.WithField("err", err).Fatal("failed to initialize etcd /properties")
	}
	cfs, err := cloudstore.DefaultFileSystem(consensus.MapAdapter(properties.Node))
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

	var runner = gazette.Runner{
		Etcd:          etcdClient,
		LocalRouteKey: localRoute,
		ReplicaCount:  *replicaCount,
		Router:        router,
	}

	m := mux.NewRouter()
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

	if err := runner.Run(); err != nil {
		log.WithField("err", err).Error("runner.Run() failed")
	}
	listener.Close()

	persister.Stop()
	log.Info("service stop complete")
}
