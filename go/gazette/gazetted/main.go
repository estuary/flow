package main

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/api-server/discovery"
	"github.com/pippio/api-server/gazette"
	"github.com/pippio/api-server/varz"
)

var (
	// Service discovery.
	etcdEndpoint = flag.String("etcdEndpoint", "http://127.0.0.1:2379",
		"Etcd network service address")
	announceEndpoint = flag.String("announceEndpoint", "http://127.0.0.1:8081",
		"Endpoint to announce")

	spoolDirectory = flag.String("spoolDir", "/var/tmp/gazette",
		"Local directory for journal spools")
	replicaCount = flag.Int("replicaCount", 3, "Number of replicas")

	// Informational.
	releaseTag = flag.String("tag", "<none>", "Release tag")
	replica    = flag.String("replica", "<none>", "Replica number")
)

func main() {
	flag.Parse()

	log.SetFormatter(&log.TextFormatter{DisableTimestamp: true})
	log.AddHook(&varz.LogThrottleHook{})
	log.AddHook(&varz.LogCallerHook{})
	log.AddHook(&varz.LogServiceHook{
		Service: "gazetted", Tag: *releaseTag, Replica: *replica})
	varz.StartDebugListener()

	log.WithFields(log.Fields{
		"spoolDir":         *spoolDirectory,
		"replicaCount":     *replicaCount,
		"etcdEndpoint":     *etcdEndpoint,
		"announceEndpoint": *announceEndpoint,
		"releaseTag":       *releaseTag,
		"replica":          *replica,
	}).Info("flag configuration")

	// Fail fast if spool directory cannot be created.
	if err := os.MkdirAll(filepath.Dir(*spoolDirectory), 0700); err != nil {
		log.WithField("err", err).Fatal("failed to create spool directory")
	}

	etcdService, err := discovery.NewEtcdClientService(*etcdEndpoint)
	if err != nil {
		log.WithField("err", err).Fatal("failed to initialize etcdService")
	}
	var context = gazette.ServiceContext{
		Directory:    *spoolDirectory,
		Etcd:         etcdService,
		ReplicaCount: *replicaCount,
		ServiceURL:   *announceEndpoint,
	}

	properties, err := discovery.NewProperties("/properties", context.Etcd)
	if err != nil {
		log.WithField("err", err).Fatal("failed to initialize etcd /properties")
	}
	context.CloudFileSystem, err = cloudstore.DefaultFileSystem(properties)
	if err != nil {
		log.WithField("err", err).Fatal("failed to initialize cloudstore")
	}

	if hostname, err := os.Hostname(); err != nil {
		log.WithField("err", err).Fatal("failed to get hostname")
	} else {
		context.RouteKey = hostname
	}

	if err = context.Start(); err != nil {
		log.WithField("err", err).Fatal("failed to start gazetted")
	}

	// Install a signal handler to Stop() on external signal.
	interrupt := make(chan os.Signal, 1)
	done := make(chan struct{}, 1)
	signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig, ok := <-interrupt
		if ok {
			log.WithField("signal", sig).Info("caught signal")
			context.Stop()
			// Once the consumer is cleaned up, just write to done to allow the
			// below for loop to complete and return from main, this kills all
			// running goroutines.
			done <- struct{}{}
		}
	}()

	go varz.PeriodicallyLogVarz()
	go http.ListenAndServe(":8081", context.BuildServingMux())

	<-done

	log.Info("service stop complete")
}
