package main

import (
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/api-server/discovery"
	"github.com/pippio/api-server/endpoints"
	"github.com/pippio/api-server/varz"
	"github.com/pippio/gazette/gazette"
)

var (
	// Service discovery.
	announceEndpoint = flag.String("announceEndpoint", "",
		"Endpoint to announce")

	spoolDirectory = flag.String("spoolDir", "/var/tmp/gazette",
		"Local directory for journal spools")
	replicaCount = flag.Int("replicaCount", 3, "Number of replicas")

	// Informational.
	releaseTag = flag.String("tag", "<none>", "Release tag")
	replica    = flag.String("replica", "<none>", "Replica number")
)

func main() {
	endpoints.ParseFromEnvironment()
	flag.Parse()

	log.SetFormatter(&log.TextFormatter{DisableTimestamp: true})
	log.AddHook(&varz.LogThrottleHook{})
	log.AddHook(&varz.LogCallerHook{})
	log.AddHook(&varz.LogServiceHook{
		Service: "gazetted", Tag: *releaseTag, Replica: *replica})
	varz.StartDebugListener()

	if *announceEndpoint == "" {
		// Infer from externally routable IP address.
		if addrs, err := net.InterfaceAddrs(); err != nil {
			log.WithField("err", err).Fatal(
				"error retrieving interfaces and no announce endpoint specified")
		} else {
			for i := range addrs {
				if ip, ok := addrs[i].(*net.IPNet); ok && !ip.IP.IsLoopback() {
					log.WithField("address", ip.IP.String()).Info(
						"selected an announce endpoint")
					*announceEndpoint = "http://" + ip.IP.String() + ":8081"
					break
				}
			}
		}
	}

	log.WithFields(log.Fields{
		"spoolDir":         *spoolDirectory,
		"replicaCount":     *replicaCount,
		"etcdEndpoint":     *endpoints.EtcdEndpoint,
		"announceEndpoint": *announceEndpoint,
		"releaseTag":       *releaseTag,
		"replica":          *replica,
	}).Info("flag configuration")

	// Fail fast if spool directory cannot be created.
	if err := os.MkdirAll(filepath.Dir(*spoolDirectory), 0700); err != nil {
		log.WithField("err", err).Fatal("failed to create spool directory")
	}

	etcdService, err := discovery.NewEtcdClientService(*endpoints.EtcdEndpoint)
	if err != nil {
		log.WithField("err", err).Fatal("failed to initialize etcdService")
	}
	var context = gazette.Context{
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
	go func() {
		err := http.ListenAndServe(":8081", context.BuildServingMux())
		log.WithField("err", err).Error("failed to listen")
	}()

	<-done

	log.Info("service stop complete")
}
