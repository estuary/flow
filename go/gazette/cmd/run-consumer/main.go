package main

import (
	"flag"
	"net"
	"os"
	"path/filepath"
	"plugin"

	etcd "github.com/coreos/etcd/client"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/gazette"
)

var (
	dir             = flag.String("dir", "", "Path into which Shards should be staged")
	etcdEndpoint    = flag.String("etcd", "", "Etcd endpoint")
	gazetteEndpoint = flag.String("gazette", "", "Gazette endpoint")
	name            = flag.String("name", "", "Etcd consumer name")
	standbys        = flag.Int("standbys", 0, "Number of warm stand-bys per shard")
	pluginPath      = flag.String("plugin", "", "Path to consumer plugin")
)

func main() {
	flag.Parse()

	log.WithFields(log.Fields{
		"dir":      *dir,
		"etcd":     *etcdEndpoint,
		"gazette":  *gazetteEndpoint,
		"plugin":   *pluginPath,
		"name":     *name,
		"standbys": *standbys,
	}).Info("using flags")

	var module, err = plugin.Open(*pluginPath)
	if err != nil {
		log.WithFields(log.Fields{"path": os.Args[1], "err": err}).Fatal("failed to open plugin module")
	}
	flag.Parse() // Parse again to initialize any plugin flags.

	var instance consumer.Consumer
	if i, err := module.Lookup("Consumer"); err != nil {
		log.WithField("err", err).Fatal("failed to lookup Consumer symbol")
	} else if c, ok := i.(*consumer.Consumer); !ok {
		log.WithField("instance", i).Fatalf("symbol `Consumer` is not a consumer.Consumer: %#v", i)
	} else {
		instance = *c
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.WithField("err", err).Fatal("failed to lookup network interfaces")
	}

	var routeKey string
	for _, addr := range addrs {
		if ip, ok := addr.(*net.IPNet); ok && !ip.IP.IsLoopback() {
			routeKey = ip.IP.String()
		}
	}
	if routeKey == "" {
		log.WithField("err", err).Fatal("failed to identify a route-able network interface")
	}

	etcdClient, err := etcd.New(etcd.Config{Endpoints: []string{*etcdEndpoint}})
	if err != nil {
		log.WithFields(log.Fields{"err": err, "endpoint": *etcdEndpoint}).
			Fatal("failed to init etcd client")
	}

	gazClient, err := gazette.NewClient(*gazetteEndpoint)
	if err != nil {
		log.WithFields(log.Fields{"err": err, "endpoint": *gazetteEndpoint}).
			Fatal("failed to create Gazette client")
	}

	var writeService = gazette.NewWriteService(gazClient)
	writeService.Start()

	// Flush writes on exit.
	defer writeService.Stop()

	var runner = consumer.Runner{
		Consumer:        instance,
		LocalRouteKey:   routeKey,
		LocalDir:        *dir,
		ConsumerRoot:    filepath.Join("/gazette/consumers/", *name),
		RecoveryLogRoot: filepath.Join("/recovery-logs/", *name)[1:], // Strip leading '/'.
		ReplicaCount:    *standbys,

		Etcd: etcdClient,
		Gazette: struct {
			*gazette.Client
			*gazette.WriteService
		}{gazClient, writeService},
	}

	if err = runner.Run(); err != nil {
		log.WithField("err", err).Error("runner.Run failed")
	}
}
