package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"plugin"
	"strings"

	etcd "github.com/coreos/etcd/client"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/gazette"
	"github.com/LiveRamp/gazette/pkg/metrics"
)

var configFile = flag.String("config", "", "Path to configuration file. "+
	"Defaults to `consumer-config.{toml|yaml|json}` in the current working directory.")

type Config struct {
	Service struct {
		AllocatorRoot   string // Absolute path in Etcd of the service consensus.Allocator.
		LocalRouteKey   string // Unique key of this consumer instance. By convention, this is bound "host:port" address.
		Plugin          string // Path of consumer plugin to load & run.
		RecoveryLogRoot string // Path prefix for the consumer's recovery-log Journals.
		ShardStandbys   uint8  // Number of warm-standby replicas to allocate for each Consumer shard.
		Workdir         string // Local directory for ephemeral serving files.
	}
	Etcd    struct{ Endpoint string } // Etcd endpoint to use.
	Gazette struct{ Endpoint string } // Gazette endpoint to use.
}

func (cfg Config) Validate() error {
	if !path.IsAbs(cfg.Service.AllocatorRoot) {
		return fmt.Errorf("Service.AllocatorRoot not an absolute path: %s", cfg.Service.AllocatorRoot)
	} else if cfg.Service.RecoveryLogRoot == "" {
		return fmt.Errorf("Service.RecoveryLogRoot not specified")
	} else if cfg.Service.Workdir == "" {
		return fmt.Errorf("Service.Workdir not specified")
	} else if cfg.Service.LocalRouteKey == "" {
		return fmt.Errorf("Service.LocalRouteKey not specified")
	} else if cfg.Etcd.Endpoint == "" {
		return fmt.Errorf("Etcd.Endpoint not specified")
	} else if cfg.Gazette.Endpoint == "" {
		return fmt.Errorf("Gazette.Endpoint not specified")
	}
	return nil
}

func main() {
	prometheus.MustRegister(metrics.GazetteClientCollectors()...)
	prometheus.MustRegister(metrics.GazetteConsumerCollectors()...)
	flag.Parse()

	if *configFile != "" {
		viper.SetConfigFile(*configFile)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName("consumer-config")
	}

	if err := viper.ReadInConfig(); err != nil {
		log.WithField("err", err).Fatal("failed to read config")
	} else {
		log.WithField("path", viper.ConfigFileUsed()).Info("read config")
	}

	// Allow environment variables to override file configuration.
	// Treat variable underscores as nested-path specifiers.
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		log.WithField("err", err).Fatal("failed to unmarshal")
	} else if err := config.Validate(); err != nil {
		viper.Debug()
		log.WithFields(log.Fields{"err": err, "cfg": config, "env": os.Environ()}).Fatal("config validation failed")
	}

	var module, err = plugin.Open(config.Service.Plugin)
	if err != nil {
		log.WithFields(log.Fields{"path": config.Service.Plugin, "err": err}).Fatal("failed to open plugin module")
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

	etcdClient, err := etcd.New(etcd.Config{Endpoints: []string{config.Etcd.Endpoint}})
	if err != nil {
		log.WithField("err", err).Fatal("failed to init etcd client")
	}
	gazClient, err := gazette.NewClient(config.Gazette.Endpoint)
	if err != nil {
		log.WithField("err", err).Fatal("failed to init gazette client")
	}

	var writeService = gazette.NewWriteService(gazClient)
	writeService.Start()
	defer writeService.Stop() // Flush writes on exit.

	var runner = &consumer.Runner{
		Consumer:        instance,
		ConsumerRoot:    config.Service.AllocatorRoot,
		LocalDir:        config.Service.Workdir,
		LocalRouteKey:   config.Service.LocalRouteKey,
		RecoveryLogRoot: config.Service.RecoveryLogRoot,
		ReplicaCount:    int(config.Service.ShardStandbys),

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
