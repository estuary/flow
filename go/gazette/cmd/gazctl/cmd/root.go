package cmd

import (
	"flag"
	"fmt"
	"os"
	"plugin"
	"strings"

	etcd "github.com/coreos/etcd/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/gazette"
)

var configFile string

// Execute evaluates provided arguments against the rootCmd hierarchy.
// This is called by main.main().
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	if lazyWriteService != nil {
		lazyWriteService.Stop()
	}
}

// rootCmd parents all other commands in the hierarchy.
var rootCmd = &cobra.Command{
	Use:   "gazctl",
	Short: "gazctl is a command-line interface for interacting with gazette",
}

func init() {
	log.SetOutput(os.Stderr)

	cobra.OnInitialize(initConfig)
	flag.Parse()

	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "f", "",
		"config file (default is $HOME/.gazctl.yaml)")

}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if configFile != "" {
		viper.SetConfigFile(configFile)
	}

	viper.SetConfigName(".gazctl")
	viper.AddConfigPath("$HOME")

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.WithField("file", viper.ConfigFileUsed()).Info("read config")
	}

	// Allow environment variables to override file configuration.
	// Treat variable underscores as nested-path specifiers.
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}

func etcdClient() etcd.Client {
	if lazyEtcdClient == nil {
		var ep = viper.GetString("etcd.endpoint")
		if ep == "" {
			log.Fatal("etcd.endpoint not provided")
		}

		var err error
		lazyEtcdClient, err = etcd.New(etcd.Config{Endpoints: []string{ep}})
		if err != nil {
			log.WithField("err", err).Fatal("building etcd client")
		}
	}
	return lazyEtcdClient
}

func gazetteClient() *gazette.Client {
	if lazyGazetteClient == nil {
		var ep = viper.GetString("gazette.endpoint")
		if ep == "" {
			log.Fatal("gazette.endpoint not provided")
		}

		var err error
		lazyGazetteClient, err = gazette.NewClient(ep)
		if err != nil {
			log.WithField("err", err).Fatal("building gazette client")
		}
	}
	return lazyGazetteClient
}

func consumerPlugin() consumer.Consumer {
	if lazyConsumerPlugin == nil {
		var path = viper.GetString("consumer.plugin")
		if path == "" {
			return nil
		}

		var module, err = plugin.Open(path)
		if err != nil {
			log.WithFields(log.Fields{"path": path, "err": err}).Fatal("failed to open plugin module")
		}

		if i, err := module.Lookup("Consumer"); err != nil {
			log.WithField("err", err).Fatal("failed to lookup Consumer symbol")
		} else if c, ok := i.(*consumer.Consumer); !ok {
			log.WithField("instance", i).Fatalf("symbol `Consumer` is not a consumer.Consumer: %#v", i)
		} else {
			lazyConsumerPlugin = *c
		}
	}
	return lazyConsumerPlugin
}

func writeService() *gazette.WriteService {
	if lazyWriteService == nil {
		lazyWriteService = gazette.NewWriteService(gazetteClient())
		lazyWriteService.Start()
	}
	return lazyWriteService
}

var (
	lazyGazetteClient  *gazette.Client
	lazyEtcdClient     etcd.Client
	lazyWriteService   *gazette.WriteService
	lazyConsumerPlugin consumer.Consumer
)
