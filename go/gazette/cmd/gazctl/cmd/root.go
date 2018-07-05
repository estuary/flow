package cmd

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"plugin"
	"strings"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
	etcd "github.com/coreos/etcd/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/gazette"
	"github.com/LiveRamp/gazette/pkg/recoverylog"
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
			log.WithFields(log.Fields{"path": path, "err": err}).Fatal("failed to open consumer plugin module")
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

func mergePlugin() Merge {
	if lazyMergePlugin == nil {
		var path = viper.GetString("merge.plugin")
		if path == "" {
			return nil
		}

		var module, err = plugin.Open(path)
		if err != nil {
			log.WithFields(log.Fields{"path": path, "err": err}).Fatal("failed to open merge plugin module")
		}

		if i, err := module.Lookup("Merge"); err != nil {
			log.WithField("err", err).Fatal("failed to lookup Merge symbol")
		} else if merge, ok := i.(*Merge); !ok {
			log.WithField("instance", i).Fatalf("symbol `Merge` is not a plugin_types.Merge: %#v", i)
		} else {
			lazyMergePlugin = *merge
		}
	}
	return lazyMergePlugin
}

func writeService() *gazette.WriteService {
	if lazyWriteService == nil {
		lazyWriteService = gazette.NewWriteService(gazetteClient())
		lazyWriteService.Start()
	}
	return lazyWriteService
}

func cloudFS() cloudstore.FileSystem {
	if lazyCFS == nil {
		var cfs, err = cloudstore.NewFileSystem(nil, viper.GetString("cloud.fs.url"))
		if err != nil {
			log.WithField("err", err).Fatal("cannot initialize cloud filesystem")
		}
		lazyCFS = cfs
	}
	return lazyCFS
}

func userConfirms(message string) {
	if defaultYes {
		return
	}
	fmt.Println(message)
	fmt.Print("Confirm (y/N): ")

	var response string
	fmt.Scanln(&response)

	for _, opt := range []string{"y", "yes"} {
		if strings.ToLower(response) == opt {
			return
		}
	}
	log.Fatal("aborted by user")
}

// loadHints loads FSMHints given a locator, which can take the form of a simple path
// to a file on disk, or an "etcd:///path/to/key".
func loadHints(locator string) recoverylog.FSMHints {
	var u, err = url.Parse(locator)
	switch {
	case err != nil:
		log.WithField("err", err).Fatal("failed to parse URL")
	case u.Host != "":
		log.WithField("host", u.Host).Fatal("url.Host should be empty (use `etcd:///path/to/key` syntax)")
	case u.Scheme != "" && u.Scheme != "etcd":
		log.WithField("scheme", u.Scheme).Fatal("url.Scheme must be empty or `etcd://`")
	case u.RawQuery != "":
		log.WithField("query", u.RawQuery).Fatal("url.Query must be empty")
	}

	var content []byte

	if u.Scheme == "etcd" {
		var r, err = etcd.NewKeysAPI(etcdClient()).Get(context.Background(), u.Path, nil)
		if err != nil {
			log.WithField("err", err).Fatal("failed to read hints from Etcd")
		}
		content = []byte(r.Node.Value)
	} else if content, err = ioutil.ReadFile(u.Path); err != nil {
		log.WithFields(log.Fields{"err": err, "path": u.Path}).Fatal("failed to read hints file")
	}

	var hints recoverylog.FSMHints
	if err = json.Unmarshal(content, &hints); err != nil {
		log.WithFields(log.Fields{"err": err, "hints": string(content)}).Fatal("failed to unmarshal hints")
	}
	return hints
}

var (
	lazyCFS            cloudstore.FileSystem
	lazyConsumerPlugin consumer.Consumer
	lazyMergePlugin    Merge
	lazyEtcdClient     etcd.Client
	lazyGazetteClient  *gazette.Client
	lazyWriteService   *gazette.WriteService

	defaultYes bool
)
