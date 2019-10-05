package main

import (
	"errors"
	"io/ioutil"
	"os"

	"github.com/jessevdk/go-flags"
	"go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v2"
)

const (
	iniFilename = "gazctl.ini"

	// maxTxnSizeWarning is a warning regarding the use of --max-txn-size that
	// is shared across several commands
	maxTxnSizeWarning = `
In the event that this command generates more changes than are possible in a
single Etcd transaction given the current server configuration (default 128),
gazctl supports a flag which will send changes in batches of at most
--max-txn-size. However, this means the entire apply is no longer issued as
a single Etcd transaction and it should therefore be used with caution.
If possible, prefer to use label selectors to limit the number of changes.`
)

var (
	baseCfg = new(struct {
		mbp.ZoneConfig
		Log mbp.LogConfig `group:"Logging" namespace:"log" env-namespace:"LOG"`
	})
	journalsCfg = new(struct {
		Broker mbp.ClientConfig `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	})
	shardsCfg = new(struct {
		Consumer mbp.ClientConfig `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
		Broker   mbp.ClientConfig `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	})

	parser = flags.NewParser(baseCfg, flags.Default)

	// Subcommands that exist solely to contain and organize further nested
	// subcommands; i.e., they do nothing when executed. They must be
	// initialized here so they exist prior to any init() functions being
	// called to add nested subcommands.
	cmdJournals = mustAddCmd(parser.Command, "journals", "Interact with broker journals", "", journalsCfg)
	cmdShards   = mustAddCmd(parser.Command, "shards", "Interact with consumer shards", "", shardsCfg)
)

// ListConfig is common configuration of list operations.
type ListConfig struct {
	Selector string   `long:"selector" short:"l" description:"Label Selector query to filter on"`
	Format   string   `long:"format" short:"o" choice:"table" choice:"yaml" choice:"json" choice:"proto" default:"table" description:"Output format"`
	Labels   []string `long:"label-columns" short:"L" description:"Labels to present as columns, eg -L label-one -L label-two"`
	Primary  bool     `long:"primary" short:"p" description:"Show primary column"`
	Replicas bool     `long:"replicas" short:"r" description:"Show replicas column"`
	RF       bool     `long:"rf" description:"Show replication factor column"`
}

// ApplyConfig is common configuration of apply operations.
type ApplyConfig struct {
	SpecsPath  string `long:"specs" default:"-" description:"Input specifications path to apply. Use '-' for stdin"`
	DryRun     bool   `long:"dry-run" description:"Perform a dry-run of the apply"`
	MaxTxnSize int    `long:"max-txn-size" default:"0" description:"maximum number of specs to be processed within an apply transaction. If 0, the default, all changes are issued in a single transaction"`
}

// EditConfig is common configuration for exit operations.
type EditConfig struct {
	Selector   string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on" no-ini:"true"`
	MaxTxnSize int    `long:"max-txn-size" default:"0" description:"maximum number of specs to be processed within an apply transaction. If 0, the default, all changes are issued in a single transaction"`
}

type pruneConfig struct {
	Selector string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on"`
	DryRun   bool   `long:"dry-run" description:"Perform a dry-run of the apply"`
}

func (cfg ApplyConfig) decode(into interface{}) error {
	var buffer []byte
	var err error

	if cfg.SpecsPath == "-" {
		buffer, err = ioutil.ReadAll(os.Stdin)
	} else {
		buffer, err = ioutil.ReadFile(cfg.SpecsPath)
	}
	mbp.Must(err, "failed to read YAML input")

	if err = yaml.UnmarshalStrict(buffer, into); err != nil {
		// `yaml` produces nicely formatted error messages that are best printed as-is.
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		return errors.New("YAML decode failed")
	}
	return nil
}

func startup() {
	mbp.InitLog(baseCfg.Log)
	protocol.RegisterGRPCDispatcher(baseCfg.Zone)
}

func mustAddCmd(cmd *flags.Command, name, short, long string, cfg interface{}) *flags.Command {
	cmd, err := cmd.AddCommand(name, short, long, cfg)
	mbp.Must(err, "failed to add command")
	return cmd
}

func init() {
	mbp.AddPrintConfigCmd(parser, iniFilename)
}

func main() {
	mbp.MustParseConfig(parser, iniFilename)
}
