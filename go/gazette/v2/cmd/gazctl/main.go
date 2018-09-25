package main

import (
	"errors"
	"io/ioutil"
	"os"

	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/jessevdk/go-flags"
	"gopkg.in/yaml.v2"
)

var (
	baseCfg = new(struct {
		mbp.ZoneConfig
		Log mbp.LogConfig `group:"Logging" namespace:"log" env-namespace:"LOG"`
	})
	journalsCfg = new(struct {
		Broker mbp.AddressConfig `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	})
	shardsCfg = new(struct {
		Consumer mbp.AddressConfig `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	})
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
	SpecsPath string `long:"specs" description:"Path to specifications file to apply. Stdin is used if not set"`
	DryRun    bool   `long:"dry-run" description:"Perform a dry-run of the apply"`
}

func (cfg ApplyConfig) decode(into interface{}) error {
	var buffer []byte
	var err error

	if cfg.SpecsPath != "" {
		buffer, err = ioutil.ReadFile(cfg.SpecsPath)
	} else {
		buffer, err = ioutil.ReadAll(os.Stdin)
	}
	mbp.Must(err, "failed to read YAML input")

	if err = yaml.UnmarshalStrict(buffer, into); err != nil {
		// `yaml` produces nicely formatted error messages that are best printed as-is.
		os.Stderr.WriteString(err.Error() + "\n")
		return errors.New("YAML decode failed")
	}
	return nil
}

func startup() {
	mbp.InitLog(baseCfg.Log)
	protocol.RegisterGRPCDispatcher(baseCfg.Zone)
}

func main() {
	var parser = flags.NewParser(baseCfg, flags.Default)

	var addCmd = func(cmd *flags.Command, name, short, long string, cfg interface{}) *flags.Command {
		cmd, err := cmd.AddCommand(name, short, long, cfg)
		mbp.Must(err, "failed to add command")
		return cmd
	}

	var cmdJournals = addCmd(parser.Command, "journals", "Interact with broker journals", "", journalsCfg)
	var cmdShards = addCmd(parser.Command, "shards", "Interact with consumer shards", "", shardsCfg)

	_ = addCmd(cmdJournals, "list", "List journals", `
List journal specifications and status.

Use --selector to supply a LabelSelector which constrains the set of returned
journals. Journal selectors support additional meta-labels "name" and "prefix".

Match JournalSpecs having an exact name:
>    --selector "name in (foo/bar, baz/bing)"

Match JournalSpecs having a name prefix (must end in '/'):
>    --selector "prefix = my/prefix/"

Results can be output in a variety of --format options:
yaml:  Prints a YAML journal hierarchy, compatible with "journals apply"
json:  Prints JournalSpecs encoded as JSON
proto: Prints JournalSpecs encoded in protobuf text format
table: Prints as a table (see other flags for column choices)

When output as a journal hierarchy, gazctl will "hoist" the returned collection
of JournalSpecs into a hierarchy of journals having common prefixes and,
typically, common configuration. This hierarchy is simply sugar for and is
exactly equivalent to the original JournalSpecs.
`, &cmdJournalsList{})

	_ = addCmd(cmdShards, "list", "List shards", `
List shard specifications and status.

Use --selector to supply a LabelSelector which constrains the set of returned
shards. Shard selectors support an additional meta-label "id".

Match ShardSpecs having a specific ID:
>    --selector "id in (shard-12, shard-34)"

Results can be output in a variety of --format options:
yaml:  Prints shards in YAML form, compatible with "shards apply"
json:  Prints ShardSpecs encoded as JSON
proto: Prints ShardSpecs encoded in protobuf text format
table: Prints as a table (see other flags for column choices)
`, &cmdShardsList{})

	_ = addCmd(cmdJournals, "apply", "Apply journal specifications", `
Apply a collection of JournalSpec creations, updates, or deletions.

JournalSpecs should be provided as a YAML journal hierarchy, the format
produced by "gazctl journals list". This YAML hierarchy format is sugar for
succinctly representing a collection of JournalSpecs, which typically exhibit
common prefixes and configuration. gazctl will flatten the YAML hierarchy
into the implicated collection of JournalSpec changes, and send each to the
brokers for application.

Brokers verify that the etcd "revision" field of each JournalSpec is correct,
and will fail the entire apply operation if any have since been updated. A
common operational pattern is to list, edit, and re-apply a collection of
JournalSpecs; this check ensures concurrent modifications are caught.

JournalSpecs may be created by setting "revision" to zero or omitting altogether.

JournalSpecs may be deleted by setting field "delete" to true on individual
journals or parents thereof in the hierarchy. Note that deleted parent prefixes
will cascade only to JournalSpecs *explicitly listed* as children of the prefix
in the YAML, and not to other JournalSpecs which may exist with the prefix but
are not enumerated.
`, &cmdJournalsApply{})

	_ = addCmd(cmdShards, "apply", "Apply shard specifications", `
Apply a collection of ShardSpec creations, updates, or deletions.

ShardSpecs should be provided as a YAML list, the same format produced by
"gazctl shards list". Consumers verify that the etcd "revision" field of each
ShardSpec is correct, and will fail the entire apply operation if any have since
been updated. A common operational pattern is to list, edit, and re-apply a
collection of ShardSpecs; this check ensures concurrent modifications are caught.

ShardSpecs may be created by setting "revision" to zero or omitting it altogether.

ShardSpecs may be deleted by setting their field "delete" to true.
`, &cmdShardsApply{})

	mbp.MustParseConfig(parser, iniFilename)
}

const iniFilename = "gazctl.ini"
