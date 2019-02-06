package main

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os"
	"text/template"

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
		Broker mbp.ClientConfig `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	})
	shardsCfg = new(struct {
		Consumer mbp.AddressConfig `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	})

	journalsEditLongDesc, shardsEditLongDesc string
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

type EditConfig struct {
	Selector string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on" no-ini:"true"`
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
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
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

	mbp.AddPrintConfigCmd(parser, "gazctl.ini")
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

	_ = addCmd(cmdJournals, "read", "Read journal contents", `
Read the contents journal or journals as a stream.

Use --selector to supply a LabelSelector which constrains the set of journals
to be read from.

Match JournalSpecs having an exact name:
>    --selector "name in (foo/bar, baz/bing)"

Match JournalSpecs having a name prefix (must end in '/'):
>    --selector "prefix = my/prefix/"

Read can run in a blocking fashion with --blocking which will not exit when 
it has reached the head of the current journal(s). When new data becomes available 
it will be sent to Stdout.

To read from an arbitrary offset into a journal(s) use the --offset flag. 
If not passed the default value is -1 which will read from the head of the journal.
`, &cmdJournalRead{})

	_ = addCmd(cmdJournals, "edit", "Edit journal specifications", journalsEditLongDesc, &cmdJournalsEdit{})
	_ = addCmd(cmdShards, "edit", "Edit shard specifications", shardsEditLongDesc, &cmdShardsEdit{})
	_ = addCmd(cmdShards, "prune", "Removes fragments of a hinted recovery log which are no longer needed", `
Recovery logs capture every write which has ever occurred in a Shard DB.
This includes all prior writes of client keys & values, and also RocksDB
compactions, which can significantly inflate the total volume of writes
relative to the data currently represented in a RocksDB.

Prune log examines the provided hints to identify Fragments of the log
which have no intersection with any live files of the DB, and can thus
be safely deleted.
`, &cmdShardsPrune{})

	_ = addCmd(cmdJournals, "reset-head", "Reset journal append offset (disaster recovery)", `
Reset the append offset of journals.

Gazette appends are transactional: all brokers must agree on the exact offsets
at which an append operation will be written into a journal. The offset is an
explicit participate in the broker's transaction protocol. New participants are
"caught up" on the current offset by participating in broker transactions, and
brokers will delay releasing responsibility for a journal until all peers have
participated in a synchronizing transaction. This makes Gazette tolerant to up
to R-1 independent broker process failures, where R is the replication factor
of the journal.

However, disasters and human errors do happen, and if R or more independent
failures occur, Gazette employs a fail-safe to minimize the potential for a
journal offset to be written more than once: brokers require that the remote
fragment index not include a fragment offset larger than the append offset known
to replicating broker peers, and will refuse the append if this constraint is
violated.

Eg, If N >= R failures occur, then the set of broker peers of a journal will not
have participated in an append transaction; their append offset will be zero,
which is less than the maximum offset contained in the fragment store. The
brokers will refuse all appends to preclude double-writing of an offset.

This condition must be explicitly cleared by the Gazette operator using the
reset-head command. The operator should delay running reset-head until absolutely
confident that all journal fragments have been persisted to cloud storage (eg,
because all previous broker processes have exited).

Then, the effect of reset-head is to jump the append offset forward to the
maximum indexed offset, allowing new append operations to proceed.

reset-head is safe to run against journals which are in a fully consistent state,
though it is likely to fail harmlessly if the journal is being actively written.
`, &cmdJournalResetHead{})

	mbp.MustParseConfig(parser, iniFilename)
}

const iniFilename = "gazctl.ini"

// editCmdLongDescription is the common description of "journals edit" and "shards edit".
const editCmdLongDescription = `The edit command allows you to directly edit journal specifications matching the supplied LabelSelector. It will open the editor defined by your GAZ_EDITOR or EDITOR environment variables or fall back to 'vi'. Editing from Windows is currently not supported.

Upon exiting the editor, if the file has been changed, it will be validated and applied. If the file is invalid or fails to apply, the editor is re-opened. Exiting the editor with no changes or saving an empty file are interpreted as the user aborting the edit attempt.`

type editDescription struct {
	Type, HelpCommand, Examples string
}

func init() {
	// Avoid heavy duplication of text between "journals edit" and
	// "shards edit" commands by templating their long descriptions.
	var editTemplate = template.Must(template.New("template").Parse(`Edit and apply {{ .Type }} specifications.

The edit command allows you to directly edit {{ .Type }} specifications matching the supplied LabelSelector. It will open the editor defined by your GAZ_EDITOR or EDITOR environment variables or fall back to 'vi'. Editing from Windows is currently not supported.

Upon exiting the editor, if the file has been changed, it will be validated and applied. If the file is invalid or fails to apply, the editor is re-opened. Exiting the editor with no changes or saving an empty file are interpreted as the user aborting the edit attempt.

Use --selector to supply a LabelSelector which constrains the set of returned {{ .Type }} specifications. See "{{ .HelpCommand }}" for details and examples.

{{ .Examples }}
`))
	var journalData = editDescription{
		Type:        "journal",
		HelpCommand: "journals list --help",
		Examples: `Edit specifications of journals having an exact name:
>    gazctl journals edit --selector "name in (foo/bar, baz/bing)"

Use an alternative editor
>    GAZ_EDITOR=nano gazctl journals edit --selector "prefix = my/prefix/"`,
	}
	var shardData = editDescription{
		Type:        "shard",
		HelpCommand: "shards list --help",
		Examples: `Edit specifications of shards having an exact ID:
>    gazctl shards edit --selector "id in (foo, bar)"

Use an alternative editor
>    GAZ_EDITOR=nano gazctl shards edit --selector "id = baz"`,
	}

	// Save the template output to package vars.
	var buf = &bytes.Buffer{}
	if err := editTemplate.Execute(buf, journalData); err != nil {
		panic(err)
	}
	journalsEditLongDesc = buf.String()
	buf.Reset()
	if err := editTemplate.Execute(buf, shardData); err != nil {
		panic(err)
	}
	shardsEditLongDesc = buf.String()
}
