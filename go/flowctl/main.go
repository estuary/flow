package main

import (
	"github.com/jessevdk/go-flags"

	"go.gazette.dev/core/cmd/gazctl/gazctlcmd"
	mbp "go.gazette.dev/core/mainboilerplate"

	"github.com/estuary/flow/go/runtime"
)

const iniFilename = "flow.ini"

func main() {
	var parser = flags.NewParser(nil, flags.HelpFlag|flags.PassDoubleDash)

	addCmd(parser, "apply", "Apply a Flow catalog to a cluster", `
Build a Flow catalog and apply it to a running cluster.
`, &cmdApply{})

	addCmd(parser, "test", "Locally test a Flow catalog", `
Locally test a Flow catalog.
`, &cmdTest{})

	addCmd(parser, "develop", "Locally develop a Flow catalog", `
Locally develop a Flow catalog.
`, &cmdDevelop{})

	addCmd(parser, "check", "Check a Flow catalog for errors", `
Quickly load and validate a Flow catalog, and generate updated TypeScript types.
`, &cmdCheck{})

	addCmd(parser, "discover", "Discover available captures of an endpoint", `
Inspect a configured endpoint, and generate a Flow catalog of collections,
schemas, and capture bindings which reflect its available resources.

Discover is a two-stage workflow:

In the first invocation, the command will generate a stub
configuration YAML derived from the connector's specification.
The user reviews this YAML file, and updates it with appropriate
credentials and configuration.

In the second invocation, the command applies the completed
configuration to the endpoint and determines its available resource
bindings. It generates a Flow catalog YAML file with a Flow Capture
and associated Collection definitions. The user may then review,
update, refactor, and otherwise incorporate the generated entities
into their broader Flow catalog.
`, &cmdDiscover{})

	addCmd(parser, "json-schema", "Print the catalog JSON schema", `
Print the JSON schema specification of Flow catalogs, as understood by this
specific build of Flow. This JSON schema can be used to enable IDE support
and auto-completions.
`, &cmdJSONSchema{})

	serve, err := parser.Command.AddCommand("serve", "Serve a component of Flow", "", &struct{}{})
	mbp.Must(err, "failed to add command")

	addCmd(serve, "consumer", "Serve the Flow consumer", `
serve a Flow consumer with the provided configuration, until signaled to
exit (via SIGTERM). Upon receiving a signal, the consumer will seek to discharge
its responsible shards and will exit only when it can safely do so.
`, &runtime.FlowConsumerConfig{ConnectorNetwork: ""})

	addCmd(serve, "ingester", "Serve the Flow ingester", `
Serve a Flow ingester with the provided configuration, until signaled to
exit (via SIGTERM).
`, &cmdIngester{})

	// journals command - add all journal sub-commands from gazctl under this command
	journals, err := parser.Command.AddCommand("journals", "Interact with broker journals", "", gazctlcmd.JournalsCfg)
	mbp.Must(gazctlcmd.CmdRegistry.AddCmds("journals", journals), "failed to add commands")

	// journals command - add all shards sub-commands from gazctl under this command
	shards, err := parser.Command.AddCommand("shards", "Interact with consumer shards", "", gazctlcmd.ShardsCfg)
	mbp.Must(gazctlcmd.CmdRegistry.AddCmds("shards", shards), "failed to add commands")

	// Add split as subcommand of shards command
	addCmd(shards, "split", "Split a Flow processing shard", `
Split a Flow processing shard into two, either on shuffled key or rotated clock.
`, &cmdSplit{})

	mbp.AddPrintConfigCmd(parser, iniFilename)

	// Parse config and start command
	mbp.MustParseConfig(parser, iniFilename)

}

func addCmd(to interface {
	AddCommand(string, string, string, interface{}) (*flags.Command, error)
}, a, b, c string, iface interface{}) {
	var _, err = to.AddCommand(a, b, c, iface)
	mbp.Must(err, "failed to add flags parser command")
}
