package main

import (
	"github.com/estuary/flow/go/runtime"
	"github.com/jessevdk/go-flags"
	"go.gazette.dev/core/cmd/gazctl/gazctlcmd"
	mbp "go.gazette.dev/core/mainboilerplate"
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

	// The combine subcommand is hidden from help messages and such because we're uncertain of its
	// value, so don't want to expose it to users. We might just want to delete this, but leaving it
	// hidden for now. This was added to aid in debugging:
	// https://github.com/estuary/flow/issues/238
	var combineCommand = addCmd(parser, "combine", "Combine documents from stdin", `
Read documents from stdin, validate and combine them on the collection's key, and print the results to stdout. The input documents must be JSON encoded and given one per line, and the output documents will be printed in the same way.
`, &cmdCombine{})
	combineCommand.Hidden = true

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

	// journals command - Add all journals sub-commands from gazctl under this command.
	journals, err := parser.Command.AddCommand("journals", "Interact with broker journals", "", gazctlcmd.JournalsCfg)
	mbp.Must(gazctlcmd.CommandRegistry.AddCommands("journals", journals, true), "failed to add commands")

	// shards command - Add all shards sub-commands from gazctl under this command.
	shards, err := parser.Command.AddCommand("shards", "Interact with consumer shards", "", gazctlcmd.ShardsCfg)
	mbp.Must(gazctlcmd.CommandRegistry.AddCommands("shards", shards, true), "failed to add commands")

	mbp.AddPrintConfigCmd(parser, iniFilename)

	// Parse config and start command
	mbp.MustParseConfig(parser, iniFilename)

}

func addCmd(to interface {
	AddCommand(string, string, string, interface{}) (*flags.Command, error)
}, a, b, c string, iface interface{}) *flags.Command {
	var cmd, err = to.AddCommand(a, b, c, iface)
	mbp.Must(err, "failed to add flags parser command")
	return cmd
}
