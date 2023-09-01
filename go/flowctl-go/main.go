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

	addCmd(parser, "test", "Locally test a Flow catalog", `
Locally test a Flow catalog.
		`, &cmdTest{})

	addCmd(parser, "temp-data-plane", "Run an ephemeral, temporary local data plane", `
Run a local data plane by shelling out to start Etcd, Gazette, and the Flow consumer.
A local data plane is intended for local development and testing, and doesn't persist
fragments to the configured storage mappings of collections and Flow tasks.
Upon exit, all data is discarded.
`, &cmdTempDataPlane{})

	serve, err := parser.Command.AddCommand("serve", "Serve a component of Flow", "", &struct{}{})
	mbp.Must(err, "failed to add command")

	addCmd(serve, "consumer", "Serve the Flow consumer", `
serve a Flow consumer with the provided configuration, until signaled to
exit (via SIGTERM). Upon receiving a signal, the consumer will seek to discharge
its responsible shards and will exit only when it can safely do so.
`, &runtime.FlowConsumerConfig{})

	// journals command - Add all journals sub-commands from gazctl under this command.
	journals, err := parser.Command.AddCommand("journals", "Interact with broker journals", "", gazctlcmd.JournalsCfg)
	mbp.Must(err, "failed to add journals command")
	mbp.Must(gazctlcmd.CommandRegistry.AddCommands("journals", journals, true), "failed to add commands")

	// shards command - Add all shards sub-commands from gazctl under this command.
	shards, err := parser.Command.AddCommand("shards", "Interact with consumer shards", "", gazctlcmd.ShardsCfg)
	mbp.Must(err, "failed to add shards command")
	mbp.Must(gazctlcmd.CommandRegistry.AddCommands("shards", shards, true), "failed to add commands")

	mbp.AddPrintConfigCmd(parser, iniFilename)

	apis, err := parser.Command.AddCommand("api", "Low-level APIs for automation", `
API commands which are designed for use in scripts and automated workflows,
including the Flow control plane. Users should not need to run API commands
directly (but are welcome to).
	`, &struct{}{})
	mbp.Must(err, "failed to add command")

	addCmd(apis, "build", "Build a Flow catalog", `
Build a Flow catalog.
`, &apiBuild{})

	addCmd(apis, "activate", "Activate a built Flow catalog", `
Activate tasks and collections of a Flow catalog.
`, &apiActivate{})

	addCmd(apis, "test", "Run tests of an activated catalog", `
Run tests of an activated catalog
`, &apiTest{})

	addCmd(apis, "await", "Wait for a catalog's dataflow to complete", `
Monitor a catalog's dataflow execution in the data-plane, and exit when it finishes.
`, &apiAwait{})

	addCmd(apis, "delete", "Delete from a built Flow catalog", `
Delete tasks and collections of a built Flow catalog.
`, &apiDelete{})

	addCmd(apis, "spec", "Query a connector image for its specification", `
Query a connector image for its specification.
`, &apiSpec{})

	addCmd(apis, "discover", "Discover resource of a capture connector", `
Discover resources of a capture connector using a configuration.
`, &apiDiscover{})

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
