package main

import (
	"github.com/estuary/flow/go/runtime"
	"github.com/jessevdk/go-flags"
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

	addCmd(parser, "split", "Split a Flow processing shard", `
Split a Flow processing shard into two, either on shuffled key or rotated clock.
`, &cmdSplit{})

	addCmd(parser, "discover", "Discover available captures of an endpoint", `
Discover available captures of an endpoint
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
`, new(runtime.FlowConsumerConfig))

	addCmd(serve, "ingester", "Serve the Flow ingester", `
Serve a Flow ingester with the provided configuration, until signaled to
exit (via SIGTERM).
`, &cmdIngester{})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}

func addCmd(to interface {
	AddCommand(string, string, string, interface{}) (*flags.Command, error)
}, a, b, c string, iface interface{}) {
	var _, err = to.AddCommand(a, b, c, iface)
	mbp.Must(err, "failed to add flags parser command")
}
