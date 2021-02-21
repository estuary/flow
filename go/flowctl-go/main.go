package main

import (
	"github.com/jessevdk/go-flags"
	mbp "go.gazette.dev/core/mainboilerplate"
)

const iniFilename = "flow.ini"

// Config is the top-level configuration object of flowctl.
var Config = new(struct {
	mbp.ZoneConfig
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
})

func main() {
	var parser = flags.NewParser(Config, flags.Default)

	_, _ = parser.AddCommand("build", "Build a Flow catalog", `
Build a Flow catalog into a build directory and catalog database.
`, &cmdBuild{})

	_, _ = parser.AddCommand("test", "Test a Flow catalog", `
Build and test a Flow catalog.
`, &cmdTest{})

	_, _ = parser.AddCommand("develop", "Develop a Flow catalog", `
Build and develop a Flow catalog.
`, &cmdDevelop{})

	_, _ = parser.AddCommand("json-schema", "Print the catalog JSON schema", `
Print the JSON schema specification of Flow catalogs, as understood by this
specific build of Flow. This JSON schema can be used to enable IDE support
and auto-completions.
`, &cmdJSONSchema{})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}
