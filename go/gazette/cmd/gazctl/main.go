package main

import (
	"github.com/jessevdk/go-flags"

	"go.gazette.dev/core/cmd/gazctl/gazctlcmd"
	mbp "go.gazette.dev/core/mainboilerplate"
)

const iniFilename = "gazctl.ini"

func main() {
	parser := flags.NewParser(nil, flags.Default)

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.Must(gazctlcmd.AddCmdAttachUUIDs(parser.Command), "could not add attach-uuids subcommand")

	parser.LongDescription = `gazctl is a tool for interacting with Gazette brokers and consumer applications.

	See --help pages of each sub-command for documentation and usage examples.
	Optionally configure gazctl with a '` + iniFilename + `' file in the current working directory,
	or with '~/.config/gazette/` + iniFilename + `'. Use the 'print-config' sub-command to inspect
	the tool's current configuration.
	`

	// Create these journals and shards commands to contain sub-commands
	_ = mustAddCmd(parser.Command, "journals", "Interact with broker journals", "", gazctlcmd.JournalsCfg)
	_ = mustAddCmd(parser.Command, "shards", "Interact with consumer shards", "", gazctlcmd.ShardsCfg)

	// Add all registered commands to the root parser.Command
	mbp.Must(gazctlcmd.CommandRegistry.AddCommands("", parser.Command, true), "could not add subcommand")

	// Parse config and start app
	mbp.MustParseConfig(parser, iniFilename)
}

func mustAddCmd(cmd *flags.Command, name, short, long string, cfg interface{}) *flags.Command {
	cmd, err := cmd.AddCommand(name, short, long, cfg)
	mbp.Must(err, "failed to add command")
	return cmd
}
