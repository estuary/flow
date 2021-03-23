package mainboilerplate

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jessevdk/go-flags"
)

// MustParseConfig requires that the Parser parse from the combination of an
// optional INI file, configured environment bindings, and explicit flags.
// An INI file matching |configName| is searched for in:
//  * The current working directory.
//  * ~/.config/gazette (under the users's $HOME or %UserProfile% directory).
//  * $APPLICATION_CONFIG_ROOT
func MustParseConfig(parser *flags.Parser, configName string) {
	// Allow unknown options while parsing an INI file.
	var origOptions = parser.Options
	parser.Options |= flags.IgnoreUnknown

	var iniParser = flags.NewIniParser(parser)

	var prefixes = []string{
		".",
		filepath.Join(os.Getenv("HOME"), ".config", "gazette"),
		filepath.Join(os.Getenv("UserProfile"), ".config", "gazette"),
	}
	for _, prefix := range prefixes {
		var path = filepath.Join(prefix, configName)

		if err := iniParser.ParseFile(path); err == nil {
			break
		} else if os.IsNotExist(err) {
			// Pass.
		} else {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	// Restore original options for parsing argument flags.
	parser.Options = origOptions
	MustParseArgs(parser)
}

// MustParseArgs requires that Parser be able to ParseArgs without error.
func MustParseArgs(parser *flags.Parser) {
	if _, err := parser.ParseArgs(os.Args[1:]); err != nil {
		var flagErr, ok = err.(*flags.Error)
		if !ok {
			Must(err, "fatal error")
		}

		switch flagErr.Type {
		case flags.ErrDuplicatedFlag, flags.ErrTag, flags.ErrInvalidTag, flags.ErrShortNameTooLong, flags.ErrMarshal:
			// These error types indicate a problem in the configuration object
			// |parser| was asked to parse (eg, a developer error rather than input error).
			panic(err)

		case flags.ErrCommandRequired:
			// Extend go-flag's "Please specify one command of: ... " output with the full usage.
			// This provides a nicer UX to users running the bare binary.
			os.Stderr.WriteString("\n")
			parser.WriteHelp(os.Stderr)
			fmt.Fprintf(os.Stderr, "\nVersion %s, built at %s.\n", Version, BuildDate)
			os.Exit(1)

		case flags.ErrHelp:
			if parser.Options&flags.PrintErrors != 0 {
				// Help was already printed.
			} else {
				parser.WriteHelp(os.Stderr)
				fmt.Fprintf(os.Stderr, "\nVersion %s, built at %s.\n", Version, BuildDate)
			}
			os.Exit(1)

		default:
			// Other error types indicate a problem of input. Generally, `go-flags`
			// already prints a helpful message and we can simply exit.
			os.Exit(1)
		}
	}
}

// AddPrintConfigCmd to the Parser. The "print-config" command helps users test
// whether their applications are correctly configured, by exporting all runtime
// configuration in INI format.
func AddPrintConfigCmd(parser *flags.Parser, configName string) {
	parser.AddCommand("print-config", "Print combined configuration and exit", `
print-config parses the combined configuration from `+configName+`, flags,
and environment variables, and then writes the configuration to stdout in INI format.
`, &printConfig{parser})
}

type printConfig struct {
	*flags.Parser `no-flag:"t"`
}

func (p printConfig) Execute([]string) error {
	var ini = flags.NewIniParser(p.Parser)
	ini.Write(os.Stdout, flags.IniIncludeComments|flags.IniCommentDefaults|flags.IniIncludeDefaults)
	return nil
}
