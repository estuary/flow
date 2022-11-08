package airbyte

import (
	"encoding/json"
	"fmt"
	"os"

	flags "github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
)

// JSONFile represents a path to a file with JSON contents.
type JSONFile string

// Parse unmarshals the JSON contents into the given target.
func (c JSONFile) Parse(target interface{ Validate() error }) error {
	var r, err = os.Open(string(c))
	if err != nil {
		return fmt.Errorf("opening '%s': %w", c, err)
	}

	var d = json.NewDecoder(r)
	d.DisallowUnknownFields()

	if err = d.Decode(target); err != nil {
		return fmt.Errorf("decoding '%s': %w", c, err)
	} else if err = target.Validate(); err != nil {
		return fmt.Errorf("validating '%s': %w", c, err)
	}
	return nil
}

type ConfigFile struct {
	ConfigFile JSONFile `long:"config" description:"Path to connector configuration"`
}

// Parse delegates to JSONFile.Parse.
func (c ConfigFile) Parse(target interface{ Validate() error }) error {
	return c.ConfigFile.Parse(target)
}

// logConfig configures handling of application log events.
type logConfig struct {
	Level  string `long:"log.level" env:"LOG_LEVEL" default:"info" choice:"info" choice:"debug" choice:"warn" description:"Logging level"`
	Format string `long:"log.format" env:"LOG_FORMAT" default:"text" choice:"json" choice:"text" choice:"color" description:"Logging output format"`
}

func (cfg *logConfig) setup() {
	if cfg.Format == "json" {
		log.SetFormatter(&log.JSONFormatter{})
	} else if cfg.Format == "text" {
		log.SetFormatter(&log.TextFormatter{})
	} else if cfg.Format == "color" {
		log.SetFormatter(&log.TextFormatter{ForceColors: true})
	}

	if lvl, err := log.ParseLevel(cfg.Level); err != nil {
		log.WithField("err", err).Fatal("unrecognized log level")
	} else {
		log.SetLevel(lvl)
	}
}

type SpecCmd struct {
	logging    *logConfig
	actualSpec Spec `no-flag:"y"`
}

func (c *SpecCmd) Execute(_ []string) error {
	c.logging.setup()
	return NewStdoutEncoder().Encode(
		&Message{
			Type: MessageTypeSpec,
			Spec: &c.actualSpec,
		})
}

type CheckCmd struct {
	ConfigFile
	logging *logConfig
	doCheck func(CheckCmd) error `no-flag:"y"`
}

func (c *CheckCmd) Execute(_ []string) error {
	c.logging.setup()
	return c.doCheck(*c)
}

type DiscoverCmd struct {
	ConfigFile
	logging    *logConfig
	doDiscover func(DiscoverCmd) error `no-flag:"y"`
}

func (c *DiscoverCmd) Execute(_ []string) error {
	c.logging.setup()
	return c.doDiscover(*c)
}

type ReadCmd struct {
	ConfigFile
	StateFile   JSONFile `long:"state"`
	CatalogFile JSONFile `long:"catalog"`
	logging     *logConfig
	doRead      func(ReadCmd) error `no-flag:"y"`
}

func (c *ReadCmd) Execute(_ []string) error {
	c.logging.setup()
	return c.doRead(*c)
}

// RunMain does argument parsing and executes the given subcommand. This function will not return.
// It will call `os.Exit` with an appropriate exit code.
func RunMain(spec Spec, doCheck func(CheckCmd) error, doDiscover func(DiscoverCmd) error, doRead func(ReadCmd) error) {
	var logConfig = &logConfig{}
	var parser = flags.NewParser(logConfig, flags.Default)
	var specCmd = SpecCmd{
		logging:    logConfig,
		actualSpec: spec,
	}
	parser.AddCommand("spec", "prints the spec", "prints the ConnectorDefinition to stdout and exits", &specCmd)

	var checkCmd = CheckCmd{
		logging: logConfig,
		doCheck: doCheck,
	}
	parser.AddCommand("check", "Checks the connection", "Tries to connect to the external system to validate the connection information", &checkCmd)

	var discoverCmd = DiscoverCmd{
		logging:    logConfig,
		doDiscover: doDiscover,
	}
	parser.AddCommand("discover", "List Streams that can be captured", "Prints a Catalog enumerating all of the Streams that may be read", &discoverCmd)

	var readCmd = ReadCmd{
		logging: logConfig,
		doRead:  doRead,
	}
	parser.AddCommand("read", "Read records from the remote system", "Reads records and prints them to stdout", &readCmd)

	// This will actually execute the given subcommand because that's clearly what "parse" means /s.
	var _, err = parser.Parse()
	if err != nil {
		// We don't log the error because flags.Default includes flags.PrintErrors,
		// and thus Parse() has already done so on our behalf.
		os.Exit(1)
	}
	os.Exit(0)
}

func NewStdoutEncoder() *json.Encoder {
	return json.NewEncoder(os.Stdout)
}
