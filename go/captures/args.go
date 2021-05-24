package captures

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	flags "github.com/jessevdk/go-flags"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type JSONFile string

type ConfigFile struct {
	ConfigFile JSONFile `long:"config"`
}

func (c JSONFile) Parse(target interface{}) error {
	var bytes, err = ioutil.ReadFile(string(c))
	if err != nil {
		return fmt.Errorf("reading '--config': %w", err)
	}
	err = json.Unmarshal(bytes, target)
	if err != nil {
		return fmt.Errorf("parsing '--config' json: %w", err)
	}
	return nil
}

type SpecCmd struct {
	actualSpec Spec          `no-flag:"y"`
	Log        mbp.LogConfig `group:"Logging" namespace:"log" env-namespace:"LOG"`
}

func (s *SpecCmd) Execute(_ []string) error {
	mbp.InitLog(s.Log)
	return NewStdoutEncoder().Encode(&s.actualSpec)
}

type CheckCmd struct {
	ConfigFile
	Log     mbp.LogConfig        `group:"Logging" namespace:"log" env-namespace:"LOG"`
	doCheck func(CheckCmd) error `no-flag:"y"`
}

func (c *CheckCmd) Execute(_ []string) error {
	mbp.InitLog(c.Log)
	return c.doCheck(*c)
}

type DiscoverCmd struct {
	ConfigFile
	Log        mbp.LogConfig           `group:"Logging" namespace:"log" env-namespace:"LOG"`
	doDiscover func(DiscoverCmd) error `no-flag:"y"`
}

func (c *DiscoverCmd) Execute(_ []string) error {
	mbp.InitLog(c.Log)
	return c.doDiscover(*c)
}

type ReadCmd struct {
	ConfigFile
	Log         mbp.LogConfig       `group:"Logging" namespace:"log" env-namespace:"LOG"`
	StateFile   JSONFile            `long:"state"`
	CatalogFile JSONFile            `long:"catalog"`
	doRead      func(ReadCmd) error `no-flag:"y"`
}

func (c *ReadCmd) Execute(_ []string) error {
	mbp.InitLog(c.Log)
	return c.doRead(*c)
}

func RunMain(spec Spec, doCheck func(CheckCmd) error, doDiscover func(DiscoverCmd) error, doRead func(ReadCmd) error) {
	var parser = flags.NewParser(nil, flags.Default)
	var specCmd = SpecCmd{
		actualSpec: spec,
	}
	parser.AddCommand("spec", "prints the spec", "prints the ConnectorDefinition to stdout and exits", &specCmd)

	var checkCmd = CheckCmd{
		doCheck: doCheck,
	}
	parser.AddCommand("check", "Checks the connection", "Tries to connect to the external system to validate the connection information", &checkCmd)

	var discoverCmd = DiscoverCmd{
		doDiscover: doDiscover,
	}
	parser.AddCommand("discover", "List Streams that can be captured", "Prints a Catalog enumerating all of the Streams that may be read", &discoverCmd)

	var readCmd = ReadCmd{
		doRead: doRead,
	}
	parser.AddCommand("read", "Read records from the remote system", "Reads records and prints them to stdout", &readCmd)

	var _, err = parser.Parse()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: ", err)
		os.Exit(1)
	}
}

func NewStdoutEncoder() *json.Encoder {
	return json.NewEncoder(os.Stdout)
}
