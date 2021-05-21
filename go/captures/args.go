package captures

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	flags "github.com/jessevdk/go-flags"
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

type SpecCmd struct{}

type CheckCmd struct {
	ConfigFile
}

type DiscoverCmd struct {
	ConfigFile
}

type ReadCmd struct {
	ConfigFile
	StateFile   JSONFile `long:"state"`
	CatalogFile JSONFile `long:"catalog"`
}

type Args struct {
	Spec     *SpecCmd     `command:"spec"`
	Check    *CheckCmd    `command:"check"`
	Discover *DiscoverCmd `command:"discover"`
	Read     *ReadCmd     `command:"read"`
}

func ParseArgsOrExit() *Args {
	var args = &Args{}
	var _, err = flags.Parse(args)
	if err != nil {
		os.Exit(1)
	}
	return args
}

func (args *Args) Run(spec Spec, doCheck func(CheckCmd) error, doDiscover func(DiscoverCmd) error, doRead func(ReadCmd) error) {
	var err error
	if args.Spec != nil {
		var message = Message{
			Type: MessageTypeSpec,
			Spec: &spec,
		}
		var encoder = NewStdoutEncoder()
		err = encoder.Encode(message)
	} else if args.Check != nil {
		err = doCheck(*args.Check)
	} else if args.Discover != nil {
		err = doDiscover(*args.Discover)
	} else if args.Read != nil {
		err = doRead(*args.Read)
	} else {
		err = fmt.Errorf("missing subcommand, one of spec, check, discover, or read is required")
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func NewStdoutEncoder() *json.Encoder {
	return json.NewEncoder(os.Stdout)
}
