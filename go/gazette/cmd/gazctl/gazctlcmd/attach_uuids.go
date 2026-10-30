package gazctlcmd

import (
	"bufio"
	"os"
	"text/template"
	"time"

	"github.com/jessevdk/go-flags"
	"go.gazette.dev/core/message"
)

type cmdAttachUUIDs struct {
	Template   string `long:"template" default:"{{.UUID}},{{.Line}}\n" description:"Go text/template for output"`
	MaxLength  int    `long:"max-length" description:"Maximum allowed byte-length of an input line" default:"4194304"`
	SkipHeader bool   `long:"skip-header" description:"Omit the first line of each input file"`
	Input      struct {
		Paths []string
	} `positional-args:"yes" positional-arg-name:"/path/to/file"`
}

func AddCmdAttachUUIDs(cmd *flags.Command) error {
	_, err := cmd.AddCommand("attach-uuids",
		"Generate and attach UUIDs to text input records", `
For each line of each argument input file, generate a RFC 4122 v1 compatible
UUID and, using the --template, combine it with the input line into output
written to stdout. If no input file arguments are given, stdin is read instead.
	
Exactly-once processing semantics require that messages carry a v1 UUID which
is authored by Gazette. The UUID encodes a unique producer ID, monotonic Clock,
and transaction flags.

attach-uuids facilitates pre-processing text files or unix pipelines in
preparation for appending to a journal, by associating each input with a
corresponding UUID. UUIDs are flagged as committed, meaning they will be
processed immediately by readers. attach-uuids may be used directly in a
pipeline of streamed records.

When processing files in preparation for append to Gazette, it's best practice
to attach UUIDs into new temporary file(s), and then append the temporary files
to journals. This ensures messages are processed only once even if one or both
of the attach-uuids or append steps fail partway through and are restarted.

However avoid appending many small files in this way, as each invocation of
attach-uuids generates a new random producer ID, and each producer ID requires
that consumers track a very small amount of state (eg, its Clock). Instead,
first combine many small files into few large ones before attaching UUIDs.
	
Prefix CSV rows with a UUID (using the default --template):
>  gazctl attach-uuids inputOne.csv inputTwo.csv inputN.csv
	
Prefix CSV rows, but skip a initial header row of each input:
>  gazctl attach-uuids --skip-header inputOne.csv inputTwo.csv
	
Postfix CSV rows with a UUID (use $'..' to correctly handle newline escape):
>  gazctl attach-uuids input.csv --template=$'{{.Line}},{{.UUID}}\n'
	
Wrap JSON inputs with a UUID:
> gazctl attach-uuids input.json --template=$'{"uuid": "{{.UUID}}","record":{{.Line}}}\n'

Optionally compose with "jq" to un-nest the JSON objects:
> gazctl attach-uuids input.json --template=$'{"uuid": "{{.UUID}}","record":{{.Line}}}\n' \
>	| jq -c '{uuid: .uuid} + .record'
`, &cmdAttachUUIDs{})
	return err
}

func (cmd *cmdAttachUUIDs) Execute([]string) error {
	var tpl, err = template.New("root").Parse(cmd.Template)
	if err != nil {
		return err
	}

	var inputs []*os.File
	for _, path := range cmd.Input.Paths {
		if f, err := os.Open(path); err != nil {
			return err
		} else {
			inputs = append(inputs, f)
		}
	}
	if len(inputs) == 0 {
		inputs = []*os.File{os.Stdin}
	}

	var producer = message.NewProducerID()
	var clock = message.NewClock(time.Now())

	go func() {
		var ticker = time.NewTicker(time.Millisecond * 100)
		for now := range ticker.C {
			clock.Update(now)
		}
	}()

	for _, input := range inputs {
		var scanner = bufio.NewScanner(input)
		scanner.Buffer(nil, cmd.MaxLength)

		for skip := cmd.SkipHeader; scanner.Scan(); {
			if skip {
				skip = false
				continue
			}

			if err = tpl.Execute(os.Stdout, struct {
				Line string
				UUID message.UUID
			}{
				Line: scanner.Text(),
				UUID: message.BuildUUID(producer, clock.Tick(), message.Flag_OUTSIDE_TXN),
			}); err != nil {
				return err
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	return nil
}
