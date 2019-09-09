package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"os"
	"sort"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/message"
)

type cmdJournalAppend struct {
	Selector string `long:"selector" short:"l" required:"true" description:"Label selector of journals to append to"`
	Input    string `long:"input" short:"i" default:"-" description:"Input file path. Use '-' for stdin"`
	Framing  string `long:"framing" short:"f" choice:"none" choice:"lines" choice:"fixed" default:"none" description:"Framing of records in input, if any"`
	Mapping  string `long:"mapping" short:"m" choice:"random" choice:"modulo" choice:"rendezvous" choice:"direct" default:"random" description:"Mapping function of records to journals"`
	Base64   bool   `long:"base64" description:"Partition keys under 'lines' framing are interpreted as base64"`

	unpackKey    func(r *bufio.Reader) ([]byte, error)
	unpackRecord func(r *bufio.Reader) ([]byte, error)
	mapping      message.MappingFunc
	appendSvc    *client.AppendService
}

func init() {
	_ = mustAddCmd(cmdJournals, "append", "Append journal content", `
Append content to one or more journals.

A label --selector is required, and determines the set of journals which are appended.
See "journals list --help" for details and examples of using journal selectors.

If --framing 'none', then --mapping must be 'random' and the input is atomically
appended to a random journal of the selector. Note --selector name=my/journal/name
can be used to append to a specific journal.

If --framing 'lines' then records are read from the input line-by-line. Each is
mapped to a journal and appended on an atomic-per-record basis. The relative
ordering of records in a specific mapped journal is preserved. --framing 'fixed'
functions like 'lines', except that records are read from input delimited by a
a leading fixed-framing header. Note that record delimiters (newlines or fixed-
framing headers) are retained and included when appending into mapped journals.

If --mapping 'random', each record is independently mapped to a random journal.
If --mapping 'modulo' or 'rendezvous', then each input record is expected to be
preceded by a partition-key written with the same --framing (eg, if --framing
'lines' then 'A-Partition-Key\nA-Record\n'). The partition key is used to map
the record which follows to a target journal under the applicable mapping scheme
(eg, modulo arithmetic or rendezvous / "cuckoo" hashing). To use binary
partition keys with --mapping 'lines', encoded each partition key using base64
and specify --base64.

If --mapping 'direct', then each input record is preceded by a journal name,
which must be a current journal of the --selector, and to which the record is
appended.

Use --log.level=debug to inspect individual mapping decisions.

Examples:

# Write the content of ./fizzbuzz to my/journal:
gazctl journals append -l name=my/journal -i ./fizzbuz

# Write two records to partitions of my-label mapped by Key1 and Key2, respectively:
gazctl journals append -l my-label --framing 'lines' --mapping 'modulo' --base64 << EOF
S2V5MQ==
{"Msg": "record 1"}
S2V5Mg==
{"Msg": "record 2"}
EOF

# Serve all writers to my-fifo as a long-lived daemon. Note that posix FIFOs do
# not EOF while at least one process holds an open write descriptor. But, do
# take care to have just one pipe writer at a time:
mkfifo my-fifo
cat /dev/stdout > my-fifo &	# Hold my-fifo open so gazctl doesn't read EOF.
gazctl journals append -l my-label --framing 'lines' --mapping 'rendezvous' --input my-fifo
	`, &cmdJournalAppend{})
}

func (cmd *cmdJournalAppend) Execute([]string) error {
	startup()

	// Validate argument combinations.
	if cmd.Framing == "none" && cmd.Mapping != "random" {
		log.Fatal("'none' --framing may only be used with 'random' --mapping")
	} else if cmd.Base64 && (cmd.Framing != "lines" || cmd.Mapping == "random") {
		log.Fatal("--base64 requires --framing 'lines' and not --mapping 'random'")
	}

	var err error
	var ctx = context.Background()
	var listRequest pb.ListRequest

	listRequest.Selector, err = pb.ParseLabelSelector(cmd.Selector)
	mbp.Must(err, "failed to parse label selector", "selector", cmd.Selector)

	// Perform an initial load and thereafter periodically poll for journals
	// matching the --selector.
	var rjc = journalsCfg.Broker.MustRoutedJournalClient(ctx)
	list, err := client.NewPolledList(ctx, rjc, time.Minute, listRequest)
	mbp.Must(err, "failed to resolve label selector to journals")

	var fin = os.Stdin
	if cmd.Input != "-" {
		fin, err = os.Open(cmd.Input)
		mbp.Must(err, "failed to open input file")
	}
	var input = bufio.NewReaderSize(fin, 32*1024)

	// If --framing=none than we process all input as a single, atomic append to
	// a random mapped journal of the --selector.
	if cmd.Framing == "none" {
		var journal, _, err = message.RandomMapping(list.List)(nil)
		mbp.Must(err, "mapping to journal")

		var app = client.NewAppender(ctx, rjc, pb.AppendRequest{Journal: journal})
		if _, err = io.Copy(app, input); err == nil {
			err = app.Close()
		} else {
			app.Abort()
		}
		mbp.Must(err, "appending to journal")

		if log.GetLevel() >= log.DebugLevel {
			log.WithFields(log.Fields{
				"commit": app.Response.Commit,
				"broker": app.Response.Header.ProcessId,
			}).Debug("append response")
		}
		return nil // All done.
	}

	switch cmd.Framing {
	case "lines":
		if cmd.Base64 {
			cmd.unpackKey = unpackKeyLineBase64
		} else {
			cmd.unpackKey = unpackKeyLine
		}
		cmd.unpackRecord = message.UnpackLine
	case "fixed":
		cmd.unpackKey = unpackKeyFixed
		cmd.unpackRecord = message.FixedFraming.Unpack
	default:
		log.Fatal("invalid framing")
	}

	switch cmd.Mapping {
	case "random":
		cmd.unpackKey = nil // We don't expect to see partition keys.
		cmd.mapping = message.RandomMapping(list.List)
	case "modulo":
		cmd.mapping = message.ModuloMapping(byteMappingFunc, list.List)
	case "rendezvous":
		cmd.mapping = message.RendezvousMapping(byteMappingFunc, list.List)
	case "direct":
		cmd.mapping = func(msg message.Mappable) (journal pb.Journal, _ message.Framing, err error) {
			journal = pb.Journal(msg.([]byte))

			var all = list.List().Journals
			var ind = sort.Search(len(all), func(i int) bool { return journal >= all[i].Spec.Name })

			if ind == len(all) || all[ind].Spec.Name != journal {
				err = errors.Errorf("journal %q not found", journal)
			}
			return
		}
	default:
		log.Fatal("invalid mapping")
	}

	cmd.appendSvc = client.NewAppendService(ctx, rjc)
	mbp.Must(cmd.process(input), "failed to process record stream")

	for op := range cmd.appendSvc.PendingExcept("") {
		mbp.Must(op.Err(), "failed to flush remaining appends")
	}
	return nil
}

// process stream of records from |r| until EOF. If unpackKey != nil, a
// preceding partition key is read and mapped for each record.
func (cmd *cmdJournalAppend) process(r *bufio.Reader) error {
	for {
		var b []byte
		var err error

		if cmd.unpackKey != nil {
			if b, err = cmd.unpackKey(r); err == io.EOF {
				return nil
			} else if err != nil {
				return errors.WithMessage(err, "unpacking partition key")
			}
		}

		journal, _, err := cmd.mapping(b)
		if err != nil {
			return errors.WithMessage(err, "mapping partition key to journal")
		}

		if log.GetLevel() >= log.DebugLevel {
			log.WithFields(log.Fields{
				"journal": journal,
				"key":     string(b),
			}).Debug("mapped partition key to journal")
		}

		if b, err = cmd.unpackRecord(r); err == io.EOF {
			return nil
		} else if err != nil {
			return errors.WithMessage(err, "unpacking record")
		}

		var aa = cmd.appendSvc.StartAppend(pb.AppendRequest{Journal: journal}, nil)
		_, _ = aa.Writer().Write(b)
		if err = aa.Release(); err != nil {
			return errors.WithMessage(err, "starting append")
		}
	}
}

// unpackKeyLine reads a newline-trimmed line from |r|.
func unpackKeyLine(r *bufio.Reader) ([]byte, error) {
	var b, err = message.UnpackLine(r)
	b = bytes.TrimRight(b, "\r\n")
	return b, err
}

// unpackKeyLineBase64 reads a b64-decoded line from |r|.
func unpackKeyLineBase64(r *bufio.Reader) (b []byte, err error) {
	if b, err = unpackKeyLine(r); err != nil {
		return
	}
	n, err := base64.StdEncoding.Decode(b, b) // Decode |b| in-place.
	b = b[:n]
	return
}

// unpackKeyFixed reads a binary key written with a fixed-framing header from |r|.
func unpackKeyFixed(r *bufio.Reader) (b []byte, err error) {
	if b, err = message.FixedFraming.Unpack(r); err != nil {
		return
	}
	// Unmarshal() in-place to remove the header from |b|, leaving
	// the payload portion only.
	return b, message.FixedFraming.Unmarshal(b, (*unmarshalBytes)(&b))
}

// unmarshalBytes is a []byte slice that Unmarshal()s by copying into itself.
type unmarshalBytes []byte

func (b *unmarshalBytes) Unmarshal(o []byte) error {
	var n = copy(*b, o)
	*b = (*b)[:n]
	return nil
}

// byteMappingFunc expects and passes-through []byte messages. The key unpackers
// uphold this expectation.
func byteMappingFunc(m message.Mappable, w io.Writer) { _, _ = w.Write(m.([]byte)) }
