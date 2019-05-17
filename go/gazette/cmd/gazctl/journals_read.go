package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/client"
	mbp "go.gazette.dev/core/mainboilerplate"
	pb "go.gazette.dev/core/protocol"
)

type cmdJournalRead struct {
	Selector string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on"`
	Blocking bool   `long:"blocking" short:"b" description:"Stream contents to Stdout as they are written to the selected journals"`
	Offset   int64  `long:"offset" short:"o" default:"-1" description:"Offset to beging reading from journal"`
}

func init() {
	_ = mustAddCmd(cmdJournals, "read", "Read journal contents", `
Read the contents journal or journals as a stream.

Use --selector to supply a LabelSelector which constrains the set of journals
to be read from.

Match JournalSpecs having an exact name:
>    --selector "name in (foo/bar, baz/bing)"

Match JournalSpecs having a name prefix (must end in '/'):
>    --selector "prefix = my/prefix/"

Read can run in a blocking fashion with --blocking which will not exit when
it has reached the head of the current journal(s). When new data becomes available
it will be sent to Stdout.

To read from an arbitrary offset into a journal(s) use the --offset flag.
If not passed the default value is -1 which will read from the head of the journal.
`, &cmdJournalRead{})
}

func (cmd *cmdJournalRead) Execute([]string) error {
	startup()

	var err error
	var ctx = context.Background()
	var brokerClient = journalsCfg.Broker.MustRoutedJournalClient(ctx)

	// Get the list of journals which match this selector.
	var listRequest pb.ListRequest
	listRequest.Selector, err = pb.ParseLabelSelector(cmd.Selector)
	mbp.Must(err, "failed to parse label selector", "selector", cmd.Selector)

	var listResp *pb.ListResponse
	listResp, err = client.ListAllJournals(ctx, brokerClient, listRequest)
	mbp.Must(err, "failed to resolved journals from selector", cmd.Selector)

	var doneCounter int32
	var doneChan = make(chan struct{})
	var writer = newLockedWriter(os.Stdout)
	for _, journal := range listResp.Journals {
		go readJournal(readjournalOpts{
			doneChan: doneChan,
			ctx:      ctx,
			spec:     journal.Spec,
			client:   brokerClient,
			blocking: cmd.Blocking,
			offset:   cmd.Offset,
			writer:   writer,
		})
		doneCounter++
	}

	for {
		<-doneChan
		if doneCounter--; doneCounter == 0 {
			return nil
		}
	}
}

type lockedWriter struct {
	sync.Mutex
	w io.Writer
}

func newLockedWriter(w io.Writer) *lockedWriter {
	return &lockedWriter{w: w}
}

func (l *lockedWriter) writeN(reader io.Reader, n int64) (int64, error) {
	l.Lock()
	defer l.Unlock()

	return io.CopyN(l.w, reader, n)
}

type readjournalOpts struct {
	ctx      context.Context
	spec     pb.JournalSpec
	client   pb.RoutedJournalClient
	blocking bool
	offset   int64
	writer   *lockedWriter
	doneChan chan<- struct{}
}

func readJournal(opts readjournalOpts) {
	var req = pb.ReadRequest{
		Journal: opts.spec.Name,
		Offset:  opts.offset,
		Block:   opts.blocking,
	}
	var reader = client.NewReader(opts.ctx, opts.client, req)
	var bufferedReader = bufio.NewReader(reader)
	for {
		// BufferedReader can not be used here as this is treated as a noop.
		var _, err = reader.Read(nil)
		switch err {
		case client.ErrOffsetJump:
			// Offset jumps means that the offset is set to -1, or data has been removed from
			// a journal. Relevant fragment metadata should be available.
			break
		case client.ErrOffsetNotYetAvailable:
			// The error is returned when we have reached the writehead of a journal when blocking is not set.
			// Signal that reading from this journal is finished.
			opts.doneChan <- struct{}{}
			return
		default:
			mbp.Must(err, "error reading fragment")
		}

		if reader.Response.Fragment == nil {
			log.Panic(errors.New("expected fragment metadata but it was not available"))
			return
		}
		var numBytesToWrite = reader.Response.Fragment.End - reader.Request.Offset
		_, err = opts.writer.writeN(bufferedReader, numBytesToWrite)
		mbp.Must(err, "error writing fragment")
	}
}
