package gazctlcmd

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdJournalRead struct {
	Selector       string `long:"selector" short:"l" required:"true" description:"Label selector of journals to read"`
	Block          bool   `long:"block" short:"b" description:"Do not exit on journal EOF; wait for new data until signaled"`
	Tail           bool   `long:"tail" description:"Start reading from the journal write-head (rather than offset 0)"`
	Output         string `long:"output" short:"o" default:"-" description:"Output file path. Use '-' for stdout"`
	OffsetsPath    string `long:"offsets" description:"Path from which initial journal offsets are read at startup"`
	OffsetsOutPath string `long:"offsets-out" description:"Path to which final journal offsets are written at exit"`
	FileRoot       string `long:"file-root" description:"Filesystem path which roots file:// fragment store"`
	FromUnix       int64  `long:"from" description:"Skip over fragments persisted before this time, in unix seconds since epoch"`

	pumpCh       chan pumpResult                   // Chan into which completed read pumps are sent.
	beginOffsets map[pb.Journal]int64              // Contents of initial --offsets.
	endOffsets   map[pb.Journal]int64              // Collected --offsets-out.
	cancelFns    map[pb.Journal]context.CancelFunc // CancelFuncs of active read pumps.
	output       *os.File                          // Output to which we're multiplexing reads.
	buffer       []byte                            // Buffer for copying to |output|
}

func init() {
	CommandRegistry.AddCommand("journals", "read", "Read journal contents", `
Read the contents of one or more journals.

A label --selector is required, and determines the set of journals which are read.
See "journals list --help" for details and examples of using journal selectors.

Matched journals are read concurrently, and their content is multiplexed into
the output file (or stdout). Content is copied to the output in whole-fragment
chunks, and so long as journal appends reflect whole message boundaries, this
command will also respect those boundaries in the merged output.

The --selector is evaluated both at startup and also periodically during
execution. As new journals are matched by the selector, and old ones stop
matching, corresponding read operations are started and stopped.

Journals are read until the write-head is reached (OFFSET_NOT_YET_AVAILABLE),
or gazctl is signaled (Ctrl-C or SIGTERM). If --block is specified, reads will
block upon reaching the write-head and thereafter stream content as it commits.

By default reads of journals begin at byte offset 0. If --offsets is specified,
it must exist and be a JSON mapping of journal name to read offset, and is used
to supply the initial read offsets for selected journals. --offsets-out in turn
is a path to which final journal offsets are written on exit (either due to
Ctrl-C or because all available content has been read). If --offsets and
--offsets-out are the same path, the existing offsets will be retained and
moved to a ".previous" suffix.

If --tail is specified and a journal is not present in --offsets, then its read
begins at its current write-head. This option generally only makes sense with
--block, but can also be used to initialize --offsets-out.

When running in high-volume production settings, be sure to set a non-zero
--broker.cache.size to significantly reduce broker load. Aside from controlling
the cache size itself, a non-zero value will:
* Disable broker-side proxying of requests, such that gazctl directly routes and
dispatches to applicable brokers, and
* Turn of broker proxy reads of fragment files in backing stores. Instead,
gazctl will read directly from stores via signed URLs that brokers provide.

When client-side reads of fragments stored to a 'file://' backing store are
desired, use the --file-root option to specify the directory of the store (eg,
this might be the local mount-point of a NAS array also used by brokers).

Examples:

# Read all available journal content:
gazctl journals read -l name=my/journal

# Streaming read from tail of current (and future) journals matching my-label:
gazctl journals read -l my-label --block --tail

# Read new content from matched journals since the last invocation. Dispatch to
# brokers in our same availability zone where available, and directly read
# persisted fragments from their respective stores:
echo "{}" > offsets.json # Must already exist.
gazctl journals read -l my-label -o output --offsets offsets.json --offsets-out offsets.json --broker.cache.size=256 --zone=us-east-1
`, &cmdJournalRead{})
}

func (cmd *cmdJournalRead) Execute([]string) error {
	startup(JournalsCfg.BaseConfig)

	if cmd.FileRoot != "" {
		client.InstallFileTransport(cmd.FileRoot)
	}
	cmd.pumpCh = make(chan pumpResult)
	cmd.beginOffsets = make(map[pb.Journal]int64)
	cmd.endOffsets = make(map[pb.Journal]int64)
	cmd.buffer = make([]byte, 32*1024)

	if cmd.OffsetsPath != "" {
		var fin, err = os.Open(cmd.OffsetsPath)
		mbp.Must(err, "failed to open offsets for reading")
		mbp.Must(json.NewDecoder(fin).Decode(&cmd.beginOffsets), "failed to decode offsets")
		mbp.Must(fin.Close(), "failed to close offsets")

		// If we're reading and writing to the same offsets path, move the input
		// offsets to a ".previous" suffix. This:
		//  * Preserves the prior values.
		//  * Makes it clear when a crash occurred (--offsets goes missing).
		//  * Lets us work on Windows (can't rename onto an existing file).
		if cmd.OffsetsPath == cmd.OffsetsOutPath {
			mbp.Must(os.Rename(cmd.OffsetsPath, cmd.OffsetsPath+".previous"),
				"failed to rename previous offsets")
		}
		// Copy to carry-through any offsets of --offsets which are not matched
		// to a journal on this invocation. They might match again in the future,
		// and it would be surprising to the user were we to forget them.
		for j, o := range cmd.beginOffsets {
			cmd.endOffsets[j] = o
		}
	}

	var err error
	if cmd.Output == "-" {
		cmd.output = os.Stdout
	} else {
		cmd.output, err = os.Create(cmd.Output)
		mbp.Must(err, "failed to open output file")
	}

	var listRequest pb.ListRequest
	listRequest.Selector, err = pb.ParseLabelSelector(cmd.Selector)
	mbp.Must(err, "failed to parse label selector", "selector", cmd.Selector)

	// Install a signal handler which cancels a top-level |ctx|.
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	var ctx, cancel = context.WithCancel(context.Background())
	go func() {
		<-signalCh
		cancel()
	}()

	// List and watch journals matching the --selector.
	var rjc = JournalsCfg.Broker.MustRoutedJournalClient(ctx)
	var list = client.NewWatchedList(ctx, rjc, listRequest, nil)
	mbp.Must(<-list.UpdateCh(), "failed to resolve label selector to journals")

	if len(list.List().Journals) == 0 {
		log.Warn("no journals were matched by the selector")
	}
	// Start initial set of read pumps.
	cmd.listRefreshed(ctx, rjc, list)

	// Service completed read pumps and |list| refreshes.
	for len(cmd.cancelFns) != 0 {
		select {
		case <-list.UpdateCh():
			cmd.listRefreshed(ctx, rjc, list)
		case r := <-cmd.pumpCh:
			cmd.pumpFinished(r.rr, r.err, r.nextCh)
		}
	}

	if cmd.output != os.Stdout {
		mbp.Must(cmd.output.Sync(), "failed to sync output file")
		mbp.Must(cmd.output.Close(), "failed to close output file")
	}
	if cmd.OffsetsOutPath != "" {
		// Use a temporary to atomically create the offsets.
		var fout, err = os.Create(cmd.OffsetsOutPath + ".temp")
		mbp.Must(err, "failed to open offsets for writing")
		mbp.Must(json.NewEncoder(fout).Encode(cmd.endOffsets), "failed to encode offsets")
		mbp.Must(fout.Sync(), "failed to sync offset file")
		mbp.Must(fout.Close(), "failed to close offset file")
		mbp.Must(os.Rename(cmd.OffsetsOutPath+".temp", cmd.OffsetsOutPath),
			"failed to rename output offsets from temporary")
	}

	return nil
}

func (cmd *cmdJournalRead) listRefreshed(ctx context.Context, rjc pb.RoutedJournalClient, list *client.WatchedList) {
	var (
		// Construct a new map of CancelFunc, to enable detection of
		// journals which were but are are no longer in |list|.
		prevFns = cmd.cancelFns
		nextFns = make(map[pb.Journal]context.CancelFunc)
	)
	for _, j := range list.List().Journals {
		if fn, ok := prevFns[j.Spec.Name]; ok {
			// Reader has already been started for this journal.
			nextFns[j.Spec.Name] = fn
			delete(prevFns, j.Spec.Name)
			continue
		}

		// If the journal's in |beginOffsets|, use that as our initial offset.
		// Otherwise if |Tail|, use -1. Else use zero.
		var offset, ok = cmd.beginOffsets[j.Spec.Name]
		if !ok && cmd.Tail {
			offset = -1
		}

		log.WithFields(log.Fields{
			"journal": j.Spec.Name,
			"offset":  offset,
		}).Info("read started")

		var subCtx, fn = context.WithCancel(ctx)

		go pumpReader(client.NewRetryReader(subCtx, rjc, pb.ReadRequest{
			Journal:      j.Spec.Name,
			Offset:       offset,
			Block:        cmd.Block,
			DoNotProxy:   !rjc.IsNoopRouter(),
			BeginModTime: cmd.FromUnix,
		}), cmd.pumpCh)
		nextFns[j.Spec.Name] = fn
	}
	// Cancel any prior readers which are no longer in |list|. Retain entries
	// for now; we'll remove them on seeing context.Cancelled in pumpFinish().
	for j, fn := range prevFns {
		nextFns[j] = fn
		fn()
	}
	cmd.cancelFns = nextFns
}

func (cmd *cmdJournalRead) pumpFinished(rr *client.RetryReader, err error, nextCh chan<- struct{}) {
	switch err {
	case context.Canceled, client.ErrOffsetNotYetAvailable:
		// This reader was cancelled, or we're running in non-blocking mode and
		// have reached the end of available journal content.

		log.WithFields(log.Fields{
			"journal": rr.Journal(),
			"offset":  rr.Offset(),
		}).Info("read finished")

		cmd.endOffsets[rr.Journal()] = rr.Offset()
		delete(cmd.cancelFns, rr.Journal())
		close(nextCh)

		return

	case nil, client.ErrOffsetJump:
		// Offset jumps means that the offset is set to -1, or data has been removed from
		// a journal and we've skipped passed the missing range. The read can continue.

		if log.GetLevel() >= log.DebugLevel {
			log.WithFields(log.Fields{
				"journal":        rr.Journal(),
				"offset":         rr.Offset(),
				"fragment.Begin": rr.Reader.Response.Fragment.Begin,
				"fragment.End":   rr.Reader.Response.Fragment.End,
				"fragment.URL":   rr.Reader.Response.FragmentUrl,
			}).Debug("read is ready")
		}

		// Read & copy out all ready content.
		var n = rr.Reader.Response.Fragment.End - rr.Reader.Request.Offset
		actual, err := io.CopyBuffer(cmd.output, io.LimitReader(rr, n), cmd.buffer)
		if actual != n && err == nil {
			panic("unexpected RetryReader EOF") // Its contract prohibits this case.
		}
		mbp.Must(err, "failed to write")

		nextCh <- struct{}{} // Start next pump.

	default:
		mbp.Must(err, "error reading fragment")
	}
}

type pumpResult struct {
	rr     *client.RetryReader
	err    error
	nextCh chan<- struct{}
}

func pumpReader(rr *client.RetryReader, into chan<- pumpResult) {
	var nextCh = make(chan struct{}, 1)

	for {
		var _, err = rr.Read(nil)
		into <- pumpResult{rr: rr, err: err, nextCh: nextCh}

		if _, ok := <-nextCh; !ok {
			return
		}
	}
}
