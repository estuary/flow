package recoverylog

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	gc "github.com/go-check/check"
	"github.com/pkg/errors"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/message"
)

type PlaybackSuite struct{}

func (s *PlaybackSuite) TestPlayerReader(c *gc.C) {
	var broker, cleanup = newBrokerAndLog(c)
	defer cleanup()

	var ctx = context.Background()
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var ajc = client.NewAppendService(ctx, rjc)
	var pr = newPlayerReader(ctx, aRecoveryLog, ajc)

	var fixture = strings.Repeat("x", message.FixedFrameHeaderLength)

	// Expect we can use peek to asynchronously drive Peek operations.
	var peekCh = pr.peek()
	c.Check(pr.pendingPeek, gc.Equals, true)

	writeToLog(c, ctx, ajc, fixture+"!")
	c.Check(<-peekCh, gc.IsNil)
	pr.pendingPeek = false

	var str, err = pr.br.ReadString('!')
	c.Check(str, gc.Equals, fixture+"!")
	c.Check(err, gc.IsNil)

	// Read next bytes. Peek does not consume input, so multiple peeks
	// without an interleaving Read are trivially satisfied.
	writeToLog(c, ctx, ajc, fixture)
	c.Check(<-pr.peek(), gc.IsNil)
	pr.pendingPeek = false
	c.Check(<-pr.peek(), gc.IsNil)
	pr.pendingPeek = false

	b, err := pr.br.Peek(1)
	c.Check(b, gc.DeepEquals, []byte{'x'})
	c.Check(err, gc.IsNil)
	_, _ = pr.br.Discard(len(fixture))

	select {
	case err = <-pr.peek():
		c.Check(err, gc.ErrorMatches, "peek should never select")
	case <-time.After(time.Millisecond):
	}

	// Switch to non-blocking. Expect a pending peek was drained.
	pr.setBlocking(false)
	c.Check(pr.pendingPeek, gc.Equals, false)

	// Peek now returns an immediate error.
	c.Check(<-pr.peek(), gc.Equals, client.ErrOffsetNotYetAvailable)
	pr.pendingPeek = false

	writeToLog(c, ctx, ajc, fixture+"@")

	// As do reads.
	b, err = ioutil.ReadAll(pr.br)
	c.Check(b, gc.DeepEquals, []byte(fixture+"@"))
	c.Check(err, gc.Equals, client.ErrOffsetNotYetAvailable)

	// Switch back to blocking reads, and seek backwards in the log.
	pr.seek(aRecoveryLog, 3)
	pr.setBlocking(true)

	c.Check(<-pr.peek(), gc.IsNil)
	pr.pendingPeek = false

	str, err = pr.br.ReadString('@')
	c.Check(str, gc.Equals, fixture[3:]+"!"+fixture+fixture+"@")
	c.Check(err, gc.IsNil)

	select {
	case err = <-pr.peek():
		c.Check(err, gc.ErrorMatches, "peek should never select")
	case <-time.After(time.Millisecond):
	}
	pr.close() // Cancels read underway.

	c.Check(<-peekCh, gc.Equals, context.Canceled)
	var _, ok = <-peekCh // Expect channel is now closed.
	c.Check(ok, gc.Equals, false)
}

func (s *PlaybackSuite) TestStagingPaths(c *gc.C) {
	c.Check(stagedPath("/a/local/dir", 1234), gc.Equals,
		filepath.FromSlash("/a/local/dir/.fnodes/1234"))
}

func (s *PlaybackSuite) TestCreate(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	var b = poh.frame(newCreateOp("/a/path"))

	// Expect a backing file for Fnode 42 was allocated in the staging directory.
	c.Check(poh.apply(c, b), gc.IsNil)
	c.Check(poh.files[42].Name(), gc.Equals, stagedPath(poh.dir, 42))
}

func (s *PlaybackSuite) TestCreateErrors(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	// Place a pre-existing fixture at the expected staging path.
	c.Check(ioutil.WriteFile(stagedPath(poh.dir, 42), []byte("whoops"), 0644), gc.IsNil)

	var b = poh.frame(newCreateOp("/a/path"))

	// Expect that we fail with an aborting error.
	c.Check(poh.apply(c, b), gc.ErrorMatches, ".*file exists.*")
	c.Check(poh.files, gc.HasLen, 0)
}

func (s *PlaybackSuite) TestUnlink(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	// Precondition fixture: staged fnode exists with two links.
	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)
	c.Check(poh.apply(c, poh.frame(newLinkOp(42, "/other/path"))), gc.IsNil)
	c.Check(poh.apply(c, poh.frame(newCreateOp("/yet/another/path"))), gc.IsNil)

	// Apply first unlink.
	c.Check(poh.apply(c, poh.frame(newUnlinkOp(42, "/a/path"))), gc.IsNil)

	// Expect staged files still exist.
	c.Check(poh.files[42].Name(), gc.Equals, stagedPath(poh.dir, 42))
	c.Check(poh.files[44].Name(), gc.Equals, stagedPath(poh.dir, 44))

	// Second unlink, and unlink of 44's only link.
	c.Check(poh.apply(c, poh.frame(newUnlinkOp(42, "/other/path"))), gc.IsNil)
	c.Check(poh.apply(c, poh.frame(newUnlinkOp(44, "/yet/another/path"))), gc.IsNil)

	// Staged file was removed.
	c.Check(poh.files, gc.HasLen, 0)
}

func (s *PlaybackSuite) TestUnlinkCloseError(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)

	// Sneak in a Close() such that a successive close fails.
	_ = poh.files[42].Close()

	c.Check(poh.apply(c, poh.frame(newUnlinkOp(42, "/a/path"))),
		gc.ErrorMatches, ".*file already closed")

	c.Check(poh.files, gc.HasLen, 1)
}

func (s *PlaybackSuite) TestUnlinkRemoveError(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)

	if runtime.GOOS == "windows" {
		// Close the file such that a successive Close fails.
		c.Check(poh.files[42].Close(), gc.IsNil)

		c.Check(poh.apply(c, poh.frame(newUnlinkOp(42, "/a/path"))),
			gc.ErrorMatches, `reenactOperation.*: close .*: file already closed`)
	} else {
		// Sneak in a Remove such that a successive remove fails.
		c.Check(os.Remove(poh.files[42].Name()), gc.IsNil)

		c.Check(poh.apply(c, poh.frame(newUnlinkOp(42, "/a/path"))),
			gc.ErrorMatches, `reenactOperation.*: remove .*: no such file or directory`)
	}

	c.Check(poh.files, gc.HasLen, 1)
}

func (s *PlaybackSuite) TestUnlinkUntrackedError(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	// Fnode is untracked; expect a failure to apply an unlink is not treated as an error.
	c.Check(poh.skips(c, poh.frame(newUnlinkOp(15, "/a/path"))), gc.IsNil)
}

func (s *PlaybackSuite) TestWrites(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)
	c.Check(poh.skips(c, poh.frame(newCreateOp("/skipped/path"))), gc.IsNil) // Fnode 43 is skipped by hintsFixture.
	c.Check(poh.apply(c, poh.frame(newCreateOp("/other/path"))), gc.IsNil)   // Satisfy hints expectation.

	var getContent = func(fnode Fnode) string {
		var b, err = ioutil.ReadFile(stagedPath(poh.dir, fnode))
		c.Check(err, gc.IsNil)
		return string(b)
	}

	// Perform a few writes, occurring out of order and with repetition of write range.
	var b = poh.frame(newWriteOp(42, 5, 10))
	b = append(b, []byte("over-write")...)
	c.Check(poh.apply(c, b), gc.IsNil)
	c.Check(getContent(42), gc.Equals, "\x00\x00\x00\x00\x00over-write")

	b = poh.frame(newWriteOp(42, 0, 5))
	b = append(b, []byte("abcde")...)
	c.Check(poh.apply(c, b), gc.IsNil)
	c.Check(getContent(42), gc.Equals, "abcdeover-write")

	b = poh.frame(newWriteOp(42, 5, 10))
	b = append(b, []byte("0123456789")...)
	c.Check(poh.apply(c, b), gc.IsNil)
	c.Check(getContent(42), gc.Equals, "abcde0123456789")

	// Reader returns early EOF (before op.Length).
	b = poh.frame(newWriteOp(42, 15, 10))
	b = append(b, []byte("short")...)
	c.Check(errors.Cause(poh.apply(c, b)), gc.Equals, io.EOF)
	c.Check(getContent(42), gc.Equals, "abcde0123456789short")

	// Writes to skipped fnodes succeed without error, but are ignored.
	b = poh.frame(newWriteOp(43, 5, 10))
	b = append(b, []byte("0123456789")...)
	c.Check(poh.skips(c, b), gc.IsNil)
	var _, err = os.Stat(stagedPath(poh.dir, 43))
	c.Check(os.IsNotExist(err), gc.Equals, true)

	// Skipped Fnodes will also produce EOF on a short read.
	b = poh.frame(newWriteOp(43, 15, 10))
	b = append(b, []byte("short")...)
	c.Check(errors.Cause(poh.skips(c, b)), gc.Equals, io.EOF)
}

func (s *PlaybackSuite) TestUnderlyingWriteErrors(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)
	c.Check(poh.skips(c, poh.frame(newCreateOp("/skipped/path"))), gc.IsNil) // Satisfy hints expectation.
	c.Check(poh.apply(c, poh.frame(newCreateOp("/other/path"))), gc.IsNil)   // Satisfy hints expectation.

	// Create a fixture such that underlying Write attempts return an error.
	c.Assert(poh.files[42].Close(), gc.IsNil)
	var readOnlyFile, _ = os.Open(stagedPath(poh.dir, 42))
	poh.files[42] = readOnlyFile

	var b = poh.frame(newWriteOp(42, 0, 5))
	b = append(b, []byte("abcde")...)
	c.Check(poh.apply(c, b), gc.ErrorMatches, `reenactOperation.*: write .*`)

	// Close so that a future Seek returns an error.
	_ = poh.files[42].Close()

	b = poh.frame(newWriteOp(42, 0, 5))
	b = append(b, []byte("abcde")...)
	c.Check(poh.apply(c, b), gc.ErrorMatches, `reenactOperation.*: seek .*`)
}

func (s *PlaybackSuite) TestWriteUntrackedError(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	// Expect writes to unknown Fnodes are consumed but ignored.
	var op = newWriteOp(15, 100, 10)
	op.SeqNo = 41 // Prior to first hinted Fnode.

	var b = poh.frame(op)
	b = append(b, []byte("0123456789")...)
	c.Check(poh.skips(c, b), gc.IsNil)

	// Writes of unknown Fnodes will still produce EOF on a short read.
	b = poh.frame(op)
	b = append(b, []byte("short")...)
	c.Check(errors.Cause(poh.skips(c, b)), gc.Equals, io.EOF)
}

func (s *PlaybackSuite) TestOperationDecode(c *gc.C) {
	var frame = func(op RecordedOp) []byte {
		var b, _ = message.EncodeFixedProtoFrame(&op, nil)
		return b
	}

	var parts = [][]byte{
		frame(newCreateOp("/a/path")),
		[]byte("... invalid data ..."),
		frame(newWriteOp(123, 0, 6)),
		frame(newCreateOp("/other/path")),
		frame(newCreateOp("/fin")),
	}
	var expect = []struct {
		err error
		op  RecordedOp
	}{
		{op: newCreateOp("/a/path")},
		{err: message.ErrDesyncDetected},
		{op: newWriteOp(123, 0, 6)},
		{op: newCreateOp("/other/path")},
		{op: newCreateOp("/fin")},
	}

	var br = bufio.NewReader(bytes.NewReader(bytes.Join(parts, nil)))

	var offset int
	for i, exp := range expect {
		var op, frame, err = decodeOperation(br, aRecoveryLog, int64(offset))
		c.Check(frame, gc.DeepEquals, parts[i])
		c.Check(err, gc.Equals, exp.err)

		var wlen int64
		if exp.op.Write != nil {
			wlen = exp.op.Write.Length
		}

		exp.op.FirstOffset = int64(offset)
		exp.op.LastOffset = int64(offset+len(parts[i])) + wlen
		exp.op.Log = aRecoveryLog
		c.Check(op, gc.DeepEquals, exp.op)

		offset += len(parts[i])
	}
}

func (s *PlaybackSuite) TestMakeLive(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)
	c.Check(poh.skips(c, poh.frame(newCreateOp("/skipped/path"))), gc.IsNil)
	c.Check(poh.apply(c, poh.frame(newCreateOp("/another/path"))), gc.IsNil)
	c.Check(poh.apply(c, poh.frame(newLinkOp(42, "/linked/path"))), gc.IsNil)
	c.Check(poh.skips(c, poh.frame(newUnlinkOp(43, "/skipped/path"))), gc.IsNil)

	c.Check(makeLive(poh.dir, poh.fsm, poh.files), gc.IsNil)

	var expect = func(path string, exists bool) {
		var _, err = os.Stat(path)
		if exists {
			c.Check(err, gc.IsNil)
		} else {
			c.Check(os.IsNotExist(err), gc.Equals, true)
		}
	}
	// Expect staging directory has been removed.
	expect(filepath.Join(poh.dir, fnodeStagingDir), false)

	// Expect files have been linked into final locations.
	expect(filepath.Join(poh.dir, "a/path"), true)
	expect(filepath.Join(poh.dir, "another/path"), true)
	expect(filepath.Join(poh.dir, "linked/path"), true)
	expect(filepath.Join(poh.dir, "skipped/path"), false)

	// Expect property file was written.
	var b, err = ioutil.ReadFile(filepath.Join(poh.dir, "property/path"))
	c.Check(err, gc.IsNil)
	c.Check(string(b), gc.Equals, "prop-value")
}

func (s *PlaybackSuite) TestPlayWithFinishAtWriteHead(c *gc.C) {
	var broker, cleanup = newBrokerAndLog(c)
	defer cleanup()

	var ctx = context.Background()
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var ajc = client.NewAppendService(ctx, rjc)
	writeToLog(c, ctx, ajc, "irrelevant preceding content")

	var dir, err = ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir)

	recFSM, err := NewFSM(FSMHints{Log: aRecoveryLog})
	c.Assert(err, gc.IsNil)

	// Start a Recorder, and produce a set of initial hints.
	var rec = NewRecorder(aRecoveryLog, recFSM, anAuthor, "/strip", ajc)
	var f = FileRecorder{Recorder: rec, Fnode: rec.RecordCreate("/strip/foo/bar")}
	var hints, _ = rec.BuildHints()

	// Record more content not captured in |hints|.
	f.RecordWrite([]byte("hello"))
	(&FileRecorder{Recorder: rec, Fnode: rec.RecordCreate("/strip/baz")}).
		RecordWrite([]byte("bing"))
	f.RecordWrite([]byte(" world"))

	// Model a bad, interleaved raw write. We expect this can never actually happen,
	// but want playback to be resilient should it occur.
	writeToLog(c, ctx, ajc, "... garbage data ...")
	f.RecordWrite([]byte("!"))
	<-rec.Barrier(nil).Done() // Flush all recorded ops.

	// Start a Player from the initial |hints|.
	var player = NewPlayer()
	go func() {
		c.Check(player.Play(context.Background(), hints, dir, ajc), gc.IsNil)
	}()
	// Expect we recover all content.
	player.FinishAtWriteHead()
	<-player.Done()

	c.Check(player.Resolved.FSM, gc.NotNil)

	expectFileContent(c, dir+"/foo/bar", "hello world!")
	expectFileContent(c, dir+"/baz", "bing")
}

func (s *PlaybackSuite) TestPlayWithInjectHandoff(c *gc.C) {
	var broker, cleanup = newBrokerAndLog(c)
	defer cleanup()

	var ctx = context.Background()
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var ajc = client.NewAppendService(ctx, rjc)
	writeToLog(c, ctx, ajc, "irrelevant preceding content")

	dir1, err := ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir1)

	dir2, err := ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir2)

	recFSM, err := NewFSM(FSMHints{Log: aRecoveryLog})
	c.Assert(err, gc.IsNil)

	// Start a Recorder, and two Players from initial Recorder hints.
	var rec = NewRecorder(aRecoveryLog, recFSM, anAuthor, "/strip", ajc)
	var f = FileRecorder{Recorder: rec, Fnode: rec.RecordCreate("/strip/foo/bar")}
	var hints, _ = rec.BuildHints()

	// |handoffPlayer| will inject a hand-off noop to take ownership of the log.
	var handoffPlayer = NewPlayer()
	// |tailPlayer| will observe both |rec| and |handoffPlayer|.
	var tailPlayer = NewPlayer()

	go func() { c.Check(handoffPlayer.Play(context.Background(), hints, dir1, ajc), gc.IsNil) }()
	go func() { c.Check(tailPlayer.Play(context.Background(), hints, dir2, ajc), gc.IsNil) }()

	// Record more content. Also mix in bad, raw writes. These should never
	// actually happen, but Playback shouldn't break if they do.
	f.RecordWrite([]byte("hello"))
	<-rec.Barrier(nil).Done() // Flush.
	writeToLog(c, ctx, ajc, "... bad interleaved raw data ...")
	(&FileRecorder{Recorder: rec, Fnode: rec.RecordCreate("/strip/baz")}).
		RecordWrite([]byte("bing"))
	<-rec.Barrier(nil).Done() // Flush.

	// Clear checked Recorder registers to deliberately break the journal register
	// fencing mechanism, allowing |rec| to continue writes even after |handoffPlayer|
	// has injected it's no-op.
	rec.DisableRegisterChecks()

	// Begin a write which will race |handoffPlayer|'s first attempt to inject a no-op.
	var txn = ajc.StartAppend(pb.AppendRequest{Journal: aRecoveryLog}, nil)
	txn.Writer().WriteString("... more garbage ...")

	rec.process(newWriteOp(f.Fnode, f.Offset, 6), txn.Writer())
	_, _ = txn.Writer().Write([]byte(" world"))

	// Delay the release of |txn|. This will typically give |handoffPlayer| time
	// to read through the current write head and attempt an injection, which will
	// queue behind |txn|. |handoffPlayer| must retry after losing the race. If
	// |handoffPlayer| still manages to "win" the race (by reading through |txn|
	// before attempting it's own injection), it doesn't impact test correctness.
	time.AfterFunc(time.Millisecond*20, func() { txn.Release() })

	handoffPlayer.InjectHandoff(1337)
	<-handoffPlayer.Done()
	c.Check(handoffPlayer.Resolved.FSM, gc.NotNil)

	f.RecordWrite([]byte("final write of |rec|, ignored because |handoffPlayer| injected a handoff"))
	<-rec.Barrier(nil).Done() // Flush all recorded ops.

	tailPlayer.FinishAtWriteHead()
	<-tailPlayer.Done()
	c.Check(tailPlayer.Resolved.FSM, gc.NotNil)

	// Expect both players saw the same version of events, and recovered the same content.
	expectFileContent(c, handoffPlayer.Resolved.Dir+"/foo/bar", "hello world")
	expectFileContent(c, handoffPlayer.Resolved.Dir+"/baz", "bing")
	expectFileContent(c, tailPlayer.Resolved.Dir+"/foo/bar", "hello world")
	expectFileContent(c, tailPlayer.Resolved.Dir+"/baz", "bing")

	c.Check(handoffPlayer.Resolved.FSM.BuildHints(aRecoveryLog),
		gc.DeepEquals, tailPlayer.Resolved.FSM.BuildHints(aRecoveryLog))

	// Expect players have branched from |rec|'s view of history.
	hints, err = rec.BuildHints()
	c.Check(hints, gc.Not(gc.DeepEquals),
		tailPlayer.Resolved.FSM.BuildHints(aRecoveryLog))
	c.Check(err, gc.IsNil)

	// Expect that |handoffPlayer| updated the log's registers to place a
	// cooperative fence for its author.
	var aa = rec.Barrier(nil)
	<-aa.Done()
	c.Check(aa.Response().Registers, gc.DeepEquals, Author(1337).Fence())
}

func (s *PlaybackSuite) TestPlayWithUnusedHints(c *gc.C) {
	var broker, cleanup = newBrokerAndLog(c)
	defer cleanup()

	var ctx = context.Background()
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var ajc = client.NewAppendService(ctx, rjc)
	writeToLog(c, ctx, ajc, "irrelevant preceding content")

	var dir, err = ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir)

	recFSM, err := NewFSM(FSMHints{Log: aRecoveryLog})
	c.Assert(err, gc.IsNil)

	// Record some writes of valid files. Build hints, and tweak them by adding
	// an Fnode at an offset which was not actually recorded.
	var rec = NewRecorder(aRecoveryLog, recFSM, anAuthor, "/strip", ajc)
	(&FileRecorder{Recorder: rec, Fnode: rec.RecordCreate("/strip/foo")}).
		RecordWrite([]byte("bar"))
	(&FileRecorder{Recorder: rec, Fnode: rec.RecordCreate("/strip/baz")}).
		RecordWrite([]byte("bing"))

	// Tweak hints by adding an Fnode & offset which was not actually recorded.
	var hints, _ = rec.BuildHints()
	{
		var txn = ajc.StartAppend(pb.AppendRequest{Journal: aRecoveryLog}, nil) // Determine log head.
		c.Assert(txn.Release(), gc.IsNil)
		<-txn.Done()

		var offset = txn.Response().Commit.End
		var seqNo = hints.LiveNodes[1].Segments[0].LastSeqNo + 1

		hints.LiveNodes = append(hints.LiveNodes, FnodeSegments{
			Fnode: Fnode(seqNo),
			Segments: []Segment{
				{
					Author:      5678, // Use a different Author to prevent Segments from being merged.
					FirstSeqNo:  seqNo,
					LastSeqNo:   seqNo + 1,
					FirstOffset: offset,
				},
			},
		})
	}

	// Expect Play fails immediately, because the hinted segment lies outside
	// of the journal offset range.
	var player = NewPlayer()
	c.Check(player.Play(context.Background(), hints, dir, ajc), gc.ErrorMatches,
		`max write-head of examples/.* is \d+, vs hinted segment .*`)

	// Sequence another recorded op. It will be ignored (due to player hints),
	// but does serve to extend the log head beyond the invalid segment offset.
	rec.RecordRemove("/strip/foo")
	<-rec.Barrier(nil).Done() // Flush all recorded ops.

	// This time, Play must read the log but fails upon reaching its head with unused hints.
	player = NewPlayer()
	c.Check(player.Play(context.Background(), hints, dir, ajc), gc.ErrorMatches,
		`offset examples/.*:\d+ >= readThrough \d+, but FSM has unused hints; possible data loss`)
}

func (s *PlaybackSuite) TestPlayWithMultipleLogs(c *gc.C) {
	var broker, cleanup = newBrokerAndLog(c)
	defer cleanup()

	var oldLog pb.Journal = "examples/integration-tests/other-recovery-log"
	var authorA Author = 123
	var authorB Author = 456
	var authorC Author = 789

	brokertest.CreateJournals(c, broker,
		brokertest.Journal(pb.JournalSpec{Name: oldLog}))

	var ctx = context.Background()
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var ajc = client.NewAppendService(ctx, rjc)

	// Record initial segments into |oldLog|.
	recFSM, err := NewFSM(FSMHints{Log: oldLog})
	c.Assert(err, gc.IsNil)

	var rec = NewRecorder(oldLog, recFSM, authorA, "/strip", ajc)
	var f = FileRecorder{Recorder: rec, Fnode: rec.RecordCreate("/strip/old/file")}
	f.RecordWrite([]byte("old data"))
	<-rec.Barrier(nil).Done() // Flush all recorded ops.
	hints, err := rec.BuildHints()
	c.Check(err, gc.IsNil)

	// Run Player which recovers from |oldLog| hints
	dir1, err := ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir1)

	var player = NewPlayer()
	player.InjectHandoff(authorB)
	c.Check(player.Play(context.Background(), hints, dir1, ajc), gc.IsNil)
	c.Check(player.Resolved.Log, gc.Equals, oldLog)

	// Record more segments into |aRecoveryLog|.
	rec = NewRecorder(aRecoveryLog, player.Resolved.FSM, authorB, "/strip", ajc)
	f = FileRecorder{Recorder: rec, Fnode: rec.RecordCreate("/strip/new/file")}
	f.RecordWrite([]byte("new data"))
	<-rec.Barrier(nil).Done() // Flush all recorded ops.
	hints, err = rec.BuildHints()
	c.Check(err, gc.IsNil)

	// Run Player which recovers from both |oldLog| and |aRecoveryLog|.
	dir2, err := ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir2)

	player = NewPlayer()
	player.InjectHandoff(authorC)
	c.Check(player.Play(context.Background(), hints, dir2, ajc), gc.IsNil)
	c.Check(player.Resolved.Log, gc.Equals, aRecoveryLog)

	expectFileContent(c, dir2+"/old/file", "old data")
	expectFileContent(c, dir2+"/new/file", "new data")
}

func expectFileContent(c *gc.C, path, content string) {
	var b, err = ioutil.ReadFile(path)
	c.Check(err, gc.IsNil)
	c.Check(string(b), gc.Equals, content)
}

func hintsFixture() FSMHints {
	return FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 42, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 42, FirstOffset: 11111, LastSeqNo: 45}}},
			{Fnode: 44, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 44, FirstOffset: 22222, LastSeqNo: 44}}},
		},
		Properties: []Property{{Path: "/property/path", Content: "prop-value"}},
	}
}

// playOperationHelper encapsulates common arguments and usages
// to facilitate testing of the playOperation function.
type playOperationHelper struct {
	dir   string
	fsm   *FSM
	files fnodeFileMap
}

func newPlayOperationHelper(c *gc.C) playOperationHelper {
	var err error
	var poh = playOperationHelper{
		files: make(fnodeFileMap),
	}

	poh.dir, err = ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	c.Assert(preparePlayback(poh.dir), gc.IsNil)

	poh.fsm, err = NewFSM(hintsFixture())
	c.Assert(err, gc.IsNil)

	return poh
}

func (poh playOperationHelper) frame(op RecordedOp) []byte {
	if op.Author == 0 {
		op.Author = anAuthor
	}
	if op.SeqNo == 0 {
		op.SeqNo = poh.fsm.NextSeqNo
		op.Checksum = poh.fsm.NextChecksum
	}
	var b, err = message.EncodeFixedProtoFrame(&op, nil)
	if err != nil {
		panic(err)
	}
	return b
}

func (poh playOperationHelper) apply(c *gc.C, b []byte) error {
	return poh.playOp(c, b, anAuthor, true)
}
func (poh playOperationHelper) skips(c *gc.C, b []byte) error {
	return poh.playOp(c, b, anAuthor, false)
}

func (poh playOperationHelper) playOp(c *gc.C, b []byte, expectAuthor Author, expectApply bool) error {
	var br = bufio.NewReader(bytes.NewReader(b))

	var op, applied, err = playOperation(br, aRecoveryLog, 1234, poh.fsm, poh.dir, poh.files)

	c.Check(op.Author, gc.Equals, expectAuthor)
	c.Check(applied, gc.Equals, expectApply)

	if err == nil {
		c.Check(br.Buffered(), gc.Equals, 0) // |br| fully consumed.
	}
	return err
}

func (poh playOperationHelper) destroy(c *gc.C) {
	for _, f := range poh.files {
		f.Close()
	}
	c.Assert(os.RemoveAll(poh.dir), gc.IsNil)
}

func writeToLog(c *gc.C, ctx context.Context, cl pb.RoutedJournalClient, b string) {
	var w = client.NewAppender(ctx, cl, pb.AppendRequest{Journal: aRecoveryLog})
	var _, err = w.Write([]byte(b))

	if err == nil {
		err = w.Close()
	}
	c.Check(err, gc.IsNil)
}

func newBrokerAndLog(c *gc.C) (*brokertest.Broker, func()) {
	var etcd = etcdtest.TestClient()
	var broker = brokertest.NewBroker(c, etcd, "local", "broker")

	brokertest.CreateJournals(c, broker,
		brokertest.Journal(pb.JournalSpec{Name: aRecoveryLog}))

	return broker, func() {
		broker.Tasks.Cancel()
		c.Check(broker.Tasks.Wait(), gc.IsNil)
		etcdtest.Cleanup()
	}
}

var _ = gc.Suite(&PlaybackSuite{})

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
