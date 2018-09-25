package recoverylog

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/topic"
)

type PlaybackSuite struct{}

func (s *PlaybackSuite) TestPlayerReader(c *gc.C) {
	var ctx, cancelFn = context.WithCancel(context.Background())

	var getter = new(journal.MockGetter)
	var pr = newPlayerReader(ctx, journal.Mark{Journal: "a/journal", Offset: 100}, getter)

	getter.On("Get", journal.ReadArgs{Journal: "a/journal", Offset: 100, Context: pr.rr.Context}).
		Return(journal.ReadResult{Offset: 100}, ioutil.NopCloser(&fixtureReader{ctx: pr.rr.Context, n: 2})).Once()

	// Expect we can use peek to asynchronously drive Peek operations.
	c.Check(<-pr.peek(), gc.IsNil)
	pr.pendingPeek = false

	var b, err = pr.br.ReadByte()
	c.Check(b, gc.Equals, byte('x'))
	c.Check(err, gc.IsNil)

	// Read next byte. Peek does not consume input, so multiple peeks
	// without an interleaving Read are trivially satisfied.
	c.Check(<-pr.peek(), gc.IsNil)
	pr.pendingPeek = false
	c.Check(<-pr.peek(), gc.IsNil)
	pr.pendingPeek = false

	b, err = pr.br.ReadByte()
	c.Check(b, gc.Equals, byte('x'))
	c.Check(err, gc.IsNil)

	// The next peek operation blocks indefinitely, until cancelled.
	var peekRespCh = pr.peek()
	time.Sleep(time.Millisecond)
	pr.abort()

	// Verify we see an error (eg, Peek did not return until the context was cancelled).
	c.Check(<-peekRespCh, gc.Equals, context.Canceled)
	pr.pendingPeek = false

	// After abort, |peekResp| is closed, as is the reader.
	var _, ok = <-peekRespCh
	c.Check(ok, gc.Equals, false)
	c.Check(pr.rr.MarkedReader.ReadCloser, gc.IsNil)

	// Again. This time, abort in between blocking Peek operations (rather than during one).
	pr = newPlayerReader(ctx, journal.Mark{Journal: "a/journal", Offset: 100}, getter)

	getter.On("Get", journal.ReadArgs{Journal: "a/journal", Offset: 100, Context: pr.rr.Context}).
		Return(journal.ReadResult{Offset: 100}, ioutil.NopCloser(&fixtureReader{ctx: pr.rr.Context, n: 2})).Once()

	peekRespCh = pr.peek()
	c.Check(<-peekRespCh, gc.IsNil)
	pr.pendingPeek = false

	pr.abort()

	// Despite no Peek operation being underway, expect that after abort
	// |peekResp| is still closed, as is the current reader.
	_, ok = <-peekRespCh
	c.Check(ok, gc.Equals, false)
	c.Check(pr.rr.MarkedReader.ReadCloser, gc.IsNil)

	// Last time. Here, we cancel the parent context during a blocking Peek.
	pr = newPlayerReader(ctx, journal.Mark{Journal: "a/journal", Offset: 100}, getter)
	getter.On("Get", journal.ReadArgs{Journal: "a/journal", Offset: 100, Context: pr.rr.Context}).
		Return(journal.ReadResult{Offset: 100}, ioutil.NopCloser(&fixtureReader{ctx: pr.rr.Context, n: 0})).Once()

	cancelFn()

	// The parent cancellation was passed through to the Peek operation.
	c.Check(<-pr.peek(), gc.Equals, context.Canceled)
	pr.pendingPeek = false

	c.Check(pr.rr.MarkedReader.ReadCloser, gc.IsNil)
}

// fixtureReader returns one byte n times, and then blocks until cancelled.
type fixtureReader struct {
	n   int
	ctx context.Context
}

func (r *fixtureReader) Read(p []byte) (n int, err error) {
	if r.n -= 1; r.n >= 0 {
		p[0] = 'x'
		return 1, nil
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (s *PlaybackSuite) TestReadPrepCases(c *gc.C) {
	var ctx = context.Background()
	var mark = journal.Mark{Journal: "a/journal", Offset: 100}
	var getter = new(journal.MockGetter)

	var cases = []struct {
		offset                  int64
		prevBlock, nextBlock    bool
		expectNew, expectClosed bool
	}{
		// Block => block cases.
		{101, true, true, false, false}, // Read at next byte.
		{103, true, true, false, false}, // Seek within current fragment.
		{110, true, true, false, true},  // Seek requires new read op.

		// Block => non-block cases.
		{101, true, false, true, true},
		{103, true, false, true, true},
		{110, true, false, true, true},

		// Non-block => block cases.
		{101, false, true, false, false},
		{103, false, true, false, false},
		{110, false, true, false, true},

		// Non-block => non-block cases.
		{101, false, false, false, false},
		{103, false, false, false, false},
		{110, false, false, false, true},
	}

	for _, tc := range cases {
		var prIn = newPlayerReader(ctx, mark, getter)

		getter.On("Get",
			journal.ReadArgs{Journal: "a/journal", Offset: 100, Blocking: tc.prevBlock, Context: prIn.rr.Context}).
			Return(journal.ReadResult{
				Offset:   100,
				Fragment: journal.Fragment{Begin: 95, End: 105},
			}, ioutil.NopCloser(&fixtureReader{ctx: prIn.rr.Context, n: 10})).Once()

		prIn.rr.Blocking = tc.prevBlock
		prIn.br.ReadByte()

		var prOut = prepareRead(ctx, prIn, tc.offset, tc.nextBlock)

		c.Check(prOut != prIn, gc.Equals, tc.expectNew)
		c.Check(prOut.rr.MarkedReader.ReadCloser == nil, gc.Equals, tc.expectClosed)
		c.Check(prOut.rr.Mark.Offset, gc.Equals, tc.offset)
		c.Check(prOut.rr.Blocking, gc.Equals, tc.nextBlock)
	}
}

func (s *PlaybackSuite) TestStagingPaths(c *gc.C) {
	c.Check(stagedPath("/a/local/dir", 1234), gc.Equals, "/a/local/dir/.fnodes/1234")
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
	c.Check(poh.apply(c, b), gc.ErrorMatches, "open /.*/42: file exists")
	c.Check(poh.files, gc.HasLen, 0)
}

func (s *PlaybackSuite) TestUnlink(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	// Precondition fixture: staged fnode exists with two links.
	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)
	c.Check(poh.apply(c, poh.frame(newLinkOp(42, "/other/path"))), gc.IsNil)

	// Apply first unlink.
	c.Check(poh.apply(c, poh.frame(newUnlinkOp(42, "/a/path"))), gc.IsNil)

	// Expect staged file still exists.
	c.Check(poh.files[42].Name(), gc.Equals, stagedPath(poh.dir, 42))

	// Second unlink.
	c.Check(poh.apply(c, poh.frame(newUnlinkOp(42, "/other/path"))), gc.IsNil)

	// Staged file was removed.
	c.Check(poh.files, gc.HasLen, 0)
}

func (s *PlaybackSuite) TestUnlinkCloseError(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)

	// Sneak in a Close() such that a successive close fails.
	poh.files[42].Close()

	c.Check(poh.apply(c, poh.frame(newUnlinkOp(42, "/a/path"))),
		gc.ErrorMatches, ".*file already closed")

	c.Check(poh.files, gc.HasLen, 1)
}

func (s *PlaybackSuite) TestUnlinkRemoveError(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)

	// Sneak in a Remove() such that a successive remove fails.
	c.Check(os.Remove(poh.files[42].Name()), gc.IsNil)

	c.Check(poh.apply(c, poh.frame(newUnlinkOp(42, "/a/path"))),
		gc.ErrorMatches, "remove .*: no such file or directory")

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

	// Reader returns early EOF (before op.Length). Expect an ErrUnexpectedEOF.
	b = poh.frame(newWriteOp(42, 15, 10))
	b = append(b, []byte("short")...)
	c.Check(poh.apply(c, b), gc.Equals, io.ErrUnexpectedEOF)
	c.Check(getContent(42), gc.Equals, "abcde0123456789short")

	// Writes to skipped fnodes succeed without error, but are ignored.
	b = poh.frame(newWriteOp(43, 5, 10))
	b = append(b, []byte("0123456789")...)
	c.Check(poh.skips(c, b), gc.IsNil)
	var _, err = os.Stat(stagedPath(poh.dir, 43))
	c.Check(os.IsNotExist(err), gc.Equals, true)

	// Skipped Fnodes will also produce ErrUnexpectedEOF on a short read.
	b = poh.frame(newWriteOp(43, 15, 10))
	b = append(b, []byte("short")...)
	c.Check(poh.skips(c, b), gc.Equals, io.ErrUnexpectedEOF)
}

func (s *PlaybackSuite) TestUnderlyingWriteErrors(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)

	// Create a fixture such that underlying Write attempts return an error.
	var readOnlyFile, _ = os.Open(stagedPath(poh.dir, 42))
	poh.files[42] = readOnlyFile

	var b = poh.frame(newWriteOp(42, 0, 5))
	b = append(b, []byte("abcde")...)
	c.Check(poh.apply(c, b), gc.ErrorMatches, "^write .*")

	// Close so that a future Seek returns an error.
	poh.files[42].Close()

	b = poh.frame(newWriteOp(42, 0, 5))
	b = append(b, []byte("abcde")...)
	c.Check(poh.apply(c, b), gc.ErrorMatches, "^seek .*")
}

func (s *PlaybackSuite) TestWriteUntrackedError(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	// Expect writes to unknown Fnodes are consumed but ignored.
	var b = poh.frame(newWriteOp(15, 0, 10))
	b = append(b, []byte("0123456789")...)
	c.Check(poh.skips(c, b), gc.IsNil)

	// Writes of unknown Fnodes will still produce ErrUnexpectedEOF on a short read.
	b = poh.frame(newWriteOp(15, 15, 10))
	b = append(b, []byte("short")...)
	c.Check(poh.skips(c, b), gc.Equals, io.ErrUnexpectedEOF)
}

func (s *PlaybackSuite) TestOperationDecode(c *gc.C) {
	var parts = [][]byte{
		frameRecordedOp(newCreateOp("/a/path"), nil),
		[]byte("... invalid data ..."),
		frameRecordedOp(newWriteOp(123, 0, 6), nil),
		frameRecordedOp(newCreateOp("/other/path"), nil),
		frameRecordedOp(newCreateOp("/fin"), nil),
	}
	var expect = []struct {
		err error
		op  RecordedOp
	}{
		{op: newCreateOp("/a/path")},
		{err: topic.ErrDesyncDetected},
		{op: newWriteOp(123, 0, 6)},
		{op: newCreateOp("/other/path")},
		{op: newCreateOp("/fin")},
	}

	var br = bufio.NewReader(bytes.NewReader(bytes.Join(parts, nil)))

	var offset int
	for i, exp := range expect {
		var op, frame, err = decodeOperation(br, int64(offset))
		c.Check(frame, gc.DeepEquals, parts[i])

		if exp.err != nil {
			c.Check(err, gc.Equals, exp.err)
			c.Check(op, gc.DeepEquals, RecordedOp{})
		} else {
			c.Check(err, gc.IsNil)

			exp.op.FirstOffset = int64(offset)
			exp.op.LastOffset = int64(offset+len(parts[i])) + exp.op.GetWrite().GetLength()
			c.Check(op, gc.DeepEquals, exp.op)
		}
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

func (s *PlaybackSuite) TestErrWhenHintsRemainOnMakeLive(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)

	c.Check(makeLive(poh.dir, poh.fsm, poh.files), gc.ErrorMatches, "FSM has remaining unused hints.*")
}

func (s *PlaybackSuite) TestErrWhenHintsRemainWhenAttemptingCompletion(c *gc.C) {
	var poh = newPlayOperationHelper(c)
	defer poh.destroy(c)

	c.Check(poh.apply(c, poh.frame(newCreateOp("/a/path"))), gc.IsNil)

	_, err := attemptCompletion(poh.fsm, poh.dir, poh.files, false)
	c.Check(err, gc.ErrorMatches, "FSM has remaining unused hints.*")
}

func (s *PlaybackSuite) TestPlayerFinishAtWriteHead(c *gc.C) {
	var broker = journal.NewMemoryBroker()
	broker.Write(aRecoveryLog, []byte("irrelevant preceding content"))

	var dir, err = ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir)

	recFSM, err := NewFSM(FSMHints{Log: aRecoveryLog})
	c.Assert(err, gc.IsNil)

	// Start a Recorder, and then a Player from initial Recorder hints.
	var rec = NewRecorder(recFSM, anAuthor, 0, broker)
	var f = rec.NewWritableFile("foo/bar")

	player, err := NewPlayer(rec.BuildHints(), dir)
	c.Assert(err, gc.IsNil)

	go func() { c.Check(player.PlayContext(context.Background(), broker), gc.IsNil) }()

	// Record more content, giving |player| a chance to catch up.
	f.Append([]byte("hello"))
	time.Sleep(time.Millisecond)

	rec.NewWritableFile("baz").Append([]byte("bing"))
	f.Append([]byte(" world"))

	// Model a bad, interleaved raw write. We expect this can never actually happen,
	// but want playback to be resilient should it occur.
	broker.Write(aRecoveryLog, []byte("... garbage data ..."))
	f.Append([]byte("!"))

	// Expect we recover all content.
	var fsm = player.FinishAtWriteHead()
	c.Check(fsm, gc.NotNil)

	expectFileContent(c, dir+"/foo/bar", "hello world!")
	expectFileContent(c, dir+"/baz", "bing")
}

func (s *PlaybackSuite) TestPlayerInjectHandoff(c *gc.C) {
	var broker = journal.NewMemoryBroker()
	broker.Write(aRecoveryLog, []byte("irrelevant preceding content"))

	dir1, err := ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir1)

	dir2, err := ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir2)

	recFSM, err := NewFSM(FSMHints{Log: aRecoveryLog})
	c.Assert(err, gc.IsNil)

	// Start a Recorder, and two Players from initial Recorder hints.
	var rec = NewRecorder(recFSM, anAuthor, 0, broker)
	var f = rec.NewWritableFile("foo/bar")

	// |handoffPlayer| will inject a hand-off noop to take ownership of the log.
	handoffPlayer, err := NewPlayer(rec.BuildHints(), dir1)
	c.Assert(err, gc.IsNil)

	// |tailPlayer| will observe both |rec| and |handoffPlayer|.
	tailPlayer, err := NewPlayer(rec.BuildHints(), dir2)
	c.Assert(err, gc.IsNil)

	go func() { c.Check(handoffPlayer.PlayContext(context.Background(), broker), gc.IsNil) }()
	go func() { c.Check(tailPlayer.PlayContext(context.Background(), broker), gc.IsNil) }()

	// Record more content, giving both players a chance to catch up. Also mix in bad,
	// raw writes. These should never actually happen, but Playback shouldn't break if they do.
	f.Append([]byte("hello"))
	time.Sleep(time.Millisecond)

	broker.Write(aRecoveryLog, []byte("... bad interleaved raw data ..."))
	rec.NewWritableFile("baz").Append([]byte("bing"))

	// Queue a |rec| write which will precede |handoffPlayer|'s first attempt
	// to inject a no-op. |handoffPlayer| will retry after losing the race.
	broker.DelayWrites = true
	broker.Write(aRecoveryLog, []byte("... bad interleaved raw data ..."))
	f.Append([]byte(" world"))
	broker.DelayWrites = false

	var handoffFSM = handoffPlayer.InjectHandoff(1337)
	c.Check(handoffFSM, gc.NotNil)

	f.Append([]byte("final write of |rec|, ignored because |handoffPlayer| injected a handoff"))

	var tailFSM = tailPlayer.FinishAtWriteHead()
	c.Check(tailFSM, gc.NotNil)

	// Expect both players saw the same version of events, and recovered the same content.
	expectFileContent(c, dir1+"/foo/bar", "hello world")
	expectFileContent(c, dir1+"/baz", "bing")
	expectFileContent(c, dir2+"/foo/bar", "hello world")
	expectFileContent(c, dir2+"/baz", "bing")
	c.Check(handoffFSM.BuildHints(), gc.DeepEquals, tailFSM.BuildHints())

	// Expect players have branched from |rec|'s view of history.
	c.Check(rec.BuildHints(), gc.Not(gc.DeepEquals), tailFSM.BuildHints())
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
	return frameRecordedOp(op, nil)
}

func (poh playOperationHelper) apply(c *gc.C, b []byte) error { return poh.playOp(c, b, anAuthor, true) }
func (poh playOperationHelper) skips(c *gc.C, b []byte) error {
	return poh.playOp(c, b, anAuthor, false)
}

func (poh playOperationHelper) playOp(c *gc.C, b []byte, expectAuthor Author, expectApply bool) error {
	var br = bufio.NewReader(bytes.NewReader(b))

	var op, applied, err = playOperation(br, journal.Mark{}, poh.fsm, poh.dir, poh.files)

	c.Check(op.Author, gc.Equals, expectAuthor)
	c.Check(applied, gc.Equals, expectApply)

	if !applied && op.Write != nil {
		// playLog seeks the RetryReader in this case. For tests, we just read and discard the extra content.
		err = copyFixed(ioutil.Discard, br, op.Write.Length)
	}

	if err == nil {
		c.Check(br.Buffered(), gc.Equals, 0) // |br| fully consumed.
	}
	return err
}

func (poh playOperationHelper) destroy(c *gc.C) {
	c.Assert(os.RemoveAll(poh.dir), gc.IsNil)
}

var _ = gc.Suite(&PlaybackSuite{})
