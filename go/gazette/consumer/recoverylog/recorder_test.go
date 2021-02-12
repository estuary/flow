package recoverylog

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"os"
	"testing"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

type RecorderSuite struct{}

func (s *RecorderSuite) TestNewFile(c *gc.C) {
	var ajc, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(aRecoveryLog, fsm, anAuthor, "/strip", ajc)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/path/to/file")
	var aa = rec.Barrier(nil)
	c.Check(aa.Request().CheckRegisters, gc.Equals, rec.checkRegisters) // CheckRegisters passed through.
	<-aa.Done()                                                         // |rec| observes operation commit.
	rec.RecordCreate("/strip/other/file")

	var op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(1))
	c.Check(op.Author, gc.Equals, anAuthor)
	c.Check(op.Create.Path, gc.Equals, "/path/to/file")

	op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Author, gc.Equals, anAuthor)
	c.Check(op.Create.Path, gc.Equals, "/other/file")

	// Expect two LiveNodes, with increasing FirstOffset (as the commit completed between operations).
	var hints, _ = rec.BuildHints()
	c.Check(hints, gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 1, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 1, FirstOffset: offset, FirstChecksum: 0x00000000, LastSeqNo: 1}}},
			{Fnode: 2, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 2, FirstOffset: offset + 32, FirstChecksum: 0x568e8e4e, LastSeqNo: 2}}},
		},
	})
}

func (s *RecorderSuite) TestDeleteFile(c *gc.C) {
	var ajc, _, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(aRecoveryLog, fsm, anAuthor, "/strip", ajc)

	rec.RecordCreate("/strip/path/to/file")
	rec.RecordRemove("/strip/path/to/file")

	_ = s.parseOp(c, br)
	var op = s.parseOp(c, br)

	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/path/to/file")

	// Expect no LiveNodes remain.
	var hints, _ = rec.BuildHints()
	c.Check(hints, gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
	})
}

func (s *RecorderSuite) TestOverwriteExistingFile(c *gc.C) {
	var ajc, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(aRecoveryLog, fsm, anAuthor, "/strip", ajc)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/path/to/file")
	<-rec.Barrier(nil).Done()               // |rec| observes operation commit.
	rec.RecordCreate("/strip/path/to/file") // Overwrites existing file.

	_ = s.parseOp(c, br) // Creation of Fnode 1.

	// Expect unlink of Fnode 1 from target path.
	var op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/path/to/file")

	// Expect create of Fnode 3 at target path.
	op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Create.Path, gc.Equals, "/path/to/file")

	// Expect one LiveNode of Fnode 3.
	var hints, _ = rec.BuildHints()
	c.Check(hints, gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 3, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 3, FirstOffset: offset + 32, FirstChecksum: 0x8ee45505, LastSeqNo: 3}}},
		},
	})
}

func (s *RecorderSuite) TestLinkFile(c *gc.C) {
	var ajc, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(aRecoveryLog, fsm, anAuthor, "/strip", ajc)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/path/to/file")
	<-rec.Barrier(nil).Done() // |rec| observes operation commit.
	rec.RecordLink("/strip/path/to/file", "/strip/linked")

	_ = s.parseOp(c, br) // Creation of Fnode 1.

	var op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Link.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Link.Path, gc.Equals, "/linked")

	// Expect one LiveNode.
	var hints, _ = rec.BuildHints()
	c.Check(hints, gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 1, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 1, FirstOffset: offset, FirstChecksum: 0x00000000, LastSeqNo: 2}}},
		},
	})
}

func (s *RecorderSuite) TestRenameTargetExists(c *gc.C) {
	var ajc, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(aRecoveryLog, fsm, anAuthor, "/strip", ajc)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/target/path")
	<-rec.Barrier(nil).Done() // |rec| observes operation commit.
	rec.RecordCreate("/strip/source/path")

	// Exercise handling for duplicate '//' prefixes.
	rec.RecordRename("/strip//source/path", "/strip/target/path")

	_ = s.parseOp(c, br) // Creation of Fnode 1.
	_ = s.parseOp(c, br) // Creation of Fnode 2.

	// Expect unlink of Fnode 1 from target path.
	var op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/target/path")

	// Expect link of Fnode 2 to target path.
	op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(4))
	c.Check(op.Link.Fnode, gc.Equals, Fnode(2))
	c.Check(op.Link.Path, gc.Equals, "/target/path")

	// Expect unlink of Fnode 2 from source path.
	op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(5))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(2))
	c.Check(op.Unlink.Path, gc.Equals, "/source/path")

	// Expect only Fnode 2 in LiveNodes.
	var hints, _ = rec.BuildHints()
	c.Check(hints, gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 2, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 2, FirstOffset: offset + 31, FirstChecksum: 0x5ccbeeaa, LastSeqNo: 5}}},
		},
	})
}

func (s *RecorderSuite) TestRenameTargetIsNew(c *gc.C) {
	var ajc, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(aRecoveryLog, fsm, anAuthor, "/strip", ajc)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/source/path")
	<-rec.Barrier(nil).Done() // |rec| observes operation commit.
	rec.RecordRename("/strip/source/path", "/strip//target/path")

	_ = s.parseOp(c, br) // Creation of Fnode 1.

	// Expect link of Fnode 1 to target path.
	var op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Link.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Link.Path, gc.Equals, "/target/path")

	// Expect unlink of Fnode 1 from source path.
	op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/source/path")

	var hints, _ = rec.BuildHints()
	c.Check(hints, gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 1, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 1, FirstOffset: offset, FirstChecksum: 0x00000000, LastSeqNo: 3}}},
		},
	})
}

func (s *RecorderSuite) TestFileAppends(c *gc.C) {
	var ajc, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(aRecoveryLog, fsm, anAuthor, "/strip", ajc)
	var offset = r.AdjustedOffset(br)

	var f = FileRecorder{
		Recorder: rec,
		Fnode:    rec.RecordCreate("/strip/source/path"),
	}
	f.RecordWrite([]byte("first-write"))
	f.RecordWrite([]byte(""))
	f.RecordWrite([]byte("second-write"))

	_ = s.parseOp(c, br) // Creation of Fnode 1.

	var op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Write.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Write.Length, gc.Equals, int64(11))
	c.Check(op.Write.Offset, gc.Equals, int64(0))
	c.Check(s.readLen(c, op.Write.Length, br), gc.Equals, "first-write")

	op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Write.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Write.Length, gc.Equals, int64(0))
	c.Check(op.Write.Offset, gc.Equals, int64(11))
	c.Check(s.readLen(c, op.Write.Length, br), gc.Equals, "")

	op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(4))
	c.Check(op.Write.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Write.Length, gc.Equals, int64(12))
	c.Check(op.Write.Offset, gc.Equals, int64(11))
	c.Check(s.readLen(c, op.Write.Length, br), gc.Equals, "second-write")

	var hints, _ = rec.BuildHints()
	c.Check(hints, gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 1, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 1, FirstOffset: offset, FirstChecksum: 0x00000000, LastSeqNo: 4}}},
		},
	})
}

func (s *RecorderSuite) TestPropertyUpdate(c *gc.C) {
	var ajc, _, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var dir, err = ioutil.TempDir("", "recorder-property-update")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir)

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(aRecoveryLog, fsm, anAuthor, dir, ajc)

	// Properties are updated when a file is renamed to a property path.
	// Recorder extracts content directly from the target path.
	rec.RecordCreate(dir + "/tmp_file")
	c.Assert(ioutil.WriteFile(dir+"/IDENTITY", []byte("value"), 0666), gc.IsNil)
	rec.RecordRename(dir+"/tmp_file", dir+"/IDENTITY")

	_ = s.parseOp(c, br) // Creation of Fnode 1.

	// Expect a property update.
	var op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Property.Path, gc.Equals, "/IDENTITY")
	c.Check(op.Property.Content, gc.Equals, "value")

	// Expect unlink of Fnode 1.
	op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/tmp_file")

	// Property is tracked under fsm.Properties and produced into FSMHints.
	var hints, _ = rec.BuildHints()
	c.Check(hints, gc.DeepEquals, FSMHints{
		Log:       aRecoveryLog,
		LiveNodes: nil,
		Properties: []Property{
			{
				Path:    "/IDENTITY",
				Content: "value",
			},
		},
	})
}

func (s *RecorderSuite) TestMixedNewFileWriteAndDelete(c *gc.C) {
	var ajc, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(aRecoveryLog, fsm, anAuthor, "/strip", ajc)
	var offset = r.AdjustedOffset(br)

	// The first Fnode is unlinked prior to log end, and is not tracked in hints.
	rec.RecordCreate("/strip/delete/path")
	<-rec.Barrier(nil).Done() // |rec| observes operation commit.
	(&FileRecorder{Recorder: rec, Fnode: rec.RecordCreate("/strip/a/path")}).
		RecordWrite([]byte("file-write"))
	rec.RecordRemove("/strip/delete/path")

	var op = s.parseOp(c, br)         // Creation of Fnode 1.
	op = s.parseOp(c, br)             // Creation of Fnode 2.
	op = s.parseOp(c, br)             // Write to Fnode 2.
	s.readLen(c, op.Write.Length, br) // Write content.
	op = s.parseOp(c, br)             // Deletion of Fnode 1.

	// Expect Fnode 2 (only) is included in LiveNodes, with its write.
	var hints, _ = rec.BuildHints()
	c.Check(hints, gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 2, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 2, FirstChecksum: 0x950c2859, FirstOffset: offset + 31, LastSeqNo: 3}}}},
	})
}

func (s *RecorderSuite) TestContextCancellation(c *gc.C) {
	var (
		broker, cleanup = newBrokerAndLog(c)
		ctx, cancel     = context.WithCancel(context.Background())
		rjc             = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
		ajc             = client.NewAppendService(ctx, rjc)
		fsm, _          = NewFSM(FSMHints{Log: aRecoveryLog})
		rec             = NewRecorder(aRecoveryLog, fsm, anAuthor, "/strip", ajc)
	)
	defer cleanup()
	cancel()

	// Expect operations continue to sequence, but their appends are aborted
	// immediately due to the cancellation. We block on barrier.Done to
	// ensure we're also exercising Recorder inspections of |recentTxn|.
	for _, n := range []string{"/strip/file2", "/strip/file3"} {
		rec.RecordCreate(n)
		var barrier = rec.Barrier(nil)
		<-barrier.Done()
		c.Check(barrier.Err(), gc.Equals, context.Canceled)
	}

	var _, err = rec.BuildHints()
	c.Check(err, gc.Equals, context.Canceled)

	broker.Tasks.Cancel()
	c.Check(broker.Tasks.Wait(), gc.IsNil)
}

func (s *RecorderSuite) TestRandomAuthorGeneration(c *gc.C) {
	c.Check(NewRandomAuthor(), gc.Not(gc.Equals), NewRandomAuthor()) // No two are alike.
	c.Check(NewRandomAuthor(), gc.Not(gc.Equals), Author(0))         // Zero is never returned.
}

func (s *RecorderSuite) parseOp(c *gc.C, br *bufio.Reader) RecordedOp {
	var frame, err = message.UnpackFixedFrame(br)
	c.Assert(err, gc.IsNil)

	var op RecordedOp
	c.Check(op.Unmarshal(frame[message.FixedFrameHeaderLength:]), gc.IsNil)

	return op
}

func (s *RecorderSuite) readLen(c *gc.C, length int64, br *bufio.Reader) string {
	var buf = make([]byte, length)

	var n, err = io.ReadFull(br, buf)
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, int(length))

	return string(buf)
}

func newBrokerLogAndReader(c *gc.C) (client.AsyncJournalClient, *client.Reader, *bufio.Reader, func()) {
	var (
		broker, cleanup = newBrokerAndLog(c)
		rjc             = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
		ajc             = client.NewAppendService(context.Background(), rjc)
		r               = client.NewReader(context.Background(), rjc, pb.ReadRequest{
			Journal: aRecoveryLog,
			Block:   true,
		})
		br = bufio.NewReader(r)
	)
	return ajc, r, br, func() {
		broker.Tasks.Cancel()

		// Expect we read a closing error (all content consumed).
		var _, err = br.ReadByte()
		c.Check(err, gc.NotNil)

		cleanup()
	}
}

const (
	aRecoveryLog pb.Journal = "examples/integration-tests/recovery-log"
	anAuthor     Author     = 1234
)

var _ = gc.Suite(&RecorderSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
