package recoverylog

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/LiveRamp/gazette/v2/pkg/brokertest"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type RecorderSuite struct{}

func (s *RecorderSuite) TestNewFile(c *gc.C) {
	var bk, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(fsm, anAuthor, "/strip", bk)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/path/to/file")
	<-rec.WeakBarrier().Done() // |rec| observes operation commit.
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
	c.Check(rec.BuildHints(), gc.DeepEquals, FSMHints{
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
	var bk, _, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(fsm, anAuthor, "/strip", bk)

	rec.RecordCreate("/strip/path/to/file")
	rec.RecordRemove("/strip/path/to/file")

	_ = s.parseOp(c, br)
	var op = s.parseOp(c, br)

	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/path/to/file")

	// Expect no LiveNodes remain.
	c.Check(rec.BuildHints(), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
	})
}

func (s *RecorderSuite) TestOverwriteExistingFile(c *gc.C) {
	var bk, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(fsm, anAuthor, "/strip", bk)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/path/to/file")
	<-rec.WeakBarrier().Done()              // |rec| observes operation commit.
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
	c.Check(rec.BuildHints(), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 3, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 3, FirstOffset: offset + 32, FirstChecksum: 0x8ee45505, LastSeqNo: 3}}},
		},
	})
}

func (s *RecorderSuite) TestLinkFile(c *gc.C) {
	var bk, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(fsm, anAuthor, "/strip", bk)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/path/to/file")
	<-rec.WeakBarrier().Done() // |rec| observes operation commit.
	rec.RecordLink("/strip/path/to/file", "/strip/linked")

	_ = s.parseOp(c, br) // Creation of Fnode 1.

	var op = s.parseOp(c, br)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Link.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Link.Path, gc.Equals, "/linked")

	// Expect one LiveNode.
	c.Check(rec.BuildHints(), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 1, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 1, FirstOffset: offset, FirstChecksum: 0x00000000, LastSeqNo: 2}}},
		},
	})
}

func (s *RecorderSuite) TestRenameTargetExists(c *gc.C) {
	var bk, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(fsm, anAuthor, "/strip", bk)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/target/path")
	<-rec.WeakBarrier().Done() // |rec| observes operation commit.
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
	c.Check(rec.BuildHints(), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 2, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 2, FirstOffset: offset + 31, FirstChecksum: 0x5ccbeeaa, LastSeqNo: 5}}},
		},
	})
}

func (s *RecorderSuite) TestRenameTargetIsNew(c *gc.C) {
	var bk, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(fsm, anAuthor, "/strip", bk)
	var offset = r.AdjustedOffset(br)

	rec.RecordCreate("/strip/source/path")
	<-rec.WeakBarrier().Done() // |rec| observes operation commit.
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

	c.Check(rec.BuildHints(), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 1, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 1, FirstOffset: offset, FirstChecksum: 0x00000000, LastSeqNo: 3}}},
		},
	})
}

func (s *RecorderSuite) TestFileAppends(c *gc.C) {
	var bk, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(fsm, anAuthor, "/strip", bk)
	var offset = r.AdjustedOffset(br)

	var handle = rec.RecordCreate("/strip/source/path")
	handle.RecordWrite([]byte("first-write"))
	handle.RecordWrite([]byte(""))
	handle.RecordWrite([]byte("second-write"))

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

	c.Check(rec.BuildHints(), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 1, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 1, FirstOffset: offset, FirstChecksum: 0x00000000, LastSeqNo: 4}}},
		},
	})
}

func (s *RecorderSuite) TestPropertyUpdate(c *gc.C) {
	var bk, _, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var dir, err = ioutil.TempDir("", "recorder-property-update")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir)

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(fsm, anAuthor, dir, bk)

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
	c.Check(rec.BuildHints(), gc.DeepEquals, FSMHints{
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
	var bk, r, br, cleanup = newBrokerLogAndReader(c)
	defer cleanup()

	var fsm, _ = NewFSM(FSMHints{Log: aRecoveryLog})
	var rec = NewRecorder(fsm, anAuthor, "/strip", bk)
	var offset = r.AdjustedOffset(br)

	// The first Fnode is unlinked prior to log end, and is not tracked in hints.
	rec.RecordCreate("/strip/delete/path")
	<-rec.WeakBarrier().Done() // |rec| observes operation commit.
	rec.RecordCreate("/strip/a/path").RecordWrite([]byte("file-write"))
	rec.RecordRemove("/strip/delete/path")

	var op = s.parseOp(c, br)         // Creation of Fnode 1.
	op = s.parseOp(c, br)             // Creation of Fnode 2.
	op = s.parseOp(c, br)             // Write to Fnode 2.
	s.readLen(c, op.Write.Length, br) // Write content.
	op = s.parseOp(c, br)             // Deletion of Fnode 1.

	// Expect Fnode 2 (only) is included in LiveNodes, with its write.
	c.Check(rec.BuildHints(), gc.DeepEquals, FSMHints{
		Log: aRecoveryLog,
		LiveNodes: []FnodeSegments{
			{Fnode: 2, Segments: []Segment{
				{Author: anAuthor, FirstSeqNo: 2, FirstChecksum: 0x950c2859, FirstOffset: offset + 31, LastSeqNo: 3}}}},
	})
}

func (s *RecorderSuite) TestRandomAuthorGeneration(c *gc.C) {
	var author, err = NewRandomAuthorID()
	c.Check(err, gc.IsNil)
	c.Check(author, gc.Not(gc.Equals), Author(0)) // Zero is never returned.
}

func (s *RecorderSuite) parseOp(c *gc.C, br *bufio.Reader) RecordedOp {
	var frame, err = message.FixedFraming.Unpack(br)
	c.Assert(err, gc.IsNil)

	var op RecordedOp
	c.Check(message.FixedFraming.Unmarshal(frame, &op), gc.IsNil)

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
	var etcd = etcdtest.TestClient()
	var broker = brokertest.NewBroker(c, etcd, "local", "broker")

	brokertest.CreateJournals(c, broker,
		brokertest.Journal(pb.JournalSpec{Name: aRecoveryLog}))

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = client.NewAppendService(context.Background(), rjc)

	var r = client.NewReader(context.Background(), rjc, pb.ReadRequest{
		Journal: aRecoveryLog,
		Block:   true,
	})
	var br = bufio.NewReader(r)

	return as, r, br, func() {
		broker.RevokeLease(c)

		// Expect read is remotely closed with all content consumed.
		var _, err = br.ReadByte()
		c.Check(err, gc.Equals, io.EOF)

		broker.WaitForExit()
		etcdtest.Cleanup()
	}
}

const (
	aRecoveryLog pb.Journal = "examples/integration-tests/recovery-log"
	anAuthor     Author     = 1234
)

var _ = gc.Suite(&RecorderSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
