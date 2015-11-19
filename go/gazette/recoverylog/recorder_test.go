package recoverylog

import (
	"bytes"
	"io"
	"testing"
	"time"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/async"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
)

const kOpLog = journal.Name("a/journal")
const kPrefix = "/prefix"

type RecorderSuite struct {
	recorder *Recorder

	writes       *bytes.Buffer
	parsedOffset int64
	promise      async.Promise
}

func (s *RecorderSuite) SetUpTest(c *gc.C) {
	var err error
	s.recorder, err = NewRecorder(NewFSM(EmptyHints("a/journal")), len(kPrefix), s)
	c.Check(err, gc.IsNil)

	s.writes = bytes.NewBuffer(nil)
	s.parsedOffset = 0

	// Default to a resolved promise for writes.
	s.promise = make(async.Promise)
	s.promise.Resolve()
}

func (s *RecorderSuite) TestNewFile(c *gc.C) {
	s.recorder.NewWritableFile(kPrefix + "/path/to/file")

	op := s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(1))
	c.Check(op.Recorder, gc.Not(gc.Equals), uint32(0))
	c.Check(op.Create.Path, gc.Equals, "/path/to/file")

	// Expect the tracked offset was incremented by the written frame size.
	c.Check(s.recorder.fsm.LogMark.Offset, gc.Equals, s.parsedOffset)

	s.recorder.NewWritableFile(kPrefix + "/other/file")
	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Recorder, gc.Not(gc.Equals), uint32(0))
	c.Check(op.Create.Path, gc.Equals, "/other/file")
	c.Check(s.recorder.fsm.LogMark.Offset, gc.Equals, s.parsedOffset)
}

func (s *RecorderSuite) TestDeleteFile(c *gc.C) {
	s.recorder.NewWritableFile(kPrefix + "/path/to/file")
	_ = s.parseOp(c)

	s.recorder.DeleteFile(kPrefix + "/path/to/file")
	op := s.parseOp(c)

	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/path/to/file")
}

func (s *RecorderSuite) TestLinkFile(c *gc.C) {
	s.recorder.NewWritableFile(kPrefix + "/path/to/file")
	_ = s.parseOp(c)

	s.recorder.LinkFile(kPrefix+"/path/to/file", kPrefix+"/linked")
	op := s.parseOp(c)

	c.Check(op.Link.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Link.Path, gc.Equals, "/linked")
}

func (s *RecorderSuite) TestRenameTargetExists(c *gc.C) {
	s.recorder.NewWritableFile(kPrefix + "/target/path")
	_ = s.parseOp(c)
	s.recorder.NewWritableFile(kPrefix + "/source/path")
	_ = s.parseOp(c)

	// Excercise handling for duplicate '//' prefixes.
	s.recorder.RenameFile(kPrefix+"//source/path", kPrefix+"/target/path")

	// Expect unlink of Fnode 1 from target path.
	op := s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/target/path")

	// Expect link of Fnode 2 to target path.
	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(4))
	c.Check(op.Link.Fnode, gc.Equals, Fnode(2))
	c.Check(op.Link.Path, gc.Equals, "/target/path")

	// Expect unlink of Fnode 2 from source path.
	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(5))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(2))
	c.Check(op.Unlink.Path, gc.Equals, "/source/path")
}

func (s *RecorderSuite) TestRenameTargetIsNew(c *gc.C) {
	s.recorder.NewWritableFile(kPrefix + "/source/path")
	_ = s.parseOp(c)

	s.recorder.RenameFile(kPrefix+"/source/path", kPrefix+"/target/path")

	// Expect link of Fnode 1 to target path.
	op := s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Link.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Link.Path, gc.Equals, "/target/path")

	// Expect unlink of Fnode 1 from source path.
	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/source/path")
}

func (s *RecorderSuite) TestFileAppends(c *gc.C) {
	handle := s.recorder.NewWritableFile(kPrefix + "/source/path")
	_ = s.parseOp(c)

	handle.Append([]byte("first-write"))

	op := s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Write.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Write.Length, gc.Equals, int64(11))
	c.Check(op.Write.Offset, gc.Equals, int64(0))
	c.Check(s.readLen(c, op.Write.Length), gc.Equals, "first-write")
	// Expect the tracked offset was incremented by the frame + payload size.
	c.Check(s.recorder.fsm.LogMark.Offset, gc.Equals, s.parsedOffset)

	handle.Append([]byte(""))

	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Write.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Write.Length, gc.Equals, int64(0))
	c.Check(op.Write.Offset, gc.Equals, int64(11))
	c.Check(s.readLen(c, op.Write.Length), gc.Equals, "")
	c.Check(s.recorder.fsm.LogMark.Offset, gc.Equals, s.parsedOffset)

	handle.Append([]byte("second-write"))

	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(4))
	c.Check(op.Write.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Write.Length, gc.Equals, int64(12))
	c.Check(op.Write.Offset, gc.Equals, int64(11))
	c.Check(s.readLen(c, op.Write.Length), gc.Equals, "second-write")
	c.Check(s.recorder.fsm.LogMark.Offset, gc.Equals, s.parsedOffset)
}

func (s *RecorderSuite) TestFileSync(c *gc.C) {
	handle := s.recorder.NewWritableFile(kPrefix + "/source/path")
	_ = s.parseOp(c)

	s.promise = make(async.Promise)
	finished := make(chan struct{})

	go func() {
		handle.Sync()
		close(finished)
	}()

	time.Sleep(time.Millisecond)

	// Expect handle.Sync() hasn't returned yet.
	select {
	case <-finished:
		c.Fail()
	default:
	}
	s.promise.Resolve()
	<-finished
}

func (s *RecorderSuite) TestHints(c *gc.C) {
	// The first Fnode is unlinked prior to log end, and is not tracked in hints.
	s.recorder.NewWritableFile(kPrefix + "/delete/path")

	// Expect hints will start from the next Fnode.
	expectChecksum := s.recorder.fsm.NextChecksum
	expectMark := s.recorder.fsm.LogMark

	s.recorder.NewWritableFile(kPrefix + "/a/path").Append([]byte("file-write"))
	s.recorder.DeleteFile(kPrefix + "/delete/path")

	// Expect that hints are produced for the current FSM state.
	c.Check(s.recorder.BuildHints(), gc.DeepEquals, FSMHints{
		LogMark:       expectMark,
		FirstChecksum: expectChecksum,
		FirstSeqNo:    2,
		Recorders:     []RecorderRange{{ID: s.recorder.id, LastSeqNo: 4}},
		SkipWrites:    map[Fnode]bool{},
	})
}

func (s *RecorderSuite) parseOp(c *gc.C) RecordedOp {
	var op RecordedOp
	var frame []byte

	n, err := message.Parse(&op, s.writes, &frame)
	c.Check(err, gc.IsNil)

	s.parsedOffset += int64(n)
	return op
}

func (s *RecorderSuite) readLen(c *gc.C, length int64) string {
	buf := make([]byte, length)

	n, err := s.writes.Read(buf)
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, int(length))

	s.parsedOffset += int64(n)
	return string(buf)
}

// journal.Writer implementation
func (s *RecorderSuite) Write(log journal.Name, buf []byte) (async.Promise, error) {
	s.writes.Write(buf)
	return s.promise, nil
}

// journal.Writer implementation
func (s *RecorderSuite) ReadFrom(log journal.Name, r io.Reader) (async.Promise, error) {
	s.writes.ReadFrom(r)
	return s.promise, nil
}

var _ = gc.Suite(&RecorderSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
