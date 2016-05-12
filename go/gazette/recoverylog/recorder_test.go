package recoverylog

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
)

const kOpLog = journal.Name("a/journal")

type RecorderSuite struct {
	recorder  *Recorder
	tmpDir    string
	writes    *bytes.Buffer // Captured log writes.
	writeHead int64
	promise   chan struct{} // Returned promise fixture for captured writes.
}

func (s *RecorderSuite) SetUpTest(c *gc.C) {
	var err error
	s.tmpDir, err = ioutil.TempDir("", "recorder-suite")
	c.Assert(err, gc.IsNil)

	// Retain writes, defaulting to a resolved Ready promise for each one.
	s.writes = bytes.NewBuffer(nil)
	s.writeHead = 42
	s.promise = make(chan struct{})
	close(s.promise)

	s.recorder, err = NewRecorder(NewFSM(EmptyHints("a/journal")), len(s.tmpDir), s)
	c.Check(err, gc.IsNil)

	// Expect recorder initialized Offset to the current write head.
	c.Check(s.recorder.fsm.LogMark.Offset, gc.Equals, int64(42))
}

func (s *RecorderSuite) TearDownTest(c *gc.C) {
	// Expect the test consumed all framed operations.
	c.Check(s.writes.Len(), gc.Equals, 0)

	// After issuing a final write, expect the FSM offset is up to date with
	// collective length of all written frames.
	s.recorder.WriteBarrier()
	c.Check(s.recorder.fsm.LogMark.Offset, gc.Equals, s.writeHead)

	os.RemoveAll(s.tmpDir)
}

func (s *RecorderSuite) TestNewFile(c *gc.C) {
	s.recorder.NewWritableFile(s.tmpDir + "/path/to/file")

	op := s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(1))
	c.Check(op.Author, gc.Not(gc.Equals), Author(0))
	c.Check(op.Create.Path, gc.Equals, "/path/to/file")

	s.recorder.NewWritableFile(s.tmpDir + "/other/file")
	c.Check(s.recorder.fsm.LogMark.Offset, gc.Equals, int64(79))

	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Author, gc.Not(gc.Equals), Author(0))
	c.Check(op.Create.Path, gc.Equals, "/other/file")
}

func (s *RecorderSuite) TestDeleteFile(c *gc.C) {
	s.recorder.NewWritableFile(s.tmpDir + "/path/to/file")
	_ = s.parseOp(c)

	s.recorder.DeleteFile(s.tmpDir + "/path/to/file")
	op := s.parseOp(c)

	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/path/to/file")
}

func (s *RecorderSuite) TestOverwriteExistingFile(c *gc.C) {
	// Initial creation of target path.
	s.recorder.NewWritableFile(s.tmpDir + "/path/to/file")
	_ = s.parseOp(c)

	s.recorder.NewWritableFile(s.tmpDir + "/path/to/file")

	// Expect unlink of Fnode 1 from target path.
	op := s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/path/to/file")

	// Expect create of Fnode 3 at target path.
	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Create.Path, gc.Equals, "/path/to/file")
}

func (s *RecorderSuite) TestLinkFile(c *gc.C) {
	s.recorder.NewWritableFile(s.tmpDir + "/path/to/file")
	_ = s.parseOp(c)

	s.recorder.LinkFile(s.tmpDir+"/path/to/file", s.tmpDir+"/linked")
	op := s.parseOp(c)

	c.Check(op.Link.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Link.Path, gc.Equals, "/linked")
}

func (s *RecorderSuite) TestRenameTargetExists(c *gc.C) {
	s.recorder.NewWritableFile(s.tmpDir + "/target/path")
	_ = s.parseOp(c)
	s.recorder.NewWritableFile(s.tmpDir + "/source/path")
	_ = s.parseOp(c)

	// Excercise handling for duplicate '//' prefixes.
	s.recorder.RenameFile(s.tmpDir+"//source/path", s.tmpDir+"/target/path")

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
	s.recorder.NewWritableFile(s.tmpDir + "/source/path")
	_ = s.parseOp(c)

	s.recorder.RenameFile(s.tmpDir+"/source/path", s.tmpDir+"/target/path")

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
	handle := s.recorder.NewWritableFile(s.tmpDir + "/source/path")
	_ = s.parseOp(c)

	handle.Append([]byte("first-write"))

	op := s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Write.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Write.Length, gc.Equals, int64(11))
	c.Check(op.Write.Offset, gc.Equals, int64(0))
	c.Check(s.readLen(c, op.Write.Length), gc.Equals, "first-write")

	handle.Append([]byte(""))

	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Write.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Write.Length, gc.Equals, int64(0))
	c.Check(op.Write.Offset, gc.Equals, int64(11))
	c.Check(s.readLen(c, op.Write.Length), gc.Equals, "")

	handle.Append([]byte("second-write"))

	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(4))
	c.Check(op.Write.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Write.Length, gc.Equals, int64(12))
	c.Check(op.Write.Offset, gc.Equals, int64(11))
	c.Check(s.readLen(c, op.Write.Length), gc.Equals, "second-write")
}

func (s *RecorderSuite) TestPropertyUpdate(c *gc.C) {
	// Properties are updated when a file is renamed to a property path.
	s.recorder.NewWritableFile(s.tmpDir + "/tmp_file")
	_ = s.parseOp(c)

	// Recorder extracts content directly from the target path.
	c.Assert(ioutil.WriteFile(s.tmpDir+"/IDENTITY", []byte("value"), 0666), gc.IsNil)

	// Record rename into property path.
	s.recorder.RenameFile(s.tmpDir+"/tmp_file", s.tmpDir+"/IDENTITY")

	// Expect a property update.
	op := s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(2))
	c.Check(op.Property.Path, gc.Equals, "/IDENTITY")
	c.Check(op.Property.Content, gc.Equals, "value")

	// Expect unlink of Fnode 1.
	op = s.parseOp(c)
	c.Check(op.SeqNo, gc.Equals, int64(3))
	c.Check(op.Unlink.Fnode, gc.Equals, Fnode(1))
	c.Check(op.Unlink.Path, gc.Equals, "/tmp_file")

	// Property is tracked under fsm.Properties.
	c.Check(s.recorder.fsm.Properties, gc.DeepEquals,
		map[string]string{"/IDENTITY": "value"})
}

func (s *RecorderSuite) TestFileSync(c *gc.C) {
	handle := s.recorder.NewWritableFile(s.tmpDir + "/source/path")
	_ = s.parseOp(c)

	s.promise = make(chan struct{})
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
	close(s.promise)
	<-finished
}

func (s *RecorderSuite) TestHints(c *gc.C) {
	// The first Fnode is unlinked prior to log end, and is not tracked in hints.
	s.recorder.NewWritableFile(s.tmpDir + "/delete/path")

	// Expect hints will start from the next Fnode.
	expectChecksum := s.recorder.fsm.NextChecksum
	expectMark := s.recorder.fsm.LogMark

	s.recorder.NewWritableFile(s.tmpDir + "/a/path").Append([]byte("file-write"))
	s.recorder.DeleteFile(s.tmpDir + "/delete/path")

	// Expect that hints are produced for the current FSM state.
	c.Check(s.recorder.BuildHints(), gc.DeepEquals, FSMHints{
		LogMark:       expectMark,
		FirstChecksum: expectChecksum,
		FirstSeqNo:    2,
		Authors:       []AuthorRange{{ID: s.recorder.id, LastSeqNo: 4}},
	})

	s.writes.Reset()
}

func (s *RecorderSuite) parseOp(c *gc.C) RecordedOp {
	var op RecordedOp
	var frame []byte

	_, err := message.Parse(&op, s.writes, &frame)
	c.Check(err, gc.IsNil)

	return op
}

func (s *RecorderSuite) readLen(c *gc.C, length int64) string {
	buf := make([]byte, length)

	n, err := s.writes.Read(buf)
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, int(length))

	return string(buf)
}

// journal.Writer implementation
func (s *RecorderSuite) Write(log journal.Name, buf []byte) (*journal.AsyncAppend, error) {
	n, _ := s.writes.Write(buf)
	s.writeHead += int64(n)

	return &journal.AsyncAppend{
		Ready:        s.promise,
		AppendResult: journal.AppendResult{WriteHead: s.writeHead},
	}, nil
}

// journal.Writer implementation
func (s *RecorderSuite) ReadFrom(log journal.Name, r io.Reader) (*journal.AsyncAppend, error) {
	n, _ := s.writes.ReadFrom(r)
	s.writeHead += n

	return &journal.AsyncAppend{
		Ready:        s.promise,
		AppendResult: journal.AppendResult{WriteHead: s.writeHead},
	}, nil
}

var _ = gc.Suite(&RecorderSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
