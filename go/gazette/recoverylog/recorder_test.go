package recoverylog

import (
	"errors"
	"io"
	"net/http"

	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"

	"github.com/pippio/gazette/async"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
)

const kOpLog = journal.Name("a/journal")
const kPrefix = "/prefix"

type RecorderSuite struct {
	manifest *MockManifest
	header   *journal.MockHeader
	writer   *journal.MockWriter
	recorder Recorder
}

func (s *RecorderSuite) SetUpTest(c *gc.C) {
	kErrRetryInterval = 0

	s.manifest = &MockManifest{}
	s.header = &journal.MockHeader{}
	s.writer = &journal.MockWriter{}

	s.recorder = NewRecorder(s.manifest, len(kPrefix), "a/journal", s.header, s.writer)
}

func (s *RecorderSuite) TearDownTest(c *gc.C) {
	s.header.AssertExpectations(c)
	s.manifest.AssertExpectations(c)
	s.writer.AssertExpectations(c)
}

func (s *RecorderSuite) TestNewFile(c *gc.C) {
	// When obtaining journal head, return errors first to exercise handling.
	s.header.On("HeadJournalAt", journal.NewMark("a/journal", -1)).
		Return(nil, errors.New("error!")).Once()
	s.header.On("HeadJournalAt", journal.NewMark("a/journal", -1)).Return(&http.Response{
		Header: http.Header{gazette.WriteHeadHeader: []string{"zzbadzz"}}}, nil).Once()
	// Return a valid response.
	s.header.On("HeadJournalAt", journal.NewMark("a/journal", -1)).Return(&http.Response{
		Header: http.Header{gazette.WriteHeadHeader: []string{"12345"}}}, nil).Once()

	s.manifest.On("CreateFile", "/path/to/file", int64(12345)).
		Return(&FileRecord{Id: "file-id"}, nil).Once()

	// Return an error on initial write attempt.
	s.writer.On("Write", journal.Name("a/journal"), mock.AnythingOfType("[]uint8")).
		Return(make(async.Promise), errors.New("error!")).Once()
	// Return success. Validate that an RecordedOp.Alloc was written.
	s.writer.On("Write", journal.Name("a/journal"), mock.AnythingOfType("[]uint8")).
		Return(make(async.Promise), nil).Run(func(args mock.Arguments) {
		var op RecordedOp
		c.Assert(message.ParseExactFrame(&op, args.Get(1).([]byte)), gc.IsNil)

		c.Check(op.Alloc, gc.NotNil)
		c.Check(op.Alloc.Id, gc.Equals, "file-id")
	}).Once()

	s.recorder.NewWritableFile(kPrefix + "/path/to/file")
}

func (s *RecorderSuite) TestDeleteMultipleLinks(c *gc.C) {
	s.manifest.On("DeleteFile", "/path/to/file").Return(&FileRecord{
		Id:    "file-id",
		Links: map[string]struct{}{"remaining/path": {}},
	}, nil).Once()

	s.recorder.DeleteFile(kPrefix + "/path/to/file")
}

func (s *RecorderSuite) TestDeleteFinalLink(c *gc.C) {
	s.manifest.On("DeleteFile", "/path/to/file").
		Return(&FileRecord{Id: "file-id"}, nil).Once()

	// Return an error on initial write attempt.
	s.writer.On("Write", journal.Name("a/journal"), mock.AnythingOfType("[]uint8")).
		Return(make(async.Promise), errors.New("error!")).Once()
	// Return success. Validate that an RecordedOp.Dealloc was written.
	s.writer.On("Write", journal.Name("a/journal"), mock.AnythingOfType("[]uint8")).
		Return(make(async.Promise), nil).Run(func(args mock.Arguments) {
		var op RecordedOp
		c.Assert(message.ParseExactFrame(&op, args.Get(1).([]byte)), gc.IsNil)

		c.Check(op.Dealloc, gc.NotNil)
		c.Check(op.Dealloc.Id, gc.Equals, "file-id")
	}).Once()

	s.recorder.DeleteFile(kPrefix + "/path/to/file")
}

func (s *RecorderSuite) TestFileRename(c *gc.C) {
	s.manifest.On("RenameFile", "/source/path", "/target/path").Return(nil, nil).Once()

	s.recorder.RenameFile(kPrefix+"/source/path", kPrefix+"/target/path")
}

func (s *RecorderSuite) TestFileLink(c *gc.C) {
	s.manifest.On("LinkFile", "/source/path", "/target/path").Return(nil, nil).Once()

	s.recorder.LinkFile(kPrefix+"/source/path", kPrefix+"/target/path")
}

func (s *RecorderSuite) TestFileAppendAndSync(c *gc.C) {
	// File creation fixtures.
	s.header.On("HeadJournalAt", journal.NewMark("a/journal", -1)).Return(&http.Response{
		Header: http.Header{gazette.WriteHeadHeader: []string{"12345"}}}, nil).Once()
	s.manifest.On("CreateFile", "/path/to/file", int64(12345)).
		Return(&FileRecord{Id: "file-id"}, nil).Once()

	// Expect that file creation is recorded.
	s.writer.On("Write", journal.Name("a/journal"), mock.AnythingOfType("[]uint8")).
		Return(make(async.Promise), nil).Run(func(args mock.Arguments) {
		var op RecordedOp
		c.Assert(message.ParseExactFrame(&op, args.Get(1).([]byte)), gc.IsNil)

		c.Check(op.Alloc, gc.NotNil)
		c.Check(op.Alloc.Id, gc.Equals, "file-id")
	}).Once()

	file := s.recorder.NewWritableFile(kPrefix + "/path/to/file")

	// Expect a first log write. Starts at offset 0.
	s.writer.On("ReadFrom", journal.Name("a/journal"), mock.AnythingOfType("*io.multiReader")).
		Return(make(async.Promise), nil).Run(func(args mock.Arguments) {
		r := args.Get(1).(io.Reader)

		var op RecordedOp
		var b []byte

		_, err := message.Parse(&op, r, &b)
		c.Assert(err, gc.IsNil)

		c.Check(op.Write, gc.NotNil)
		c.Check(op.Write.Id, gc.Equals, "file-id")
		c.Check(op.Write.Offset, gc.Equals, int64(0))
		c.Check(op.Write.Length, gc.Equals, int64(4))

		r.Read(b[:4])
		c.Check(b[:4], gc.DeepEquals, []byte{0xf, 0xe, 0xe, 0xd})
	}).Once()

	file.Append([]byte{0xf, 0xe, 0xe, 0xd})

	// Expect a second write. First attempt fails, and is re-tried successfully.
	s.writer.On("ReadFrom", journal.Name("a/journal"), mock.AnythingOfType("*io.multiReader")).
		Return(make(async.Promise), errors.New("error!")).Once()
	s.writer.On("ReadFrom", journal.Name("a/journal"), mock.AnythingOfType("*io.multiReader")).
		Return(make(async.Promise), nil).Run(func(args mock.Arguments) {
		r := args.Get(1).(io.Reader)

		var op RecordedOp
		var b []byte

		_, err := message.Parse(&op, r, &b)
		c.Assert(err, gc.IsNil)

		c.Check(op.Write, gc.NotNil)
		c.Check(op.Write.Id, gc.Equals, "file-id")
		c.Check(op.Write.Offset, gc.Equals, int64(4))
		c.Check(op.Write.Length, gc.Equals, int64(5))

		r.Read(b[:5])
		c.Check(b[:5], gc.DeepEquals, []byte{0xb, 0xe, 0xe, 0xf, 0x0})
	}).Once()

	file.Append([]byte{0xb, 0xe, 0xe, 0xf, 0x0})

	// Expect a sync on file close. First attempt fails, and is retried.
	syncPromise := make(async.Promise)
	syncPromise.Resolve()
	s.writer.On("Write", journal.Name("a/journal"), []byte(nil)).
		Return(make(async.Promise), errors.New("error!")).Once()
	s.writer.On("Write", journal.Name("a/journal"), []byte(nil)).
		Return(syncPromise, nil)

	file.Close()

}

var _ = gc.Suite(&RecorderSuite{})
