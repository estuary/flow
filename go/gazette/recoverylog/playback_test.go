package recoverylog

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
)

type PlaybackSuite struct {
	localDir string
	player   *Player
}

func (s *PlaybackSuite) SetUpSuite(c *gc.C) {
	var err error
	s.localDir, err = ioutil.TempDir("", "playback-suite")
	c.Assert(err, gc.IsNil)
}

func (s *PlaybackSuite) TearDownSuite(c *gc.C) {
	os.RemoveAll(s.localDir)
}

func (s *PlaybackSuite) SetUpTest(c *gc.C) {
	var err error

	hintsFixture := FSMHints{
		Log: "a/recovery/log",
		LiveNodes: []HintedFnode{
			{Fnode: 42, Segments: []Segment{
				{Author: 100, FirstSeqNo: 42, LastSeqNo: 45}}},
			{Fnode: 44, Segments: []Segment{
				{Author: 100, FirstSeqNo: 44, LastSeqNo: 44}}},
		},
		Properties: []Property{{Path: "/property/path", Content: "prop-value"}},
	}
	s.player, err = PreparePlayback(hintsFixture, s.localDir)
	c.Check(err, gc.IsNil)
}

func (s *PlaybackSuite) TestPlayerInit(c *gc.C) {
	c.Check(s.player.localDir, gc.Equals, s.localDir)

	_, err := os.Stat(filepath.Join(s.localDir, kFnodeStagingDir))
	c.Check(err, gc.IsNil) // Staging directory was created.

	c.Check(s.player.fsm.LogMark, gc.Equals, journal.NewMark("a/recovery/log", -1))
	c.Check(s.player.backingFiles, gc.HasLen, 0)
}

func (s *PlaybackSuite) TestStagingPaths(c *gc.C) {
	c.Check(s.player.stagedPath(1234), gc.Equals,
		filepath.Join(s.localDir, kFnodeStagingDir, "1234"))
}

func (s *PlaybackSuite) TestCreate(c *gc.C) {
	c.Check(s.apply(c, s.frameCreate("/a/path")), gc.IsNil)

	// Expect a backing file for Fnode 42 was allocated in the staging directory.
	c.Check(s.player.backingFiles[42].Name(), gc.Equals, s.player.stagedPath(42))
}

func (s *PlaybackSuite) TestCreateErrors(c *gc.C) {
	// Place a pre-existing fixture at the expected staging path.
	c.Check(ioutil.WriteFile(s.player.stagedPath(42), []byte("whoops"), 0644), gc.IsNil)

	// Expect that we fail with an aborting error.
	c.Check(s.apply(c, s.frameCreate("/a/path")), gc.ErrorMatches, "open /.*/42: file exists")
	c.Check(s.player.backingFiles, gc.HasLen, 0)
}

func (s *PlaybackSuite) TestUnlink(c *gc.C) {
	// Precondition fixture: staged fnode exists with two links.
	c.Check(s.apply(c, s.frameCreate("/a/path")), gc.IsNil)
	c.Check(s.apply(c, s.frameLink(42, "/other/path")), gc.IsNil)

	// Apply first unlink.
	c.Check(s.apply(c, s.frameUnlink(42, "/a/path")), gc.IsNil)

	// Expect staged file still exists.
	c.Check(s.player.backingFiles[42].Name(), gc.Equals, s.player.stagedPath(42))

	// Second unlink.
	c.Check(s.apply(c, s.frameUnlink(42, "/other/path")), gc.IsNil)

	// Staged file was removed.
	c.Check(s.player.backingFiles, gc.HasLen, 0)
}

func (s *PlaybackSuite) TestUnlinkCloseError(c *gc.C) {
	c.Check(s.apply(c, s.frameCreate("/a/path")), gc.IsNil)

	// Sneak in a Close() such that a successive close fails.
	s.player.backingFiles[42].Close()

	err := s.apply(c, s.frameUnlink(42, "/a/path"))
	c.Check(err, gc.ErrorMatches, "invalid argument")

	c.Check(s.player.backingFiles, gc.HasLen, 1)
}

func (s *PlaybackSuite) TestUnlinkRemoveError(c *gc.C) {
	c.Check(s.apply(c, s.frameCreate("/a/path")), gc.IsNil)

	// Sneak in a Remove() such that a successive remove fails.
	c.Check(os.Remove(s.player.backingFiles[42].Name()), gc.IsNil)

	err := s.apply(c, s.frameUnlink(42, "/a/path"))
	c.Check(err, gc.ErrorMatches, "remove .*: no such file or directory")

	c.Check(s.player.backingFiles, gc.HasLen, 1)
}

func (s *PlaybackSuite) TestUnlinkUntrackedError(c *gc.C) {
	// Expect error is swallowed / logged internally, and not surfaced.
	c.Check(s.apply(c, s.frameUnlink(15, "/a/path")), gc.IsNil)
}

func (s *PlaybackSuite) TestWrites(c *gc.C) {
	c.Check(s.apply(c, s.frameCreate("/a/path")), gc.IsNil)
	c.Check(s.apply(c, s.frameCreate("/skipped/path")), gc.IsNil)

	getContent := func(fnode Fnode) string {
		bytes, err := ioutil.ReadFile(s.player.stagedPath(fnode))
		c.Check(err, gc.IsNil)
		return string(bytes)
	}

	// Perform a few writes, occurring out of order and with repetition of write range.
	buf := s.frameWrite(42 /* Fnode*/, 5 /* Offset */, 10 /* Length */)
	buf.WriteString("over-write")
	c.Check(s.apply(c, buf), gc.IsNil)
	c.Check(getContent(42), gc.Equals, "\x00\x00\x00\x00\x00over-write")

	buf = s.frameWrite(42, 0, 5)
	buf.WriteString("abcde")
	c.Check(s.apply(c, buf), gc.IsNil)
	c.Check(getContent(42), gc.Equals, "abcdeover-write")

	buf = s.frameWrite(42, 5, 10)
	buf.WriteString("0123456789")
	c.Check(s.apply(c, buf), gc.IsNil)
	c.Check(getContent(42), gc.Equals, "abcde0123456789")

	// Reader returns early EOF (before op.Length). Expect an ErrUnexpectedEOF.
	buf = s.frameWrite(42, 15, 10)
	buf.WriteString("short")
	c.Check(s.apply(c, buf), gc.Equals, io.ErrUnexpectedEOF)
	c.Check(getContent(42), gc.Equals, "abcde0123456789short")

	// Writes to skipped fnodes succeed without error, but are ignored.
	buf = s.frameWrite(43, 5, 10)
	buf.WriteString("0123456789")
	c.Check(s.apply(c, buf), gc.IsNil)
	_, err := os.Stat(s.player.stagedPath(43))
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func (s *PlaybackSuite) TestUnderlyingWriteError(c *gc.C) {
	c.Check(s.apply(c, s.frameCreate("/a/path")), gc.IsNil)

	// Underlying Write() returns an error. Expect it's aborting.
	readOnlyFile, _ := os.Open(s.player.stagedPath(42))
	s.player.backingFiles[42] = readOnlyFile

	buf := s.frameWrite(42, 0, 5)
	buf.WriteString("abcde")
	err := s.apply(c, buf)
	c.Check(err, gc.ErrorMatches, "^write.*")

	// Seek returns an error. Expect it's aborting.
	s.player.backingFiles[42].Close()

	buf = s.frameWrite(42, 0, 5)
	buf.WriteString("abcde")
	err = s.apply(c, buf)
	c.Check(err, gc.ErrorMatches, "^seek.*")
}

func (s *PlaybackSuite) TestWriteUntrackedError(c *gc.C) {
	// Expect write to unknown file node is ignored.
	buf := s.frameWrite(15, 0, 10)
	buf.WriteString("0123456789")

	// Note apply() verifies that |buf| is fully consumed.
	c.Check(s.apply(c, buf), gc.IsNil)

	// While discarding, EOF errors are passed through and mapped to UnexpectedEOF
	buf = s.frameWrite(15, 0, 11)
	buf.WriteString("0123456789") // 10 bytes of content for 11-byte operation.

	c.Check(s.apply(c, buf), gc.Equals, io.ErrUnexpectedEOF)
}

func (s *PlaybackSuite) TestMakeLive(c *gc.C) {
	c.Check(s.apply(c, s.frameCreate("/a/path")), gc.IsNil)
	c.Check(s.apply(c, s.frameCreate("/skipped/path")), gc.IsNil)
	c.Check(s.apply(c, s.frameCreate("/another/path")), gc.IsNil)
	c.Check(s.apply(c, s.frameLink(42, "/linked/path")), gc.IsNil)
	c.Check(s.apply(c, s.frameUnlink(43, "/skipped/path")), gc.IsNil)

	c.Check(s.player.makeLive(), gc.IsNil)

	expect := func(path string, exists bool) {
		_, err := os.Stat(path)
		if exists {
			c.Check(err, gc.IsNil)
		} else {
			c.Check(os.IsNotExist(err), gc.Equals, true)
		}
	}
	// Expect staging directory has been removed.
	expect(filepath.Join(s.localDir, kFnodeStagingDir), false)

	// Expect files have been linked into final locations.
	expect(filepath.Join(s.localDir, "a/path"), true)
	expect(filepath.Join(s.localDir, "another/path"), true)
	expect(filepath.Join(s.localDir, "linked/path"), true)
	expect(filepath.Join(s.localDir, "skipped/path"), false)

	// Expect property file was written.
	bytes, err := ioutil.ReadFile(filepath.Join(s.localDir, "property/path"))
	c.Check(err, gc.IsNil)
	c.Check(string(bytes), gc.Equals, "prop-value")
}

func (s *PlaybackSuite) TestHintsRemainOnMakeLive(c *gc.C) {
	c.Check(s.apply(c, s.frameCreate("/a/path")), gc.IsNil)

	err := s.player.makeLive()
	c.Check(err, gc.ErrorMatches, "FSM has remaining unused hints.*")
}

func (s *PlaybackSuite) frame(op RecordedOp) *bytes.Buffer {
	if s.player.fsm.NextSeqNo != 0 {
		op.SeqNo = s.player.fsm.NextSeqNo
	} else {
		op.SeqNo = 1
	}
	op.Checksum = s.player.fsm.NextChecksum
	op.Author = 100

	var frame []byte
	message.Frame(&op, &frame)
	return bytes.NewBuffer(frame)
}

func (s *PlaybackSuite) frameCreate(path string) *bytes.Buffer {
	return s.frame(RecordedOp{Create: &RecordedOp_Create{Path: path}})
}

func (s *PlaybackSuite) frameLink(fnode Fnode, path string) *bytes.Buffer {
	return s.frame(RecordedOp{Link: &RecordedOp_Link{Fnode: fnode, Path: path}})
}

func (s *PlaybackSuite) frameUnlink(fnode Fnode, path string) *bytes.Buffer {
	return s.frame(RecordedOp{Unlink: &RecordedOp_Link{Fnode: fnode, Path: path}})
}

func (s *PlaybackSuite) frameWrite(fnode Fnode, offset, length int64) *bytes.Buffer {
	return s.frame(RecordedOp{
		Write: &RecordedOp_Write{Fnode: fnode, Offset: offset, Length: length}})
}

func (s *PlaybackSuite) apply(c *gc.C, buf *bytes.Buffer) error {
	err := s.player.playOperation(buf, nil)

	// Expect offset is incremented by whole-message boundary, only on success.
	if err == nil {
		c.Check(buf.Len(), gc.Equals, 0) // Fully consumed.

		// Expect a successive operation passes through an EOF at the message boundary.
		c.Check(s.player.playOperation(buf, nil), gc.Equals, io.EOF)
	}
	return err
}

var _ = gc.Suite(&PlaybackSuite{})
