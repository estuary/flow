package gazette

import (
	"bytes"
	"compress/gzip"
	"errors"
	gc "github.com/go-check/check"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"testing"
)

type SpoolSuite struct {
	localDir string
}

func (s *SpoolSuite) SetUpTest(c *gc.C) {
	var err error
	s.localDir, err = ioutil.TempDir("", "spool-suite")
	c.Assert(err, gc.IsNil)
}
func (s *SpoolSuite) TearDownTest(c *gc.C) {
	os.RemoveAll(s.localDir)
}

func (s *SpoolSuite) TestContentNameSerialization(c *gc.C) {
	fixture := &Spool{
		Journal:    "a/journal/name",
		Begin:      1234567890,
		LastCommit: math.MaxInt64,
		LastCommitSum: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	}
	c.Assert(fixture.ContentName(), gc.Equals, "a/journal/name/"+
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314")
}

func (s *SpoolSuite) TestContentNameParsing(c *gc.C) {
	begin, end, sum, err := ParseContentName(
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314")
	c.Assert(err, gc.IsNil)
	c.Assert(begin, gc.Equals, int64(1234567890))
	c.Assert(end, gc.Equals, int64(math.MaxInt64))
	c.Assert(sum, gc.DeepEquals, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20})

	// Empty spool name (begin == end, and empty checksum).
	begin, end, sum, err = ParseContentName(
		"00000000499602d2-00000000499602d2-")
	c.Assert(err, gc.IsNil)
	c.Assert(begin, gc.Equals, int64(1234567890))
	c.Assert(end, gc.Equals, int64(1234567890))
	c.Assert(sum, gc.DeepEquals, []byte{})

	_, _, _, err = ParseContentName(
		"00000000499602d2-7fffffffffffffff-010203040506")
	c.Assert(err, gc.ErrorMatches, "invalid checksum")
	// Empty checksum disallowed if begin != end.
	_, _, _, err = ParseContentName(
		"00000000499602d2-7fffffffffffffff-")
	c.Assert(err, gc.ErrorMatches, "invalid checksum")
	// Populated checksum disallowed if begin == end.
	_, _, _, err = ParseContentName(
		"00000000499602d2-00000000499602d2-0102030405060708090a0b0c0d0e0f1011121314")
	c.Assert(err, gc.ErrorMatches, "invalid checksum")

	_, _, _, err = ParseContentName(
		"2-1-0102030405060708090a0b0c0d0e0f1011121314")
	c.Assert(err, gc.ErrorMatches, "invalid content range")
	_, _, _, err = ParseContentName(
		"1-0102030405060708090a0b0c0d0e0f1011121314")
	c.Assert(err, gc.ErrorMatches, "wrong format")
}

func (s *SpoolSuite) TestCommitFlow(c *gc.C) {
	spool := NewSpool(s.localDir, "journal/name", 12345)
	c.Assert(spool.Create(), gc.IsNil)

	path1 := filepath.Join(s.localDir, "journal", "name",
		"0000000000003039-0000000000003039-")

	_, err := os.Stat(path1)
	c.Assert(err, gc.IsNil)

	c.Assert(spool.Write([]byte("a write")), gc.IsNil)
	c.Assert(spool.LastWrite, gc.Equals, int64(12345+7))
	c.Assert(spool.Write([]byte("another write")), gc.IsNil)
	c.Assert(spool.LastWrite, gc.Equals, int64(12345+7+13))

	c.Assert(spool.Commit(), gc.IsNil)
	c.Assert(spool.LastWrite, gc.Equals, int64(12345+7+13))
	c.Assert(spool.LastCommit, gc.Equals, int64(12345+7+13))
	c.Assert(spool.CommittedSize(), gc.Equals, int64(7+13))
	c.Assert(spool.LastCommitSum, gc.DeepEquals, []byte{
		0x50, 0x83, 0x09, 0x1d, 0x84, 0x08, 0x83, 0xd2, 0x4e, 0x44,
		0x1d, 0x6f, 0x4a, 0x89, 0xab, 0x0d, 0xca, 0x56, 0x59, 0x52})

	path2 := filepath.Join(s.localDir, "journal", "name",
		"0000000000003039-000000000000304d-5083091d840883d24e441d6f4a89ab0dca565952")
	c.Assert(path2, gc.Equals, spool.LocalPath())

	_, err = os.Stat(path1)
	c.Assert(os.IsNotExist(err), gc.Equals, true)
	_, err = os.Stat(path2)
	c.Assert(err, gc.IsNil)

	c.Assert(spool.Write([]byte("a final write")), gc.IsNil)
	c.Assert(spool.LastWrite, gc.Equals, int64(12345+7+13+13))

	c.Assert(spool.Commit(), gc.IsNil)
	c.Assert(spool.LastWrite, gc.Equals, int64(12345+7+13+13))
	c.Assert(spool.LastCommit, gc.Equals, int64(12345+7+13+13))
	c.Assert(spool.CommittedSize(), gc.Equals, int64(7+13+13))
	c.Assert(spool.LastCommitSum, gc.DeepEquals, []byte{
		0x65, 0xde, 0x6f, 0x9c, 0x8, 0x84, 0xc1, 0x81, 0xad, 0x91,
		0x6b, 0x7, 0xcd, 0x2e, 0x2c, 0xb2, 0x62, 0xf9, 0xde, 0xd9})

	path3 := filepath.Join(s.localDir, "journal", "name",
		"0000000000003039-000000000000305a-65de6f9c0884c181ad916b07cd2e2cb262f9ded9")
	c.Assert(path3, gc.Equals, spool.LocalPath())

	_, err = os.Stat(path1)
	c.Assert(os.IsNotExist(err), gc.Equals, true)
	_, err = os.Stat(path2)
	c.Assert(os.IsNotExist(err), gc.Equals, true)
	_, err = os.Stat(path3)
	c.Assert(err, gc.IsNil)
}

func (s *SpoolSuite) TestChecksumEquivalence(c *gc.C) {
	spool := NewSpool(s.localDir, "journal/name", 12345)

	// Write equivalent data to TestCommitFlow in a single write and transaction.
	// Verify the same resulting checksum and sizes as that test.
	c.Assert(spool.Write([]byte("a writeanother writea final write")), gc.IsNil)
	c.Assert(spool.Commit(), gc.IsNil)
	c.Assert(spool.CommittedSize(), gc.Equals, int64(7+13+13))
	c.Assert(spool.LocalPath(), gc.Equals,
		filepath.Join(s.localDir, "journal", "name",
			"0000000000003039-000000000000305a-65de6f9c0884c181ad916b07cd2e2cb262f9ded9"))
}

func (s *SpoolSuite) TestWriteFailureHandling(c *gc.C) {
	spool := NewSpool(s.localDir, "journal/name", 12345)

	c.Assert(spool.Write([]byte("initial commit")), gc.IsNil)
	c.Assert(spool.Commit(), gc.IsNil)
	firstCommitSignature := spool.ContentName()

	c.Assert(spool.Write([]byte("first write")), gc.IsNil)
	spool.backingFile.Close() // Close out from under, such that writes fail.

	err := spool.Write([]byte("failed write"))
	c.Assert(err, gc.Not(gc.IsNil))
	c.Assert(err, gc.Equals, spool.Error)

	// |err| continues to be returned by |Write| and |Commit|
	c.Assert(spool.Write([]byte("another failed write")), gc.Equals, err)
	c.Assert(spool.Commit(), gc.Equals, err)

	c.Assert(spool.ContentName(), gc.Equals, firstCommitSignature) // No change.
}

func (s *SpoolSuite) TestCommitFailureHandling(c *gc.C) {
	spool := NewSpool(s.localDir, "journal/name", 12345)

	c.Assert(spool.Write([]byte("initial commit")), gc.IsNil)
	c.Assert(spool.Commit(), gc.IsNil)
	firstCommitSignature := spool.ContentName()

	c.Assert(spool.Write([]byte("first write")), gc.IsNil)

	spool.backingFile.Close() // Close out from under, such that commit fails.
	err := spool.Commit()
	c.Assert(err, gc.Not(gc.IsNil))
	c.Assert(err, gc.Equals, spool.Error)

	c.Assert(spool.ContentName(), gc.Equals, firstCommitSignature) // No change.
}

func (s *SpoolSuite) TestPersistence(c *gc.C) {
	spool := NewSpool(s.localDir, "journal/name", 12345)

	c.Assert(spool.Write([]byte("committed content")), gc.IsNil)
	c.Assert(spool.Commit(), gc.IsNil)
	c.Assert(spool.Write([]byte("extra non-persisted write")), gc.IsNil)

	_, err := os.Stat(spool.LocalPath())
	c.Assert(err, gc.IsNil) // Precondition: local file exists.

	context := &MockStorageContext{}
	c.Assert(spool.Persist(context), gc.IsNil)

	_, err = os.Stat(spool.LocalPath())
	c.Assert(os.IsNotExist(err), gc.Equals, true) // File removed.

	c.Assert(context.RecordedCreate[0].Name, gc.Equals, spool.ContentName())
	c.Assert(context.RecordedCreate[0].ContentEncoding, gc.Equals, "gzip")
	c.Assert(context.RecordedWrites.Bytes(), gc.DeepEquals,
		gzipped("committed content"))
}

func (s *SpoolSuite) TestUploadErrorHandling(c *gc.C) {
	spool := NewSpool(s.localDir, "journal/name", 12345)
	c.Assert(spool.Write([]byte("committed content")), gc.IsNil)
	c.Assert(spool.Commit(), gc.IsNil)

	err := errors.New("error!")
	{
		context := &MockStorageContext{}
		context.CreateReturn = err
		c.Assert(spool.Persist(context), gc.DeepEquals, err)
	}
	{
		context := &MockStorageContext{}
		context.WriteReturn = err
		c.Assert(spool.Persist(context), gc.DeepEquals, err)
	}
	{
		context := &MockStorageContext{}
		context.CloseReturn = err
		c.Assert(spool.Persist(context), gc.DeepEquals, err)
	}
	_, statErr := os.Stat(spool.LocalPath())
	c.Assert(statErr, gc.IsNil) // Local file still exists.
	{
		context := &MockStorageContext{}
		c.Assert(spool.Persist(context), gc.IsNil) // Successful upload.
	}
	_, statErr = os.Stat(spool.LocalPath())
	c.Assert(os.IsNotExist(statErr), gc.Equals, true) // Was removed.
}

func (s *SpoolSuite) TestSpoolRecovery(c *gc.C) {
	var fixture1, fixture2 string
	{
		spool1 := NewSpool(s.localDir, "journal/name", 12345)
		c.Assert(spool1.Write([]byte("fixture one content")), gc.IsNil)
		c.Assert(spool1.Commit(), gc.IsNil)

		spool2 := NewSpool(s.localDir, "journal/name", spool1.LastCommit)
		c.Assert(spool2.Write([]byte("fixture two content")), gc.IsNil)
		c.Assert(spool2.Commit(), gc.IsNil)

		fixture1 = spool1.ContentName()
		fixture2 = spool2.ContentName()

		spool1.backingFile.Close() // Close out from under to release lock.
		spool2.backingFile.Close()
	}
	spool3 := NewSpool(s.localDir, "journal/name", 56789)
	c.Assert(spool3.Write([]byte("fixture three")), gc.IsNil)
	c.Assert(spool3.Commit(), gc.IsNil)

	recovered := RecoverSpools(s.localDir)

	c.Assert(recovered, gc.HasLen, 2)
	c.Assert(recovered[0].ContentName(), gc.Equals, fixture1)
	c.Assert(recovered[1].ContentName(), gc.Equals, fixture2)

	context := &MockStorageContext{}
	c.Assert(recovered[0].Persist(context), gc.IsNil)
	c.Assert(context.RecordedWrites.Bytes(), gc.DeepEquals,
		gzipped("fixture one content"))
}

func gzipped(content string) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write([]byte(content))
	w.Close()
	return buf.Bytes()
}

var _ = gc.Suite(&SpoolSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
