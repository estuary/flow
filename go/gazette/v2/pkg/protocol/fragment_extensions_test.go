package protocol

import (
	"math"
	"testing"

	gc "github.com/go-check/check"
)

type FragmentSuite struct{}

func (s *FragmentSuite) TestContentName(c *gc.C) {
	var f = Fragment{
		Journal:          "a/journal",
		Begin:            1234567890,
		End:              math.MaxInt64,
		Sum:              SHA1Sum{Part1: 0x0102030405060708, Part2: 0x090a0b0c0d0e0f10, Part3: 0x11121314},
		CompressionCodec: CompressionCodec_GZIP,
	}
	c.Check(f.ContentName(), gc.Equals,
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314.gz")

	f.CompressionCodec = CompressionCodec_SNAPPY
	c.Check(f.ContentName(), gc.Equals,
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314.sz")

	f.CompressionCodec = CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION
	c.Check(f.ContentName(), gc.Equals,
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314")
}

func (s *FragmentSuite) TestContentPath(c *gc.C) {
	var f = Fragment{
		Journal:          "a/journal/name",
		Begin:            1234567890,
		End:              math.MaxInt64,
		Sum:              SHA1Sum{Part1: 0x0102030405060708, Part2: 0x090a0b0c0d0e0f10, Part3: 0x11121314},
		CompressionCodec: CompressionCodec_GZIP,
	}
	c.Assert(f.ContentPath(), gc.Equals, "a/journal/name/"+
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314.gz")
}

func (s *FragmentSuite) TestValidationCases(c *gc.C) {
	var f = Fragment{
		Journal:          "a/journal/name",
		Begin:            1234567890,
		End:              math.MaxInt64,
		Sum:              SHA1Sum{Part1: 0x0102030405060708, Part2: 0x090a0b0c0d0e0f10, Part3: 0x11121314},
		CompressionCodec: CompressionCodec_GZIP,
	}
	c.Check(f.Validate(), gc.IsNil)

	f.Begin, f.End = f.End, f.Begin
	c.Check(f.Validate(), gc.ErrorMatches, `expected Begin <= End \(have .*`)
	f.Begin, f.End = f.End, f.Begin

	f.Journal = "a"
	c.Check(f.Validate(), gc.ErrorMatches, "Journal: invalid length .*")

	f.Journal = "foo/bar/baz"
	f.CompressionCodec = 1 << 20
	c.Check(f.Validate(), gc.ErrorMatches, "CompressionCodec: invalid value .*")
}

func (s *FragmentSuite) TestParsingSuccessCases(c *gc.C) {
	var f, err = ParseContentPath("a/journal/" +
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314.gz")

	c.Check(err, gc.IsNil)
	c.Check(f, gc.DeepEquals, Fragment{
		Journal:          "a/journal",
		Begin:            1234567890,
		End:              math.MaxInt64,
		Sum:              SHA1Sum{Part1: 0x0102030405060708, Part2: 0x090a0b0c0d0e0f10, Part3: 0x11121314},
		CompressionCodec: CompressionCodec_GZIP,
	})

	// Again, but with no file extension (indicating Content-Encoding compression).
	f, err = ParseContentPath("a/journal/" +
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314")

	c.Check(err, gc.IsNil)
	c.Check(f, gc.DeepEquals, Fragment{
		Journal:          "a/journal",
		Begin:            1234567890,
		End:              math.MaxInt64,
		Sum:              SHA1Sum{Part1: 0x0102030405060708, Part2: 0x090a0b0c0d0e0f10, Part3: 0x11121314},
		CompressionCodec: CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION,
	})

	// Empty spool (begin == end, and zero checksum).
	f, err = ParseContentPath("a/journal/" +
		"00000000499602d2-00000000499602d2-0000000000000000000000000000000000000000.raw")
	c.Assert(err, gc.IsNil)
	c.Assert(f, gc.DeepEquals, Fragment{
		Journal:          "a/journal",
		Begin:            1234567890,
		End:              1234567890,
		Sum:              SHA1Sum{},
		CompressionCodec: CompressionCodec_NONE,
	})
}

func (s *FragmentSuite) TestParsingErrorCases(c *gc.C) {
	var _, err = ParseContentPath("a/journal/" +
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314-extra.gz")
	c.Check(err, gc.ErrorMatches, "wrong Fragment format: .*")

	_, err = ParseContentPath("a/journal/" +
		"00000000499602XX-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314.gz")
	c.Check(err, gc.ErrorMatches, "Begin: strconv.ParseInt: .*")

	_, err = ParseContentPath("a/journal/" +
		"00000000499602d2-7fffffffffffffXX-0102030405060708090a0b0c0d0e0f1011121314.gz")
	c.Check(err, gc.ErrorMatches, "End: strconv.ParseInt: .*")

	_, err = ParseContentPath("a/journal/" +
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f10111213XX.gz")
	c.Check(err, gc.ErrorMatches, "Sum: encoding/hex: .*")

	_, err = ParseContentPath("a/journal/" +
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f10111213.gz")
	c.Check(err, gc.ErrorMatches, "invalid SHA1Sum length: .*")

	_, err = ParseContentPath("a/journal/" +
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314.XXX")
	c.Check(err, gc.ErrorMatches, "unrecognized compression extension: .XXX")

	// Expect we also Validate the parsed Fragment before returning.
	_, err = ParseContentPath("a/journal/" +
		"7fffffffffffffff-00000000499602d2-0102030405060708090a0b0c0d0e0f1011121314.gz")
	c.Check(err, gc.ErrorMatches, "expected Begin <= End .*")
}

var _ = gc.Suite(&FragmentSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
