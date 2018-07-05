package cmd

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"io"
	"strings"
	"testing"
	"github.com/LiveRamp/gazette/pkg/consumer"
	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/consumer/consumertest"
	"fmt"
)

// Use a small buffer to exercise bufio.Reader underflow & fill.
const bufferSize = 16

type ShardComposeSuite struct{}

func (s *ShardComposeSuite) TestHexCases(c *gc.C) {
	var cases = []iterFuncTestCase{
		{ // Valid input. We handle keys/value lengths that exceed the bufio.Reader's internal capacity.
			fn: hexIter("" +
				"0x : 0x00\n" +
				"0xaabbccdd ==> 0x11\n" +
				"0xbbccddee ==> 0x2222\n" +
				"0xCCCC : 0x333333\n" +
				"0x" + strings.Repeat("d", 4*bufferSize) + " : 0x" + strings.Repeat("4", 5*bufferSize) + "\n" +
				"0xeEeeff => : 0x\n"),
			expectKeys: [][]byte{
				nil,
				{0xaa, 0xbb, 0xcc, 0xdd},
				{0xbb, 0xcc, 0xdd, 0xee},
				{0xcc, 0xcc},
				bytes.Repeat([]byte{0xdd}, 4*bufferSize/2),
				{0xee, 0xee, 0xff},
			},
			expectVals: [][]byte{
				{0x00},
				{0x11},
				{0x22, 0x22},
				{0x33, 0x33, 0x33},
				bytes.Repeat([]byte{0x44}, 5*bufferSize/2),
				{},
			},
		},
		{ // Simple, valid example.
			fn: hexIter("0xaaaaaa 0xbbbbbb\n"),
			expectKeys: [][]byte{{0xaa, 0xaa, 0xaa}},
			expectVals: [][]byte{{0xbb, 0xbb, 0xbb}},
		},
		{ // Key missing 0x prefix.
			fn: hexIter("aaaaaa 0xbbbbbb\n"),
			expectErr: "invalid hex prefix .*",
		},
		{ // Value missing 0x prefix.
			fn: hexIter("0xaaaaaa bbbbbb\n"),
			expectErr: "invalid hex prefix .*",
		},
		{ // Missing value.
			fn: hexIter("0xaaaaaa\n"),
			expectErr: "unexpected EOF",
		},
		{ // Missing trailing newline.
			fn: hexIter("0xaaaaaa 0xbbbbbb"),
			expectErr: "unexpected EOF",
		},
		{ // Invalid key hex.
			fn: hexIter("0xaaxxaaaa 0xbbbbbb\n"),
			expectErr: "encoding/hex: .*",
		},
		{ // Invalid value hex.
			fn: hexIter("0xaaxxaa 0xbbxxbbbb\n"),
			expectErr: "encoding/hex: .*",
		},
		{ // Extra token.
			fn: hexIter("0xaa 0xbb 0xcc\n"),
			expectErr: "encoding/hex: .*",
		},
	}

	for _, tc := range cases {
		tc.test(c)
	}
}

func (s *ShardComposeSuite) TestHeapCases(c *gc.C) {
	iterf := newHeapIterFunc(First,
		hexIter(""+
			"0xcccc 0xdddd\n"+
			"0xee 0xff\n"+
			"0xaaaaaa 0xbbbbbb\n"))
	var k, v []byte
	var err error

	for i := 0; i < 10; i++ {
		k, v, err = iterf(k, v)
		fmt.Println(k, v, err)
	}

	var cases = []iterFuncTestCase{
		{ // Simple, valid example.
			fn: newHeapIterFunc(
				First,
				hexIter(""+
					"0xaaaaaa 0xbbbbbb\n"+
					"0xcccc 0xdddd\n"+
					"0xee 0xff\n")),
			expectKeys: [][]byte{{0xaa, 0xaa, 0xaa}, {0xcc, 0xcc}, {0xee}},
			expectVals: [][]byte{{0xbb, 0xbb, 0xbb}, {0xdd, 0xdd}, {0xff}},
		},
		{ // Reversing ordering results in an error on the out-of-order key.
			fn: newHeapIterFunc(
				First,
				hexIter(""+
					"0xcccc 0xdddd\n"+
					"0xee 0xff\n"+
					"0xaaaaaa 0xbbbbbb\n")),
			expectKeys: [][]byte{{0xcc, 0xcc}, {0xee}},
			expectVals: [][]byte{{0xdd, 0xdd}, {0xff}},
			expectErr: "invalid iterator order: ee > aaaaaa",
		},
		{ // Composing iterators. Where keys collide, expect all but the first iterator value is dropped.
			fn: newHeapIterFunc(
				First,
				hexIter(""+
					"0xaabb 0x2222\n"+
					"0xbbcc 0x3333\n"),
				hexIter(""+
					"0xaaaa 0x1111\n"+
					"0xbbcc 0xfff2\n"+ // Discarded.
					"0xeeff 0x4444\n"),
				hexIter(""+
					"0xbbcc 0xfff3\n"+ // Discarded.
					"0xeeff 0xfff4\n"+ // Discarded.
					"0xffff 0x5555\n"),
			),
			expectKeys: [][]byte{{0xaa, 0xaa}, {0xaa, 0xbb}, {0xbb, 0xcc}, {0xee, 0xff}, {0xff, 0xff}},
			expectVals: [][]byte{{0x11, 0x11}, {0x22, 0x22}, {0x33, 0x33}, {0x44, 0x44}, {0x55, 0x55}},
		},
		{ // Underlying iterator errors are surfaced when encountered.
			fn: newHeapIterFunc(
				First,
				hexIter(""+
					"0xaabb 0x2222\n"+
					"0xfoobar bad value\n"),
				hexIter(""+
					"0xaaaa 0x1111\n"+
					"0xeeff 0x4444\n"),
			),
			expectKeys: [][]byte{{0xaa, 0xaa}},
			expectVals: [][]byte{{0x11, 0x11}},
			expectErr: "encoding/hex: .*",
		},
	}
	for _, tc := range cases {
		tc.test(c)
	}
}

func (s *ShardComposeSuite) TestDBIterCases(c *gc.C) {
	var shard, err = consumertest.NewShard("shard-compose-suite")
	c.Assert(err, gc.IsNil)

	defer shard.Close()

	c.Assert(shard.Database().Put(shard.WriteOptions(), []byte{0xbb, 0xbb}, []byte{0x22, 0x22}), gc.IsNil)
	c.Assert(shard.Database().Put(shard.WriteOptions(), []byte{0xaa, 0xaa}, []byte{0x11, 0x11}), gc.IsNil)

	var cases = []iterFuncTestCase{
		{ // Direct iteration of a DB.
			fn: newDBIterFunc(shard.Database()),
			expectKeys: [][]byte{{0xaa, 0xaa}, {0xbb, 0xbb}},
			expectVals: [][]byte{{0x11, 0x11}, {0x22, 0x22}},
		},
		{ // DB iterator wrapped with heap iterator.
			fn: newHeapIterFunc(
				First,
				newDBIterFunc(shard.Database())),
			expectKeys: [][]byte{{0xaa, 0xaa}, {0xbb, 0xbb}},
			expectVals: [][]byte{{0x11, 0x11}, {0x22, 0x22}},
		},
		{ // DB iterator composed with hex iterator.
			fn: newHeapIterFunc(
				First,
				newDBIterFunc(shard.Database()),
				hexIter(""+
					"0xaaaa 0xffff\n"+ // Discarded.
					"0xaabb 0x1122\n")),
			expectKeys: [][]byte{{0xaa, 0xaa}, {0xaa, 0xbb}, {0xbb, 0xbb}},
			expectVals: [][]byte{{0x11, 0x11}, {0x11, 0x22}, {0x22, 0x22}},
		},
	}
	for _, tc := range cases {
		tc.test(c)
	}
}

func (s *ShardComposeSuite) TestFilterCases(c *gc.C) {
	// |filter| modifies keys with 0xee suffix, and removes keys with 0xff suffix.
	var filter = testFilter{
		fn: func(key, value []byte) (remove bool, newValue []byte) {
			if t := key[len(key)-1]; t == 0xee {
				newValue = []byte{0x00}
			} else if t == 0xff {
				remove = true
			}
			return
		},
	}

	var cases = []iterFuncTestCase{
		{ // Passed through.
			fn: newFilterIterFunc(filter, hexIter("0xaaaa 0xbbbb\n")),
			expectKeys: [][]byte{{0xaa, 0xaa}},
			expectVals: [][]byte{{0xbb, 0xbb}},
		},
		{ // Modified.
			fn: newFilterIterFunc(filter, hexIter("0xaaee 0xbbbb\n")),
			expectKeys: [][]byte{{0xaa, 0xee}},
			expectVals: [][]byte{{0x00}},
		},
		{ // Filtered.
			fn: newFilterIterFunc(filter, hexIter("0xaaff 0xbbbb\n")),
			expectKeys: [][]byte{},
			expectVals: [][]byte{},
		},
		{ // Multiple filtered keys.
			fn: newFilterIterFunc(filter, hexIter(""+
				"0xaaff 0xbbbb\n"+ // Filtered.
				"0xbbff 0xcccc\n"+ // Filtered.
				"0xddff 0xdddd\n"+ // Filtered.
				"0xeeee 0xeeee\n"+ // Modified.
				"0xffaa 0xffff\n")), // Passed through.
			expectKeys: [][]byte{{0xee, 0xee}, {0xff, 0xaa}},
			expectVals: [][]byte{{0x00}, {0xff, 0xff}},
		},
	}
	for _, tc := range cases {
		tc.test(c)
	}
}

func (s *ShardComposeSuite) TestOffsetUpdateCases(c *gc.C) {
	var fixture = consumer.AppendOffsetKeyEncoding(nil, "foo/bar")
	c.Check(fixture, gc.DeepEquals,
		[]byte{0x0, 0x12, 'm', 'a', 'r', 'k', 0x0, 0x1, 0x12, 'f', 'o', 'o', '/', 'b', 'a', 'r', 0x0, 0x1})

	var cases = []iterFuncTestCase{
		{
			fn:         newConsumerOffsetIterFunc("foo/bar", 12345),
			expectKeys: [][]byte{fixture},
			expectVals: [][]byte{{0xf7, 0x30, 0x39}},
		},
		{ // Mix a consumer offset into another sequence.
			fn: newHeapIterFunc(
				First,
				newConsumerOffsetIterFunc("foo/bar", 12345),
				hexIter(""+
					"0x0001 0xaaaa\n"+
					"0xcccc 0xdddd\n")),
			expectKeys: [][]byte{{0x0, 0x1}, fixture, {0xcc, 0xcc}},
			expectVals: [][]byte{{0xaa, 0xaa}, {0xf7, 0x30, 0x39}, {0xdd, 0xdd}},
		},
		{ // newHeapIterFunc ensures the consumer offset has precedence over a prior value.
			fn: newHeapIterFunc(
				First,
				newConsumerOffsetIterFunc("foo/bar", 12345),
				hexIter(""+
					"0x"+ hex.EncodeToString(fixture)+ " 0xaaaa\n"+
					"0xcccc 0xdddd\n")),
			expectKeys: [][]byte{fixture, {0xcc, 0xcc}},
			expectVals: [][]byte{{0xf7, 0x30, 0x39}, {0xdd, 0xdd}},
		},
	}
	for _, tc := range cases {
		tc.test(c)
	}
}

type testFilter struct {
	fn func(key, value []byte) (remove bool, newValue []byte)
}

func (tf testFilter) Filter(level int, key, value []byte) (bool, []byte) { return tf.fn(key, value) }

func hexIter(s string) iterFunc {
	return newHexIterFunc(bufio.NewReaderSize(bytes.NewReader([]byte(s)), bufferSize))
}

type iterFuncTestCase struct {
	fn         iterFunc
	expectErr  string
	expectKeys [][]byte
	expectVals [][]byte
}

func (tc iterFuncTestCase) test(c *gc.C) {
	var key, value []byte
	var err error

	for i := range tc.expectKeys {
		key, value, err = tc.fn(key, value)
		c.Check(err, gc.IsNil)
		c.Check(key, gc.DeepEquals, tc.expectKeys[i])
		c.Check(value, gc.DeepEquals, tc.expectVals[i])
	}

	key, value, err = tc.fn(key, value)

	if tc.expectErr != "" {
		c.Check(err, gc.ErrorMatches, tc.expectErr)
	} else {
		c.Check(err, gc.Equals, io.EOF)
	}
}

var _ = gc.Suite(&ShardComposeSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
