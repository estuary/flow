package keyspace

import (
	"strconv"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	gc "github.com/go-check/check"
)

type KeyValuesSuite struct{}

func (s *KeyValuesSuite) TestSearchCases(c *gc.C) {
	var kv = buildKeyValuesFixture(c)

	var table = []struct {
		key   string
		found bool
		ind   int
	}{
		{"/foo", true, 0},
		{"/foo/aaa/0", false, 2},
		{"/foo/aaa/3", true, 3},
		{"/foo/aaa/4", false, 4},
		{"/foo/bb", false, 4},
		{"/foo/bbb", false, 4},
		{"/foo/bbbb", false, 6},
		{"/foo/bbb/ddd/eee/1", false, 6},
		{"/foo/ccc/apple/one", true, 8},
		{"/foo/ccc/apple/two", false, 9},
		{"/foo/ccc/banana/two", false, 10},
	}
	for _, tc := range table {
		var ind, found = kv.Search(tc.key)
		c.Check(found, gc.Equals, tc.found)
		c.Check(ind, gc.Equals, tc.ind)
	}
}

func (s *KeyValuesSuite) TestRangeCases(c *gc.C) {
	var kv = buildKeyValuesFixture(c)

	var table = []struct {
		from, to   string
		begin, end int
	}{
		{"/fo", "/foo", 0, 0},
		{"/foo", "/fop", 0, 10},
		{"/foo/aaa/2", "/foo/bbb/b", 3, 5},
		{"/foo/bbb/a", "/foo/bbb/b", 4, 5},
		{"/foo/bbb/c", "/foo/ccc/apple/one", 5, 8},
		{"/foo/bbb/c", "/foo/ccc/banana", 5, 9},
		{"/foo/bbb/c", "/foo/ccc/bananaz", 5, 10},
		{"/foo/ddd", "/zz", 10, 10},

		// Expect a range where from > to returns an empty slice values.
		{"/fop", "/foo", 0, 0},
		{"/foo/ccc", "/foo/aaa", 0, 0},
	}
	for _, tc := range table {
		var rng = kv.Range(tc.from, tc.to)
		c.Check(rng, gc.DeepEquals, kv[tc.begin:tc.end])
	}
}

func (s *KeyValuesSuite) TestPrefixCases(c *gc.C) {
	var kv = buildKeyValuesFixture(c)

	var table = []struct {
		prefix     string
		begin, end int
	}{
		{"/", 0, 10},
		{"/foo", 0, 10},
		{"/food", 0, 0},
		{"/foo/aaa", 1, 4},
		{"/foo/aaa/", 2, 4},
		{"/foo/bb", 4, 6},
		{"/foo/ccc", 6, 10},
		{"/foo/cccc", 10, 10},
		{"/zz", 10, 10},
	}
	for _, tc := range table {
		var rng = kv.Prefixed(tc.prefix)
		c.Check(rng, gc.DeepEquals, kv[tc.begin:tc.end])
	}
}

func (s *KeyValuesSuite) TestCopy(c *gc.C) {
	var kv = buildKeyValuesFixture(c)
	var ckv = kv.Copy()

	c.Check(kv, gc.DeepEquals, ckv)
	c.Check(&kv[0], gc.Not(gc.Equals), &ckv[0])
}

func (s *KeyValuesSuite) TestUpdateTailCases(c *gc.C) {
	var kv KeyValues

	var table = []struct {
		*clientv3.Event
		err string
	}{
		// Deletion when |kv| is empty (ignored).
		{Event: delEvent("/not-here", 1), err: "unexpected deletion of unknown key"},
		// Insertion when |kv| is empty.
		{Event: putEvent("/aaaa", "1111", 2, 2, 1)},
		// Modify the current tail.
		{Event: putEvent("/aaaa", "2222", 2, 3, 2)},
		// Modify such that CreateRevision unexpectedly differs.
		{Event: putEvent("/aaaa", "4444", 4, 4, 1),
			err: `unexpected CreateRevision \(should be equal; applied anyway; prev is .*`},

		// Add new key where ModRevision != CreateRevision.
		{Event: putEvent("/bbbb", "5555", 3, 5, 2),
			err: `unexpected modification of unknown key \(applied anyway\)`},

		// Add new key where Version != 1.
		{Event: putEvent("/cccc", "6666", 6, 6, 2),
			err: `unexpected Version of created key \(should be 1; applied anyway\)`},
		// Modified key where Version is not monotonic.
		{Event: putEvent("/cccc", "7777", 6, 7, 4),
			err: `unexpected Version \(should be monotonic; applied anyway; prev is .*`},
		// Modification at older revision is rejected.
		{Event: putEvent("/cccc", "0000", 6, 6, 5),
			err: `unexpected ModRevision \(it's too small; prev is .*`},

		// Insert and then delete a key.
		{Event: putEvent("/dddd", "8888", 8, 8, 1)},
		{Event: delEvent("/dddd", 9)},

		// Insert a key, and verify that a deletion at an older revision is rejected.
		{Event: putEvent("/eeee", "9999", 9, 9, 1)},
		{Event: delEvent("/eeee", 8),
			err: `unexpected ModRevision \(it's too small; prev is .*`},

		// Deletion referencing an unknown key is ignored.
		{Event: delEvent("/ffff", 10), err: `unexpected deletion of unknown key`},

		// Insertion of a key which fails to decode is ignored.
		{Event: putEvent("/gggg", "invalid", 11, 11, 1),
			err: `strconv.ParseInt: parsing .*`},
		// Modification of a key which fails to decode is also ignored.
		{Event: putEvent("/eeee", "invalid", 12, 12, 1),
			err: `strconv.ParseInt: parsing .*`},
	}
	for _, tc := range table {
		var err error
		if kv, err = updateKeyValuesTail(kv, testDecoder, *tc.Event); tc.err != "" {
			c.Check(err, gc.ErrorMatches, tc.err)
		} else {
			c.Check(err, gc.IsNil)
		}
	}

	verifyDecodedKeyValues(c, kv, map[string]int{
		"/aaaa": 4444,
		"/bbbb": 5555,
		"/cccc": 7777,
		"/eeee": 9999,
	})
}

func putEvent(key, value string, revCreate, revMod, version int64) *clientv3.Event {
	return &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte(key),
			Value:          []byte(value),
			CreateRevision: revCreate,
			ModRevision:    revMod,
			Version:        version,
		},
	}
}

func delEvent(key string, revMod int64) *clientv3.Event {
	return &clientv3.Event{
		Type: clientv3.EventTypeDelete,
		Kv: &mvccpb.KeyValue{
			Key:         []byte(key),
			ModRevision: revMod,
		},
	}
}

func verifyDecodedKeyValues(c *gc.C, kv KeyValues, expect map[string]int) {
	var content = make(map[string]int)
	for _, kv := range kv {
		content[string(kv.Raw.Key)] = kv.Decoded.(int)
	}
	c.Check(content, gc.DeepEquals, expect)
}

func buildKeyValuesFixture(c *gc.C) KeyValues {
	var kv KeyValues
	var anInt int
	var err error

	for i, t := range rawKeyValuesFixture {
		kv, err = appendKeyValue(kv, testDecoder, &t)
		c.Assert(err, gc.IsNil)
		c.Assert(kv[i].Decoded, gc.FitsTypeOf, anInt)
	}
	c.Assert(kv, gc.HasLen, len(rawKeyValuesFixture))
	return kv
}

// testDecoder interprets values as integers.
func testDecoder(kv *mvccpb.KeyValue) (interface{}, error) {
	var i, err = strconv.ParseInt(string(kv.Value), 10, 64)
	return int(i), err
}

var (
	rawKeyValuesFixture = func() []mvccpb.KeyValue {
		var t = []mvccpb.KeyValue{
			{Key: []byte("/foo"), Value: []byte("0")},
			{Key: []byte("/foo/aaa"), Value: []byte("1")},
			{Key: []byte("/foo/aaa/1"), Value: []byte("11")},
			{Key: []byte("/foo/aaa/3"), Value: []byte("13")},
			{Key: []byte("/foo/bbb/a"), Value: []byte("21")},
			{Key: []byte("/foo/bbb/c"), Value: []byte("23")},
			{Key: []byte("/foo/ccc"), Value: []byte("3")},
			{Key: []byte("/foo/ccc/apple"), Value: []byte("31")},
			{Key: []byte("/foo/ccc/apple/one"), Value: []byte("311")},
			{Key: []byte("/foo/ccc/banana"), Value: []byte("32")},
		}
		for i := range t {
			t[i].CreateRevision, t[i].ModRevision, t[i].Version = 1, 1, 1
		}
		return t
	}()
)

var _ = gc.Suite(&KeyValuesSuite{})
