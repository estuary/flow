// +build !norocksdb

package consumer

import (
	"io/ioutil"
	"os"

	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
	"github.com/tecbot/gorocksdb"
)

type RocksDBSuite struct{}

func (s *RocksDBSuite) TestWriteAndReadKeysAndOffsets(c *gc.C) {
	var dir, err = ioutil.TempDir("", "rocksdb")
	c.Assert(err, gc.IsNil)
	defer os.RemoveAll(dir)

	// Replace observed Env with regular one.
	var store = NewRocksDBStore(nil, dir)
	store.Env = gorocksdb.NewDefaultEnv()

	c.Assert(store.Open(), gc.IsNil)

	store.WriteBatch.Put([]byte("foo"), []byte("bar"))
	store.WriteBatch.Put([]byte("baz"), []byte("bing"))

	store.Flush(map[protocol.Journal]int64{
		"journal/A": 1234,
	})

	r, err := store.DB.Get(store.ReadOptions, []byte("foo"))
	c.Assert(err, gc.IsNil)
	c.Check(r.Data(), gc.DeepEquals, []byte("bar"))
	r.Free()

	store.Flush(map[protocol.Journal]int64{
		"journal/B": 5678,
	})

	offsets, err := store.FetchJournalOffsets()
	c.Check(err, gc.IsNil)
	c.Check(offsets, gc.DeepEquals, map[protocol.Journal]int64{
		"journal/A": 1234,
		"journal/B": 5678,
	})

	store.Destroy()
}

var _ = gc.Suite(&RocksDBSuite{})
