package consumertest

import (
	"io/ioutil"
	"os"

	rocks "github.com/tecbot/gorocksdb"

	"github.com/LiveRamp/gazette/consumer"
	"github.com/LiveRamp/gazette/topic"
)

// Test type which conforms to consumer.Shard, and manages setup & teardown
// of a test RocksDB instance.
type Shard struct {
	IDFixture        consumer.ShardID
	PartitionFixture topic.Partition

	tmpdir string

	env  *rocks.Env
	opts *rocks.Options
	ro   *rocks.ReadOptions
	wo   *rocks.WriteOptions
	db   *rocks.DB

	tx *rocks.WriteBatch

	cache interface{}
}

// consumer.Shard implementation.
func (s *Shard) ID() consumer.ShardID              { return s.IDFixture }
func (s *Shard) Partition() topic.Partition        { return s.PartitionFixture }
func (s *Shard) Cache() interface{}                { return s.cache }
func (s *Shard) SetCache(c interface{})            { s.cache = c }
func (s *Shard) Database() *rocks.DB               { return s.db }
func (s *Shard) Transaction() *rocks.WriteBatch    { return s.tx }
func (s *Shard) ReadOptions() *rocks.ReadOptions   { return s.ro }
func (s *Shard) WriteOptions() *rocks.WriteOptions { return s.wo }

// Initializes a Shard & database backed by a temporary directory.
// TODO(johnny): Since this is test support, panic on error (rather than returning it).
func NewShard(prefix string) (*Shard, error) {
	var s = new(Shard)
	var err error

	s.tmpdir, err = ioutil.TempDir("", prefix)
	if err != nil {
		return nil, err
	}

	s.env = rocks.NewDefaultEnv()

	s.opts = rocks.NewDefaultOptions()
	s.opts.SetCreateIfMissing(true)

	s.ro = rocks.NewDefaultReadOptions()
	s.wo = rocks.NewDefaultWriteOptions()

	s.db, err = rocks.OpenDb(s.opts, s.tmpdir)
	if err != nil {
		return nil, err
	}

	s.tx = rocks.NewWriteBatch()
	return s, nil
}

// Flushes the current Shard transaction WriteBatch to the database.
func (s *Shard) FlushTransaction() error {
	var err = s.db.Write(s.wo, s.tx)
	s.tx.Clear()
	return err
}

// DatabaseContents enumerates and returns keys and values from the database
// as a map. As this is a test support method, it panics on iterator error.
func (s *Shard) DatabaseContent() map[string]string {
	var results = make(map[string]string)

	var it = s.db.NewIterator(s.ro)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		results[string(it.Key().Data())] = string(it.Value().Data())
	}
	if err := it.Err(); err != nil {
		panic(err.Error())
	}
	return results
}

// Closes and removes the Shard database.
func (s *Shard) Close() error {
	s.opts.Destroy()
	s.ro.Destroy()
	s.wo.Destroy()
	s.db.Close()
	s.env.Destroy()
	return os.RemoveAll(s.tmpdir)
}
