// +build !norocksdb

package consumer

import (
	"os"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	"github.com/cockroachdb/cockroach/util/encoding"
	log "github.com/sirupsen/logrus"
	rocks "github.com/tecbot/gorocksdb"
)

// RocksDBStore implements the Store interface.
type RocksDBStore struct {
	DB           *rocks.DB
	Env          *rocks.Env
	Options      *rocks.Options
	ReadOptions  *rocks.ReadOptions
	WriteBatch   *rocks.WriteBatch
	WriteOptions *rocks.WriteOptions

	// Cache is a convenient mechanism for consumers to associate shard-specific,
	// in-memory state with a RocksDBStore, typically for performance reasons.
	// Examples might include:
	//
	// - Records we expect to reduce / aggregate over multiple times in a consumer
	//   transaction, and want to write to the DB only once per transaction (ie,
	//   as part of a consumer Flush).
	// - An LRU of "hot" records we expect to reference again soon.
	//
	// The representation of Cache is up to the consumer; it is not directly used
	// by RocksDBStore.
	Cache interface{}

	rec *recoverylog.Recorder
	dir string
}

// NewRocksDBStore builds a RocksDBStore which is prepared to open its database,
// but has not yet done so. The caller may wish to further tweak Options and Env
// settings, and should then call Open to open the database.
func NewRocksDBStore(rec *recoverylog.Recorder, dir string) *RocksDBStore {
	return &RocksDBStore{
		Env:          rocks.NewObservedEnv(recoverylog.RecordedRocksDB{Recorder: rec}),
		Options:      rocks.NewDefaultOptions(),
		ReadOptions:  rocks.NewDefaultReadOptions(),
		WriteBatch:   rocks.NewWriteBatch(),
		WriteOptions: rocks.NewDefaultWriteOptions(),
		rec:          rec,
		dir:          dir,
	}
}

// Open the RocksDB. After Open, further updates to Env or Options are ignored.
func (s *RocksDBStore) Open() (err error) {
	// The DB must use our recorded environment for file IO, and should initialize
	// itself if the recovered directory is empty.
	s.Options.SetEnv(s.Env)
	s.Options.SetCreateIfMissing(true)

	// The MANIFEST file is a WAL of database file state, including current live
	// SST files and their begin & ending key ranges. A new MANIFEST-00XYZ is
	// created at database start, where XYZ is the next available sequence number,
	// and CURRENT is updated to point at the live MANIFEST. By default MANIFEST
	// files may grow to 4GB, but they are typically written very slowly and thus
	// artificially inflate the recovery log horizon. We use a much smaller limit
	// to encourage more frequent compactions into new files.
	s.Options.SetMaxManifestFileSize(1 << 17) // 131072 bytes.

	s.DB, err = rocks.OpenDb(s.Options, s.dir)
	return
}

// Recorder of the RocksDB.
func (s *RocksDBStore) Recorder() *recoverylog.Recorder { return s.rec }

// FetchJournalOffsets returns a map of Journals and offsets captured by the DB.
func (s *RocksDBStore) FetchJournalOffsets() (offsets map[pb.Journal]int64, err error) {
	var prefix = appendOffsetKeyEncoding(nil, "")
	offsets = make(map[pb.Journal]int64)

	var it = s.DB.NewIterator(s.ReadOptions)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		var key, val = it.Key().Data(), it.Value().Data()

		var name string
		var offset int64

		if _, name, err = encoding.DecodeStringAscending(key[len(prefix):], nil); err != nil {
			err = extendErr(err, "decoding offset key %v", string(key))
		} else if _, offset, err = encoding.DecodeVarintAscending(val); err != nil {
			err = extendErr(err, "decoding offset %s value %x", name, val)
		}

		it.Key().Free()
		it.Value().Free()

		if err != nil {
			return
		} else {
			offsets[pb.Journal(name)] = offset
		}
	}
	return
}

// Flush merges |offsets| into the WriteBatch, and atomically writes it to the DB.
func (s *RocksDBStore) Flush(offsets map[pb.Journal]int64) error {
	// Persist updated journal offsets alongside other WriteBatch content.
	for journal, offset := range offsets {
		s.WriteBatch.Put(
			appendOffsetKeyEncoding(nil, journal),
			appendOffsetValueEncoding(nil, offset))
	}
	if err := s.DB.Write(s.WriteOptions, s.WriteBatch); err != nil {
		return err
	}
	s.WriteBatch.Clear()

	return nil
}

// Destroy the RocksDBStore
func (s *RocksDBStore) Destroy() {
	if s.DB != nil {
		s.DB.Close() // Blocks until all background compaction has completed.
		s.DB = nil
	}
	s.Env.Destroy()
	s.Options.Destroy()
	s.ReadOptions.Destroy()
	s.WriteBatch.Destroy()
	s.WriteOptions.Destroy()

	if err := os.RemoveAll(s.dir); err != nil {
		log.WithFields(log.Fields{
			"dir": s.dir,
			"err": err,
		}).Error("failed to remove RocksDB directory")
	}
}

// appendOffsetKeyEncoding encodes |name| into a database key representing
// a consumer journal offset checkpoint. A |name| of "" will generate a
// key which prefixes all other offset key encodings.
func appendOffsetKeyEncoding(b []byte, journal pb.Journal) []byte {
	b = encoding.EncodeNullAscending(b)
	b = encoding.EncodeStringAscending(b, "mark")
	if journal != "" {
		b = encoding.EncodeStringAscending(b, journal.String())
	}
	return b
}

// appendOffsetValueEncoding encodes |offset| into a database value representing
// a consumer journal offset checkpoint.
func appendOffsetValueEncoding(b []byte, offset int64) []byte {
	return encoding.EncodeVarintAscending(b, offset)
}
