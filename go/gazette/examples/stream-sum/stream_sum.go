// Package stream_sum is an example application consisting of three stages:
//
// 1) A `chunker` job randomly generates a number of unique "streams", with
// stream content emitted across a number of interleaved data chunks.
//
// 2) A `summer` consumer accumulates stream chunks and computes a running
// SHA1-sum of each stream's content. When the stream is completed,
// the `summer` consumer emits a final sum to an output journal.
//
// 3) Having written a complete stream, the `chunker` job confirms that the
// correct sum is written to the output journal.
//
// The `chunker` and `summer` tasks may be independently scaled, and are
// invariant to process failures and restarts.
//
// The stream-sum example application is also a sneaky integration test:
// it actively verifies processing guarantees provided by Gazette, such as
// exactly-once semantics and bounds on end-to-end latency, and fails if those
// properties are not met. stream-sum is used in Gazette's continuous
// integration and chaos-testing suites.
package stream_sum

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	store_rocksdb "go.gazette.dev/core/consumer/store-rocksdb"
	store_sqlite "go.gazette.dev/core/consumer/store-sqlite"
	"go.gazette.dev/core/labels"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

// ChunkerConfig is the configuration used by the `chunker` job binary.
type ChunkerConfig struct {
	Chunker struct {
		mbp.ZoneConfig
		Streams int `long:"streams" default:"-1" description:"Number of streams each worker should create. <0 for infinite"`
		Chunks  int `long:"chunks" default:"100" description:"Number of chunks per stream"`
		Workers int `long:"workers" default:"4" description:"Number of parallel workers"`
	} `group:"Chunker" namespace:"chunker" env-namespace:"CHUNKER"`

	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

// FinalSumsJournal to which final stream sums are written.
const FinalSumsJournal pb.Journal = "examples/stream-sum/sums"

// StreamID uniquely identifies a stream.
type StreamID [16]byte

// Chunk is an ordered slice of stream content.
type Chunk struct {
	UUID  message.UUID
	ID    StreamID // Unique ID of the stream.
	SeqNo int      // Monotonic sequence number, starting from 1.
	Data  []byte   // Raw data included in the Value. If empty, this is the stream's final chunk.
}

func (c *Chunk) SetUUID(uuid message.UUID)                     { c.UUID = uuid }
func (c *Chunk) GetUUID() message.UUID                         { return c.UUID }
func (c *Chunk) NewAcknowledgement(pb.Journal) message.Message { return new(Chunk) }

// Sum represents a partial or final CRC64 sum of a stream.
type Sum struct {
	UUID  message.UUID
	ID    StreamID // Unique ID of the stream.
	SeqNo int      // SeqNo of last Chunk summed over.
	Value uint64   // Computed sum through SeqNo.
}

func (s *Sum) SetUUID(uuid message.UUID)                     { s.UUID = uuid }
func (s *Sum) GetUUID() message.UUID                         { return s.UUID }
func (s *Sum) NewAcknowledgement(pb.Journal) message.Message { return new(Sum) }

// Update folds a Chunk into this Sum, returning whether this is the last Chunk of the Stream.
func (s *Sum) Update(chunk Chunk) (done bool, err error) {
	if chunk.ID != s.ID {
		return true, fmt.Errorf("invalid chunk.ID (%x; sum.ID %x)", chunk.ID, s.ID)
	} else if chunk.SeqNo != s.SeqNo+1 {
		return true, fmt.Errorf("invalid chunk.SeqNo (%d; sum.SeqNo %d; id %x)",
			chunk.SeqNo, s.SeqNo, chunk.ID)
	}

	s.SeqNo = chunk.SeqNo
	s.Value = crc64.Update(s.Value, ecmaTable, chunk.Data)
	return len(chunk.Data) == 0, nil
}

// GenerateAndVerifyStreams is the main routine of the `chunker` job. It
// generates and verifies streams based on the ChunkerConfig.
func GenerateAndVerifyStreams(ctx context.Context, cfg *ChunkerConfig) error {
	var conn = cfg.Broker.MustDial(ctx)
	var rjc = pb.NewRoutedJournalClient(pb.NewJournalClient(conn), cfg.Broker.BuildRouter())
	var as = client.NewAppendService(ctx, rjc)

	var chunksMapping, err = newChunkMapping(ctx, rjc)
	if err != nil {
		return fmt.Errorf("building chunks mapping: %w", err)
	}

	// Issue an empty append transaction (a write barrier) to determine the
	// current write-head of the |FinalSumsJournal|, prior to emitting any chunks.
	var barrier = as.StartAppend(pb.AppendRequest{Journal: FinalSumsJournal}, nil)
	if err = barrier.Release(); err == nil {
		_, err = <-barrier.Done(), barrier.Err()
	}
	if err != nil {
		return fmt.Errorf("writing barrier: %w", err)
	}

	// All workers share a Publisher.
	var clock = message.NewClock(time.Now())
	var pub = message.NewPublisher(as, &clock)
	var doneCh = make(chan struct{})

	for w := 0; w != cfg.Chunker.Workers; w++ {
		go func() {

			var rr = client.NewRetryReader(ctx, rjc, pb.ReadRequest{
				Journal:    barrier.Response().Commit.Journal,
				Offset:     barrier.Response().Commit.End,
				Block:      true,
				DoNotProxy: !rjc.IsNoopRouter(),
			})
			var chunkCh = make(chan Chunk)
			var verifyCh = make(chan Sum)
			var actualCh = make(chan Sum)

			go pumpSums(rr, actualCh)
			go generate(cfg.Chunker.Streams, cfg.Chunker.Chunks, verifyCh, chunkCh)

			verify(func(chunk Chunk) {
				var _, err = pub.PublishCommitted(chunksMapping, &chunk)
				mbp.Must(err, "publishing chunk")
			}, chunkCh, verifyCh, actualCh, chunksMapping)

			doneCh <- struct{}{}
		}()
	}

	var ticker = time.NewTicker(time.Second)
	defer ticker.Stop()

	for w := 0; w != cfg.Chunker.Workers; {
		select {
		case t := <-ticker.C:
			clock.Update(t)
		case _ = <-doneCh:
			w++ // Worker finished.
		}
	}
	return conn.Close()
}

// Summer consumes stream chunks, aggregates chunk data, and emits final sums.
// It implements the runconsumer.Application interface.
type Summer struct{}

// NewConfig returns a new BaseConfig.
func (Summer) NewConfig() runconsumer.Config { return new(struct{ runconsumer.BaseConfig }) }

// InitApplication is a no-op, as Summer provides no client-facing APIs.
func (Summer) InitApplication(args runconsumer.InitArgs) error { return nil }

// NewStore builds a RocksDB or SQLite store for the Shard. consumer.Application implementation.
func (Summer) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	switch v := strings.ToLower(shard.Spec().LabelSet.ValueOf("store")); v {
	case "rocksdb":
		var store = store_rocksdb.NewStore(rec)
		store.Cache = make(map[StreamID]Sum)
		return store, store.Open()
	case "sqlite":
		var store, err = store_sqlite.NewStore(rec)
		if err != nil {
			return nil, err
		}
		store.Cache = make(map[StreamID]Sum)

		return store, store.Open(
			// Store bootstrap:
			`CREATE TABLE IF NOT EXISTS sums (
				id BLOB PRIMARY KEY NOT NULL,
				seq_no  INTEGER,
				val     BLOB
			);`,
			// Querying for a current partial sum:
			`SELECT seq_no, val FROM sums WHERE id = ?;`,
			// Updating a partial sum:
			`INSERT INTO sums(id, seq_no, val) VALUES(:id, :seqno, :val)
				ON CONFLICT(id) DO UPDATE SET seq_no=:seqno, val=:val;`,
		)
	default:
		return nil, fmt.Errorf("expected ShardSpec label 'store' to be one"+
			" of 'rocksdb' or 'sqlite' (got %q)", v)
	}
}

// NewMessage returns a Chunk message. consumer.Application implementation.
func (Summer) NewMessage(*pb.JournalSpec) (message.Message, error) { return new(Chunk), nil }

// ConsumeMessage folds a Chunk into its respective partial stream sum.
// If the Chunk represents a stream EOF, it emits a final sum.
// consumer.Application implementation.
func (Summer) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope, pub *message.Publisher) error {
	var chunk = env.Message.(*Chunk)
	var cache map[StreamID]Sum
	var sum Sum

	switch s := store.(type) {
	case *store_rocksdb.Store:
		sum, cache = Summer{}.loadSumRocks(s, chunk)
	case *store_sqlite.Store:
		sum, cache = Summer{}.loadSumSQLite(s, chunk)
	default:
		panic("invalid store")
	}

	if done, err := sum.Update(*chunk); err != nil {
		log.WithFields(log.Fields{"err": err, "id": chunk.ID}).Fatal("updating record")
	} else if done {
		// Publish completed sum.
		if _, err = pub.PublishUncommitted(finalSumsMapping, &sum); err != nil {
			log.WithFields(log.Fields{"err": err, "id": sum.ID}).Fatal("publishing sum")
		}
	}

	// Retain the partial sum for the next chunk or flush.
	cache[sum.ID] = sum
	return nil
}

func (Summer) loadSumRocks(rdb *store_rocksdb.Store, chunk *Chunk) (Sum, map[StreamID]Sum) {
	var cache = rdb.Cache.(map[StreamID]Sum)
	var sum, ok = cache[chunk.ID]
	if !ok {
		// Fill from database.
		if b, err := rdb.DB.GetBytes(rdb.ReadOptions, chunk.ID[:]); err != nil {
			log.WithFields(log.Fields{"err": err, "id": chunk.ID}).Fatal("reading db")
		} else if len(b) == 0 {
			// Miss. Initialize a new stream.
			sum.ID = chunk.ID
		} else if err = json.Unmarshal(b, &sum); err != nil {
			log.WithFields(log.Fields{"err": err, "id": chunk.ID}).Fatal("unmarshalling record")
		}
	}
	return sum, cache
}

func (Summer) loadSumSQLite(sdb *store_sqlite.Store, chunk *Chunk) (Sum, map[StreamID]Sum) {
	var cache = sdb.Cache.(map[StreamID]Sum)
	var ctx = context.Background()

	var sum, ok = cache[chunk.ID]
	if !ok {
		sum.ID = chunk.ID
		var value []byte

		if txn, err := sdb.Transaction(ctx, nil); err != nil {
			log.WithField("err", err).Fatal("opening sqlite txn")
		} else if err = txn.StmtContext(ctx, sdb.Stmts[0]).
			QueryRow(chunk.ID[:]).Scan(&sum.SeqNo, &value); err == sql.ErrNoRows {
			// Pass.
		} else if err != nil {
			log.WithFields(log.Fields{"err": err, "id": sum.ID}).
				Fatal("reading sum from sqlite")
		} else {
			sum.Value = binary.LittleEndian.Uint64(value)
		}
	}
	return sum, cache
}

// FinalizeTxn marshals partial stream sums to the |store| to ensure persistence
// across consumer transactions. consumer.Application implementation.
func (Summer) FinalizeTxn(shard consumer.Shard, store consumer.Store, _ *message.Publisher) error {
	switch s := store.(type) {
	case *store_rocksdb.Store:
		Summer{}.finalizeRocksTxn(s)
	case *store_sqlite.Store:
		Summer{}.finalizeSqlite(s)
	default:
		panic("invalid store")
	}
	return nil
}

func (Summer) finalizeRocksTxn(rdb *store_rocksdb.Store) {
	var cache = rdb.Cache.(map[StreamID]Sum)

	for id, sum := range cache {
		if b, err := json.Marshal(sum); err != nil {
			log.WithFields(log.Fields{"err": err}).Fatal("marshalling record")
		} else {
			rdb.WriteBatch.Put(id[:], b)
		}
		delete(cache, id) // Clear for next transaction.
	}
}

func (Summer) finalizeSqlite(sdb *store_sqlite.Store) {
	var cache = sdb.Cache.(map[StreamID]Sum)
	var ctx = context.Background()

	for id, sum := range cache {
		var value [8]byte
		binary.LittleEndian.PutUint64(value[:], sum.Value)

		if txn, err := sdb.Transaction(ctx, nil); err != nil {
			log.WithField("err", err).Fatal("opening sqlite txn")
		} else if _, err = txn.StmtContext(ctx, sdb.Stmts[1]).Exec(
			sql.Named("id", sum.ID[:]),
			sql.Named("seqno", sum.SeqNo),
			sql.Named("val", value[:]),
		); err != nil {
			log.WithFields(log.Fields{"err": err, "id": sum.ID}).
				Fatal("writing sum to sqlite")
		}
		delete(cache, id) // Clear for next transaction.
	}
}

func generate(numStreams, chunksPerStream int, doneCh chan<- Sum, outCh chan<- Chunk) {
	var streams []Sum

	defer close(outCh)
	defer close(doneCh)

	for {
		// Select an existing stream (if i < len(streams)) or new stream (if i == len(streams)).
		var N = len(streams)

		if numStreams != 0 {
			N += 1
		} else if N == 0 {
			return // Termination condition.
		}
		var i = prngInt(N)

		// Create a new stream.
		if i == len(streams) {
			streams = append(streams, Sum{ID: prngStreamID()})
			numStreams--
		}

		var data []byte
		if streams[i].SeqNo < chunksPerStream {
			data = fillPRNG(make([]byte, 32))
		}

		var chunk = Chunk{
			ID:    streams[i].ID,
			SeqNo: streams[i].SeqNo + 1,
			Data:  data,
		}

		done, err := streams[i].Update(chunk)
		mbp.Must(err, "update failed (invalid SeqNo?)", "ID", chunk.ID)

		outCh <- chunk

		if done {
			doneCh <- streams[i]
			copy(streams[i:], streams[i+1:])
			streams = streams[:len(streams)-1]
		}
	}
}

func verify(emit func(Chunk), chunkCh <-chan Chunk, verifyCh, actualCh <-chan Sum, mapFn message.MappingFunc) {
	var timer = time.NewTimer(0)
	<-timer.C

	// Sequence of deadlines from now, after each of which we print a warning that
	// the expected sum is delayed. On reaching the final deadline we log.Fatal.
	// As intuition on the choice of backoffs here:
	// * We default to 20s Etcd leases. It takes that long before a dead broker
	//   or consumer is detected.
	// * Once a replacement is allocated, clients max out at 5s of backoff.
	//   If an attempt happens _just_ before recovery, this implies nearly 10s
	//   of total delay before the client detects the failure has cleared.
	// * Add 10 seconds of extra headroom, as the test is likely hosing the machine.
	var timeoutSeq = []time.Duration{
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		6 * time.Second,
		8 * time.Second,
		10 * time.Second,
		15 * time.Second,
		20 * time.Second,
		25 * time.Second,
		30 * time.Second,
		35 * time.Second,
		40 * time.Second,
	}

	for chunkCh != nil || verifyCh != nil {
		// Emit chunks as they're generated, and verify as they complete. If a
		// stream is ready for verification, we block and wait for it's computed
		// sum prior to emitting any further chunks. This precludes races were
		// expected and actual stream sums potentially arrive in different orders
		// (since we can't emit a chunk which would complete a next stream before
		// we've verified the last one).

		select {
		case chunk, ok := <-chunkCh:
			if !ok {
				chunkCh = nil
				continue
			}
			emit(chunk)

		case expect, ok := <-verifyCh:
			if !ok {
				verifyCh = nil
				continue
			}

			var start = time.Now()
			var now = start
			var nextTimeout = timeoutSeq[1:]
			timer.Reset(timeoutSeq[0])

			for {
				var actual Sum

				select {
				case now = <-timer.C:
					var journal, _, _ = mapFn(&Chunk{ID: expect.ID})

					if len(nextTimeout) != 0 {
						log.WithFields(log.Fields{
							"expect":  fmt.Sprintf("%x", expect.ID),
							"journal": journal,
							"delay":   now.Sub(start).Seconds(),
						}).Warn("delay waiting for expected sum")

						timer.Reset(start.Add(nextTimeout[0]).Sub(now))
						nextTimeout = nextTimeout[1:]
					} else {
						log.WithFields(log.Fields{
							"expect":  fmt.Sprintf("%x", expect.ID),
							"journal": journal,
							"delay":   now.Sub(start).Seconds(),
						}).Fatal("timeout waiting for expected sum")
					}
					continue

				case actual = <-actualCh:
					// Pass.
				}

				if actual.ID == expect.ID {
					if actual.SeqNo != expect.SeqNo || actual.Value != expect.Value {
						log.WithFields(log.Fields{"actual": actual, "expect": expect}).
							Fatal("mis-matched sum!")
					}

					if !timer.Stop() {
						<-timer.C
					}
					break
				}
			}
		}
	}
}

// pumpSums reads committed Sum messages from the RetryReader, and dispatches to |ch|.
func pumpSums(rr *client.RetryReader, ch chan<- Sum) {
	var it = message.NewReadCommittedIter(rr,
		func(spec *pb.JournalSpec) (i message.Message, e error) { return new(Sum), nil },
		message.NewSequencer(nil, nil, 16))

	for {
		if env, err := it.Next(); errors.Is(err, context.Canceled) || err == io.EOF {
			return
		} else if err != nil {
			log.WithField("err", err).Fatal("reading sum")
		} else if message.GetFlags(env.GetUUID()) != message.Flag_ACK_TXN {
			ch <- *env.Message.(*Sum)
		}
	}
}

// newChunkMapping returns a MappingFunc over journals holding chunks.
func newChunkMapping(ctx context.Context, jc pb.JournalClient) (message.MappingFunc, error) {
	var parts = client.NewWatchedList(ctx, jc,
		pb.ListRequest{
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet(labels.MessageType, "stream_sum.Chunk"),
			},
		},
		nil,
	)
	if err := <-parts.UpdateCh(); err != nil {
		return nil, err
	}

	for len(parts.List().Journals) == 0 {
		log.Info("waiting for chunk partition journals to be created")
		<-parts.UpdateCh()
	}

	return message.ModuloMapping(func(m message.Mappable, w io.Writer) {
		_, _ = w.Write(m.(*Chunk).ID[:])
	}, parts.List), nil
}

// finalSumsMapping is a MappingFunc to FinalSumsJournal.
var finalSumsMapping message.MappingFunc = func(msg message.Mappable) (pb.Journal, string, error) {
	return FinalSumsJournal, labels.ContentType_JSONLines, nil
}

// fillPRNG generates fast but high-quality random entropy into the provided byte slice.
func fillPRNG(b []byte) []byte {
	var stream = prngSource.Get().(cipher.Stream)
	stream.XORKeyStream(b[:], b[:])
	prngSource.Put(stream)
	return b
}

var prngSource = sync.Pool{
	New: func() interface{} {
		var key [32]byte
		var iv [aes.BlockSize]byte

		// Generate a random AES key and initialization vector.
		for _, b := range [][]byte{key[:], iv[:]} {
			if _, err := io.ReadFull(rand.Reader, b); err != nil {
				panic(err) // rand.Reader should never error.
			}
		}
		if aesCipher, err := aes.NewCipher(key[:]); err != nil {
			panic(err) // Should never error (given correct |key| size).
		} else {
			return cipher.NewCTR(aesCipher, iv[:])
		}
	},
}

func prngStreamID() (id StreamID) {
	fillPRNG(id[:])
	return
}

func prngInt(N int) int {
	var b [4]byte
	fillPRNG(b[:])
	return int(binary.LittleEndian.Uint32(b[:])) % N
}

var ecmaTable = crc64.MakeTable(crc64.ECMA)
