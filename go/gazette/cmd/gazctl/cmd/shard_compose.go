package cmd

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/recoverylog"
)

var shardComposeCmd = &cobra.Command{
	Use:   "compose [input-path-one] [input-path-two] ... [output-path]",
	Short: "Combine zero or more sorted input sources into a new RocksDB.",
	Long: `Compose enumerates provided input sources (each either a path to a
RocksDB, or a sorted flat file of encoded keys/values in the standard
"ldb dump / scan --hex" format), optionally applies a provided plugin filter,
and builds a new RocksDB holding the resulting key/value set.

If two or more sources provide the same key, by default, the key from the source appearing
first in the argument list has precedence, and values of other sources are
dropped. Follow the instructions in shard_compose_plugins.go if you would like to customize 
this behavior.

Optional "--consumer-offset" and "--consumer-journal" flags are provided to help
with a special-but-common case, where one desires to update the check-pointed
consumer offset of a shard. If specified, compose will upsert a key/value of
the specified offset checkpoint, with precedence over all other sources.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 || (consumerOffset != 0 && consumerJournal == "") {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}
		var srcPaths, tgtPath = args[:len(args)-1], args[len(args)-1]

		var opts = rocks.NewDefaultOptions()
		if plugin := consumerPlugin(); plugin != nil {
			if initer, _ := plugin.(consumer.OptionsIniter); initer != nil {
				initer.InitOptions(opts)
			}
		}

		// Build iterators for all input sequences.
		var iterFuncs []iterFunc

		// A specified --consumer-offset has highest precedence.
		if consumerJournal != "" {
			iterFuncs = append(iterFuncs,
				newConsumerOffsetIterFunc(journal.Name(consumerJournal), consumerOffset))
		}

		// Followed by arguments in argument order.
		for _, p := range srcPaths {
			var fn iterFunc
			var err error

			if _, err = os.Stat(path.Join(p, "CURRENT")); err == nil {
				var db *rocks.DB
				if db, err = rocks.OpenDbForReadOnly(opts, p, true); err == nil {
					fn = newDBIterFunc(db)
				}
			} else {
				var fin *os.File
				if fin, err = os.Open(p); err == nil {
					fn = newHexIterFunc(bufio.NewReader(fin))
				}
			}

			if err != nil {
				log.WithFields(log.Fields{"path": p, "err": err}).Fatal("failed to open input source")
			}
			iterFuncs = append(iterFuncs, fn)
		}

		// Combine into a single sorted sequence via a heap iterator.
		var iterFunc = newHeapIterFunc(First, iterFuncs...)

		// Optionally wrap with a filtering iterator.
		if plugin := consumerPlugin(); plugin != nil {
			if filterer, ok := plugin.(consumer.Filterer); ok {
				iterFunc = newFilterIterFunc(filterer, iterFunc)
			}
		}

		var fsm *recoverylog.FSM
		var err error

		if composeRecoveryLog != "" {
			fsm, err = recoverylog.NewFSM(recoverylog.FSMHints{Log: journal.Name(composeRecoveryLog)})
			if err != nil {
				log.WithField("err", err).Fatal("NewFSM failed")
			}
			author, err := recoverylog.NewRandomAuthorID()
			if err != nil {
				log.WithField("err", err).Fatal("NewRandomAuthorID failed")
			}

			var recorder = recoverylog.NewRecorder(fsm, author, len(tgtPath), writeService())
			opts.SetEnv(rocks.NewObservedEnv(recorder))
		}

		opts.SetCreateIfMissing(true)
		opts.SetErrorIfExists(true)
		opts.PrepareForBulkLoad()
		// Disable any configured compaction filter, as it will prevent trivial
		// file moves during the final manual compaction step and isn't applied
		// on L0 writes anyway.
		opts.SetCompactionFilter(nil)

		tgtDB, err := rocks.OpenDb(opts, tgtPath)
		if err != nil {
			log.WithField("err", err).Fatal("failed to open target database")
		}

		var wb = rocks.NewWriteBatch()
		var wo = rocks.NewDefaultWriteOptions()
		wo.SetSync(false)
		wo.DisableWAL(true)

		// Consume keys & values from the iterator.
		key, value, err := iterFunc(nil, nil)
		for count := 0; err == nil; key, value, err = iterFunc(key, value) {
			wb.Put(key, value)

			if count++; count%writeBatchSize == 0 {
				tgtDB.Write(wo, wb)
				wb.Clear()
			}
		}
		if err != io.EOF {
			log.WithField("err", err).Fatal("failed to compose DB")
		}

		// Flush final records and memtables.
		tgtDB.Write(wo, wb)
		if err = tgtDB.Flush(rocks.NewDefaultFlushOptions()); err != nil {
			log.WithField("err", err).Fatal("failed to Flush")
		}

		// Manually compact |trgDB|, which (due to the earlier PrepareForBulkLoad)
		// has generated only L0 SST files. Fortunately, because we enumerated
		// |srcDB| in sorted order such that SSTs are non-overlapping, compaction
		// is achieved by trivially moving existing files to lower LSM tree levels.
		tgtDB.CompactRange(rocks.Range{})
		tgtDB.Close()

		if fsm != nil {
			var tgtHintsPath = path.Join(tgtPath, "fsm_hints.json")

			var tgtHints, err = os.Create(tgtHintsPath)
			if err != nil {
				log.WithField("err", err).Fatal("failed to create fsm_hints.json")
			}
			defer tgtHints.Close()

			if err = json.NewEncoder(tgtHints).Encode(fsm.BuildHints()); err != nil {
				log.WithField("err", err).Fatal("failed to Marshal FSMHints")
			}
			log.WithField("path", tgtHintsPath).Info("wrote hints")
		}
	},
}

// newHexIterFunc returns an iterFunc which parses sequential "hex-encoded-key hex-encoded-value\n"
// records from the bufio.Reader. A variety of field delineations are supported (eg, " : " & " ==> ",
// both formats produced by the "ldb dump" / "ldb scan" tool).
func newHexIterFunc(br *bufio.Reader) iterFunc {
	var readHex = func(b []byte, delim byte) ([]byte, error) {
		var t, err = br.Peek(2)
		if err != nil {
			return nil, err
		} else if t[0] != '0' && t[1] != 'x' {
			return nil, fmt.Errorf("invalid hex prefix (expected '0x'): %v", t)
		}
		br.Discard(2)

		b = b[:0]
		for done := false; !done; {
			var t, err = br.ReadSlice(delim)

			if err == nil {
				t = t[:len(t)-1] // Trim |delim|.
				done = true
			} else if err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			} else if err != bufio.ErrBufferFull {
				return nil, err
			}

			_, err = hex.Decode(t, t)
			if err != nil {
				return nil, err
			}
			b = append(b, t[:len(t)/2]...)
		}
		return b, nil
	}

	return func(prevKey, prevValue []byte) (key, value []byte, err error) {
		if key, err = readHex(prevKey, ' '); err != nil {
			return
		}
		// Skip past field separator bytes to the hex value.
		for done := false; !done; {
			var t byte

			if t, err = br.ReadByte(); err != nil {
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				return
			}

			switch t {
			case ' ', ':', '=', '>': // Pass.
			default:
				br.UnreadByte()
				done = true
			}
		}

		value, err = readHex(prevValue, '\n')
		return
	}
}

// newDBIterFunc returns an iterFunc which walks the database in ascending sorted order.
func newDBIterFunc(db *rocks.DB) iterFunc {
	var ro = rocks.NewDefaultReadOptions()
	ro.SetFillCache(false)

	var it = db.NewIterator(ro)
	it.SeekToFirst()

	return func(prevKey, prevValue []byte) (key, value []byte, err error) {
		if !it.Valid() {
			if err = it.Err(); err == nil {
				err = io.EOF
			}

			it.Close()
			ro.Destroy()
			return
		}

		// We must copy because |it| owns it's Key()/Value() memory.
		key = append(prevKey[:0], it.Key().Data()...)
		value = append(prevValue[:0], it.Value().Data()...)
		it.Next()

		return
	}
}

// newFilterIterFunc returns an iterFunc which applies a consumer.Filterer to another iterFunc.
func newFilterIterFunc(filter consumer.Filterer, fn iterFunc) iterFunc {
	return func(prevKey, prevValue []byte) (key, value []byte, err error) {
		for {
			if key, value, err = fn(prevKey, prevValue); err != nil {
				return
			}

			if remove, newVal := filter.Filter(0, key, value); remove {
				continue
			} else if newVal != nil {
				value = newVal
			}
			return
		}
	}
}

// newConsumerOffsetIterFunc returns an iterFunc which returns a single key, representing
// the encoded consumer offset checkpoint of journal |name| at |offset|.
func newConsumerOffsetIterFunc(name journal.Name, offset int64) iterFunc {
	var offsetKey = consumer.AppendOffsetKeyEncoding(nil, name)
	var offsetValue = consumer.AppendOffsetValueEncoding(nil, offset)

	return func(prevKey, prevValue []byte) (key, value []byte, err error) {
		if offsetKey == nil {
			err = io.EOF
			return
		}
		key = append(prevKey[:0], offsetKey...)
		value = append(prevValue[:0], offsetValue...)
		offsetKey = nil
		return
	}
}

// newHeapIterFunc returns an iterFunc which merges multiple other iterFuncs,
// each of which must be in ascending sorted order, and producing a single
// sorted output. Where keys collide, the value of the first iterFunc appearing
// in the argument list is used, and others are dropped.
func newHeapIterFunc(merge Merge, iterFuncs ...iterFunc) iterFunc {
	var iters iterHeap

	for i, fn := range iterFuncs {
		var key, value, err = fn(nil, nil)

		heap.Push(&iters, iter{
			fn:         fn,
			key:        key,
			precedence: i,
			value:      value,
			err:        err,
		})
	}

	return func(prevKey, prevValue []byte) (currKey, mergedValue []byte, err error) {
		var values [][]byte
		for len(iters) != 0 {
			var it = heap.Pop(&iters).(iter)

			if it.err == io.EOF {
				continue
			} else if it.err != nil {
				err = it.err
				return
			}

			if len(currKey) == 0 {
				if bytes.Compare(prevKey, it.key) > 0 {
					err = fmt.Errorf("invalid iterator order: %x > %x", prevKey, it.key)
					return
				} else if bytes.Equal(prevKey, it.key) {
					err = fmt.Errorf("iterator did not advance between invocations: %x == %x", prevKey, it.key)
					return
				}
				currKey = append([]byte{}, it.key...)
			}

			if !bytes.Equal(currKey, it.key) {
				// we've exhausted the current key, so return a merged value and put the value we peeked at back
				heap.Push(&iters, it)
				mergedValue = merge(currKey, values)
				return;
			}

			values = append(values, append([]byte{}, it.value...))

			it.key, it.value, it.err = it.fn(it.key, it.value)
			heap.Push(&iters, it)
		}
		if (len(values) > 0) {
			mergedValue = First(currKey, values)
		} else {
			err = io.EOF
		}
		return
	}
}

type iterFunc func(prevKey, prevValue []byte) (key, value []byte, err error)

type iter struct {
	fn         iterFunc
	precedence int
	key, value []byte
	err        error
}

type iterHeap []iter

func (h iterHeap) Len() int      { return len(h) }
func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Order on error, then key, then precedence; eg, pop errors first.
func (h iterHeap) Less(i, j int) bool {
	if h[j].err != nil {
		return false
	} else if h[i].err != nil {
		return true
	}

	if c := bytes.Compare(h[i].key, h[j].key); c != 0 {
		return c < 0
	}
	return h[i].precedence < h[j].precedence
}

func (h *iterHeap) Push(x interface{}) { *h = append(*h, x.(iter)) }

func (h *iterHeap) Pop() interface{} {
	var old = *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

var (
	composeRecoveryLog string
	consumerJournal    string
	consumerOffset     int64
	mergeFunction      string
)

const writeBatchSize = 200

func init() {
	shardCmd.AddCommand(shardComposeCmd)

	shardComposeCmd.Flags().StringVarP(&composeRecoveryLog, "recovery-log", "r", "",
		"Recovery log in which to record the composed shard. By default, no recording is done.")
	shardComposeCmd.Flags().StringVarP(&consumerJournal, "consumer-journal", "j", "",
		"Journal for which to update the check-pointed consumption offset. By default, the offset is not modified.")
	shardComposeCmd.Flags().Int64VarP(&consumerOffset, "consumer-offset", "o", 0,
		"Byte offset for which to update the check-pointed consumption offset. --consumer-journal must be set if this is.")
	shardComposeCmd.Flags().StringVarP(&mergeFunction, "merge-function", "m", "first",
		"Merge function to decide what value to resolve to when multiple input sources have the same key. Defaults to \"first\".")
}
