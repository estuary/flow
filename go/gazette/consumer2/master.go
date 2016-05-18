package consumer

import (
	"encoding/json"
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/api-server/varz"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/recoverylog"
)

var (
	maxConsumeQuantum = flag.Duration("maxConsumeQuantum", 200*time.Millisecond,
		"Max quantum of time a consumer may process messages before committing.")

	storeToEtcdInterval = time.Minute
	messageBufferSize   = 1 << 12 // 4096.

	// Buffered channel used to synchronize and enforce limits on maximum
	// transaction concurrency.
	txConcurrencyCh flaggedBufferedChan
)

type master struct {
	shard    ShardID
	localDir string

	// Etcd path into which FSMHints are stored.
	hintsPath string
	// Offsets read from Etcd at master initialization.
	etcdOffsets map[journal.Name]int64

	cancelCh  <-chan struct{} // master.serve exists when selectable.
	servingCh chan struct{}   // Blocks until master.serve exits.

	database *database
	cache    interface{}
}

func newMaster(shard *shard, tree *etcd.Node) (*master, error) {
	etcdOffsets, err := loadOffsetsFromEtcd(tree)
	if err != nil {
		return nil, err
	}

	if len(etcdOffsets) != 0 {
		log.WithFields(log.Fields{"shard": shard.id, "offsets": etcdOffsets}).
			Info("loaded Etcd offsets")
	}

	return &master{
		shard:       shard.id,
		localDir:    shard.localDir,
		hintsPath:   hintsPath(tree.Key, shard.id),
		etcdOffsets: etcdOffsets,
		cancelCh:    shard.cancelCh,
		servingCh:   make(chan struct{}),
	}, nil
}

func (m *master) serve(runner *Runner, replica *replica) {
	defer func() {
		if m.database != nil {
			m.database.teardown()
		}
		close(m.servingCh)
	}()

	if err := m.init(runner, replica); err == recoverylog.ErrPlaybackCancelled {
		log.WithFields(log.Fields{"shard": m.shard, "err": err}).Info("makeLive cancelled")
		return
	} else if err != nil {
		log.WithFields(log.Fields{"shard": m.shard, "err": err}).Error("master init failed")
		abort(runner, m.shard)
		return
	}

	messages, err := m.startPumpingMessages(runner)
	if err != nil {
		log.WithFields(log.Fields{"shard": m.shard, "err": err}).Error("message pump start")
		abort(runner, m.shard)
		return
	}

	if err = m.consumerLoop(runner, messages); err != nil {
		log.WithFields(log.Fields{"shard": m.shard, "err": err}).Error("consumer loop failed")
		abort(runner, m.shard)
		return
	}

	if runner.ShardPostStopHook != nil {
		runner.ShardPostStopHook(m)
	}
}

func (m *master) init(runner *Runner, replica *replica) error {
	// Ask replica to become "live" once caught up to the recovery-log write head.
	// This could potentially take a while, depending on how far behind we are.
	fsm, err := replica.player.MakeLive()
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{"shard": m.shard}).Info("makeLive finished")

	opts := rocks.NewDefaultOptions()
	if initer, ok := runner.Consumer.(OptionsIniter); ok {
		initer.InitOptions(opts)
	}

	if m.database, err = newDatabase(opts, fsm, m.localDir, runner.Gazette); err != nil {
		return err
	}

	if runner.ShardPreInitHook != nil {
		runner.ShardPreInitHook(m)
	}

	// Let the consumer initialize the context, if desired.
	if initer, ok := runner.Consumer.(ShardIniter); ok {
		if err := initer.InitShard(m); err != nil {
			return err
		}
	}
	return nil
}

func (m *master) startPumpingMessages(runner *Runner) (<-chan message.Message, error) {
	dbOffsets, err := loadOffsetsFromDB(m.database.DB, m.database.readOptions)
	if err != nil {
		return nil, err
	}
	log.WithFields(log.Fields{"shard": m.shard, "offsets": dbOffsets}).Info("loaded offsets")

	offsets := mergeOffsets(dbOffsets, m.etcdOffsets)

	// Begin pumping messages from consumed journals.
	var messages = make(chan message.Message, messageBufferSize)
	var pump = newPump(runner.Gazette, messages, m.cancelCh)
	var group *TopicGroup

	for _, g := range runner.Consumer.Groups() {
		if g.Name == m.shard.Group {
			group = &g
			break
		}
	}
	if group == nil {
		panic("could not locate topic group in consumer: " + m.shard.Group)
	}

	for name, topic := range group.JournalsForShard(m.shard.Index) {
		go pump.pump(topic, journal.Mark{Journal: name, Offset: offsets[name]})
	}
	return messages, nil
}

func (m *master) consumerLoop(runner *Runner, source <-chan message.Message) error {
	// Rate at which we publish recovery hints to Etcd.
	var storeToEtcdInterval = time.NewTicker(storeToEtcdInterval)

	// Timepoint at which the current transaction began.
	// Set on the first message of a new transaction.
	var txBegin time.Time
	// Upper-bounds the amount of time a transaction may take.
	// Reset on the first message of a new transaction.
	var txTimeoutTimer = time.NewTimer(0)
	// Number of messages processed in the current transaction.
	var txMessages int
	// Last offset for each journal observed in the current transaction.
	var txOffsets = make(map[journal.Name]int64)

	// Last sent time on txTimeoutTimer.
	var lastTimeoutTick time.Time
	// A write barrier which never selects.
	var zeroedAsyncAppend journal.AsyncAppend
	// Commit write-barrier of a previous transaction, which selects only after
	// the previous transaction has been sync'd by Gazette. We allow a current
	// transaction to process in the meantime (so we don't stall on Gazette I/O),
	// but it cannot commit until |lastWriteBarrier| is selectable.
	var lastWriteBarrier = &zeroedAsyncAppend

	// We synchronize transaction concurrency via |txConcurrencyCh|. We must
	// return a held lock on exit if we are in a transaction (txBegin != 0).
	defer func() {
		if !txBegin.IsZero() {
			txConcurrencyCh <- struct{}{}
		}
	}()

	for {
		var err error
		var msg message.Message

		// We allow messages to process in the current transaction only if we're
		// within the transaction timeout. Though we may stall an arbitrarily long
		// time waiting for |lastWriteBarrier|, we only wish to process messages
		// during the first |maxConsumeQuantum| of the transaction.
		var maybeSrc <-chan message.Message
		if txMessages == 0 || lastTimeoutTick.Before(txBegin.Add(*maxConsumeQuantum)) {
			maybeSrc = source
		}

		if txMessages == 0 || lastWriteBarrier.Ready != nil {
			// We must block until both conditions are resolved.
			select {
			case <-m.cancelCh:
				return nil
			case lastTimeoutTick = <-txTimeoutTimer.C:
				continue
			case <-lastWriteBarrier.Ready:
				if lastWriteBarrier.Error != nil {
					panic("expected write to resolve without error, or not resolve")
				}
				lastWriteBarrier = &zeroedAsyncAppend
				continue
			case msg = <-maybeSrc:
				goto CONSUME_MSG
			}
		} else {
			// We have a transaction with at least one message, and the previous
			// write barrier has completed. We're able to commit at any time.
			// We attempt to consume additional ready messages, but do not block.
			select {
			case <-m.cancelCh:
				return nil
			case lastTimeoutTick = <-txTimeoutTimer.C:
				continue
			case msg = <-maybeSrc:
				goto CONSUME_MSG
			default:
				goto COMMIT_TX
			}
		}

	CONSUME_MSG:

		// Does this message begin a new transaction?
		if txMessages == 0 {
			// The transaction begins only after a transaction lock is obtained.
			<-txConcurrencyCh
			txBegin = time.Now()
			txTimeoutTimer.Reset(*maxConsumeQuantum)
		}

		if err = runner.Consumer.Consume(msg, m, publisher{runner.Gazette}); err != nil {
			return err
		}

		txMessages += 1
		txOffsets[msg.Mark.Journal] = msg.Mark.Offset
		msg.Topic.PutMessage(msg.Value)

		if runner.ShardPostConsumeHook != nil {
			runner.ShardPostConsumeHook(msg, m)
		}
		continue // End of CONSUME_MSG.

	COMMIT_TX:

		if err = runner.Consumer.Flush(m, publisher{runner.Gazette}); err != nil {
			return err
		}
		storeOffsetsToDB(m.database.writeBatch, txOffsets)

		select {
		case <-storeToEtcdInterval.C:
			// It's time to write recovery hints to Etcd. We must be careful of
			// ordering here, as RocksDB may be performing background file operations.
			// We build hints *before* we commit, then sync to Etcd *after* the write
			// barrier resolves. This ensures hinted content is committed to the log
			// before it's observable by outside processes.
			var hints string
			if b, err := json.Marshal(m.database.recorder.BuildHints()); err != nil {
				return err
			} else {
				hints = string(b)
			}

			if lastWriteBarrier, err = m.database.commit(); err != nil {
				return err
			}

			go func(hints string, offsets map[journal.Name]int64, barrier *journal.AsyncAppend) {
				<-barrier.Ready

				storeHintsToEtcd(m.hintsPath, hints, runner.KeysAPI())
				storeOffsetsToEtcd(runner.ConsumerRoot, offsets, runner.KeysAPI())
			}(hints, copyOffsets(txOffsets), lastWriteBarrier)

		default:
			if lastWriteBarrier, err = m.database.commit(); err != nil {
				return err
			}
		}

		// Record transaction metrics.
		var txDuration = time.Now().Sub(txBegin)
		if txDuration > *maxConsumeQuantum {
			// Percent of transaction which was stalled waiting for a previous commit.
			varz.ObtainCount("gazette", "consumer", "txStalledMicros").
				Add((txDuration - *maxConsumeQuantum).Nanoseconds() / 1000)
		}
		varz.ObtainCount("gazette", "consumer", "txTotalMicros").
			Add(txDuration.Nanoseconds() / 1000)

		varz.ObtainCount("gazette", "consumer", "txMessages").Add(int64(txMessages))
		varz.ObtainCount("gazette", "consumer", "txCount").Add(1)

		// Reset for next transaction.
		clearOffsets(txOffsets)
		txMessages = 0
		txBegin = time.Time{}
		txConcurrencyCh <- struct{}{} // Release transaction lock.

		if runner.ShardPostCommitHook != nil {
			runner.ShardPostCommitHook(m)
		}
		continue // End of COMMIT_TX.
	}
}

// Shard interface implementation.
func (m *master) ID() ShardID                       { return m.shard }
func (m *master) Cache() interface{}                { return m.cache }
func (m *master) SetCache(c interface{})            { m.cache = c }
func (m *master) Database() *rocks.DB               { return m.database.DB }
func (m *master) Transaction() *rocks.WriteBatch    { return m.database.writeBatch }
func (m *master) ReadOptions() *rocks.ReadOptions   { return m.database.readOptions }
func (m *master) WriteOptions() *rocks.WriteOptions { return m.database.writeOptions }

// A buffered channel which can be sized by flag.Var.
type flaggedBufferedChan chan struct{}

// flag.Value implementation.
func (c *flaggedBufferedChan) Set(v string) error {
	i, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	c.setSize(i)
	return nil
}

// flag.Value implementation.
func (c *flaggedBufferedChan) String() string {
	return fmt.Sprintf("%v", *c)
}

// Re-allocates and fills |c| to size |i|.
func (c *flaggedBufferedChan) setSize(i int) {
	*c = make(flaggedBufferedChan, i)
	for j := 0; j < i; j++ {
		*c <- struct{}{}
	}
}

func init() {
	txConcurrencyCh.setSize(runtime.GOMAXPROCS(0)) // Default to GOMAXPROCS.

	flag.Var(&txConcurrencyCh, "maxConcurrentTx",
		"Maximum number of transactions which may execute concurrently. "+
			"Defaults to GOMAXPROCS if not set.")
}
