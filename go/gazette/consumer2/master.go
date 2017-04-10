package consumer

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	etcd3 "github.com/coreos/etcd/clientv3"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/recoverylog"
	"github.com/pippio/varz"
)

// Important tuning flags for Gazette consumers:
//
// |maxConsumeQuantum| and |minConsumeQuantum| constrain how long a consumer
// transaction *may* and *must* run for, respectively, in between commits
// to the recovery log.
//
// |maxConsumeQuantum| bounds the frequency of mandatory commits, but
// transactions may run for less time if an input Message stall occurs
// (specifically, no Message is ready to be selected without blocking).
//
// |minConsumeQuantum| forces a transaction to block for input if needed,
// until the quantum has elapsed. The default of zero implies transactions
// should commit as soon as an input stall occurs, which minimizes latency
// while still allowing for batched processing of multiple ready messages.
// This default is appropriate for most consumers, particularly those which
// retain state on a per-Message basis.
//
// Values of |minConsumeQuantum| other than zero principally make sense for
// consumers which do extensive aggregation of Messages between commits.
// For such consumers, larger Consume quantums result in fewer overall
// recovery-log writes and potentially higher throughput.
//
// Relatedly, |maxConcurrentTx| bounds the maximum concurrency of independent
// consumer transactions. For consumers doing extensive aggregation, it can
// be beneficial to maximize compute resource available to a small number of
// transactions while completely stalling others. The combination of
// |minConsumeQuantum| and |maxConcurrentTx| gives such consumers a means to
// bound the rate of writes to the recovery log without sacrificing throughput.
var (
	maxConsumeQuantum = flag.Duration("maxConsumeQuantum", 2*time.Second,
		"Max quantum of time a consumer may process messages before committing.")
	minConsumeQuantum = flag.Duration("minConsumeQuantum", time.Second,
		"Min quantum of time a consumer must process messages before committing.")

	// Flagged as |maxConcurrentTx|.
	txConcurrencyCh flaggedBufferedChan
)

const (
	storeToEtcdInterval = time.Minute

	// Channel size used between message decode & comsumption. Needs to be rather
	// large, to avoid processing stalls. Current value will tolerate a data
	// delay of up to 82ms @ 100K messages / sec without stalling.
	messageBufferSize = 1 << 13 // 8192.
	// Frequency with which the consume loop yields to the scheduler.
	messageYieldInterval = 256
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

func newMaster(shard *shard, tree *etcd.Node, client *etcd3.Client) (*master, error) {
	etcdOffsets, err := LoadOffsetsFromEtcd(tree, client)
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
		if err := os.RemoveAll(m.localDir); err != nil {
			log.WithField("err", err).Error("failed to remove local DB")
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

	// Let the consumer tear down the context, if desired.
	if runner.ShardPostStopHook != nil {
		defer runner.ShardPostStopHook(m)
	}
	if halter, ok := runner.Consumer.(ShardHalter); ok {
		defer halter.HaltShard(m)
	}

	var messages, err = m.startPumpingMessages(runner)
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
}

func (m *master) init(runner *Runner, replica *replica) error {
	// Ask replica to become "live" once caught up to the recovery-log write head.
	// This could potentially take a while, depending on how far behind we are.
	var fsm, err = replica.player.MakeLive()
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{"shard": m.shard}).Info("makeLive finished")

	var opts = rocks.NewDefaultOptions()
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
	dbOffsets, err := LoadOffsetsFromDB(m.database.DB, m.database.readOptions)
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
	// Used to bound the quantum of time a transaction may (max) or must (min)
	// take. Reset on the first message of a new transaction.
	var txTimer = time.NewTimer(0)
	// Last sent time on txTimer.
	var lastTick time.Time
	// Whether |minConsumeQuantum| & |maxConsumeQuantum| have been exceeded.
	var minQuantumElapsed, maxQuantumElapsed bool

	// Number of messages processed in the current transaction.
	var txMessages int
	// Last offset for each journal observed in the current transaction.
	var txOffsets = make(map[journal.Name]int64)

	// A write barrier which never selects.
	var zeroedAsyncAppend journal.AsyncAppend
	// Commit write-barrier of a previous transaction, which selects only after
	// the previous transaction has been sync'd by Gazette. We allow a current
	// transaction to process in the meantime (so we don't stall on Gazette I/O),
	// but it cannot commit until |lastWriteBarrier| is selectable.
	var lastWriteBarrier = &zeroedAsyncAppend
	// Specific topic.Publisher implementation passed to Consumers.
	// TODO(johnny): Eventually, we want to track partitions written to under the
	// current transaction (for later confirmation), and will also ensure that
	// messages are appropriately tagged and sequenced. We can do so with a
	// transaction-aware topic.Publisher. For now, use SimplePublisher.
	var publisher = message.SimplePublisher{Writer: runner.Gazette}

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
		// within |maxConsumeQuantum|. Ie, though we may stall an arbitrarily long
		// time waiting for |lastWriteBarrier|, only during the first
		// |maxConsumeQuantum| will we actually Consume messages.
		var maybeSrc <-chan message.Message
		if !maxQuantumElapsed {
			maybeSrc = source
		}

		// We block if the minimum quantum hasn't elapsed (or we're not in a
		// transaction in the first place). We also block if the previous
		// transaction still has not sync'd to Gazette.
		if !minQuantumElapsed || lastWriteBarrier.Ready != nil {
			select {
			case <-m.cancelCh:
				return nil
			case lastTick = <-txTimer.C:
				goto TIMER_TICK
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

			// We attempt to consume additional ready messages, and do not block,
			// but we do occasionally yield to the scheduler to ensure the decoding
			// goroutine has opportunity to keep |maybeSrc| full.
			if txMessages%messageYieldInterval == 0 {
				runtime.Gosched()
			}

			select {
			case <-m.cancelCh:
				return nil
			case lastTick = <-txTimer.C:
				goto TIMER_TICK
			case msg = <-maybeSrc:
				goto CONSUME_MSG
			default:
				goto COMMIT_TX
			}
		}

	TIMER_TICK:

		// Note that |txTimer| can fire at *any* time. Ticks may be delayed, and
		// we can't assume alignment with a current transaction. We *can* assume
		// that a timer.Reset() will result in at least one tick being generated,
		// even if the specified time.Duration is zero or negative.
		if txBegin.IsZero() {
			// No current transaction. Timer will be Reset at next transaction start.
			continue
		}

		// A delayed tick of a previous transaction, or an NTP clock adjustment
		// may mean that ticks jump backwards relative to |txBegin|. We fix time
		// to never be earlier than the current transaction start.
		if txBegin.After(lastTick) {
			lastTick = txBegin
		}

		minQuantumElapsed = !lastTick.Before(txBegin.Add(*minConsumeQuantum))
		maxQuantumElapsed = !lastTick.Before(txBegin.Add(*maxConsumeQuantum))

		if !minQuantumElapsed {
			txTimer.Reset(*minConsumeQuantum - lastTick.Sub(txBegin))
		} else if !maxQuantumElapsed {
			txTimer.Reset(*maxConsumeQuantum - lastTick.Sub(txBegin))
		}
		continue // End of TIMER_TICK.

	CONSUME_MSG:

		// Does this message begin a new transaction?
		if txMessages == 0 {
			// The transaction begins only after a transaction lock is obtained.
			<-txConcurrencyCh // May block for multiples of |maxConsumeQuantum|.
			txBegin = time.Now()
			txTimer.Reset(*minConsumeQuantum)
		}

		if err = runner.Consumer.Consume(msg, m, publisher); err != nil {
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

		if err = runner.Consumer.Flush(m, publisher); err != nil {
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

				storeHintsToEtcd(m.hintsPath, hints, runner.KeysAPI(), runner.Etcd3)
				StoreOffsetsToEtcd(runner.ConsumerRoot, offsets, runner.KeysAPI(), runner.Etcd3)
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
		minQuantumElapsed, maxQuantumElapsed = false, false
		txBegin = time.Time{}
		txConcurrencyCh <- struct{}{} // Release transaction lock.
		txMessages = 0

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
