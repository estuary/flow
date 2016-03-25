package consumer

import (
	"time"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	"github.com/pippio/api-server/varz"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/recoverylog"
)

var (
	storeHintsInterval = time.Minute
	maxTransactionTime = 100 * time.Millisecond
	messageBufferSize  = 1024
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

	if m.database, err = newDatabase(fsm, m.localDir, runner.Writer); err != nil {
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
	var pump = newPump(runner.Getter, messages, m.cancelCh)
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
	var storeHintsInterval = time.NewTicker(storeHintsInterval)

	// Timepoint at which the current transaction began.
	// Set on the first message of a new transaction.
	var txBegin time.Time
	// Upper-bounds the amount of time a transaction may take.
	// Reset on the first message of a new transaction.
	var txTimeout = time.NewTimer(0)
	<-txTimeout.C
	// Number of messages processed in the current transaction.
	var txCount int
	// Last offset for each journal observed in the current transaction.
	var txOffsets = make(map[journal.Name]int64)

	// Approximate current time. Updated via |txTimeout|.
	var now time.Time
	// Commit write-barrier of a previous transaction, which selects only after
	// the previous transaction has been sync'd by Gaette. We allow a current
	// transaction to process in the meantime (so we don't stall on Gazette IO),
	// but it cannot commit until |lastWriteBarrier| is selectable.
	var lastWriteBarrier <-chan struct{}

	for {
		var err error
		var msg message.Message

		// We allow messages to process in the current transaction only if we're
		// within the transaction timeout. Though we may stall an arbitrarily long
		// time waiting for |lastWriteBarrier|, we only wish to process messages
		// during the first |maxTransactionTime| of the transaction.
		var maybeSrc <-chan message.Message
		if txCount == 0 || now.Before(txBegin.Add(maxTransactionTime)) {
			maybeSrc = source
		}

		if txCount == 0 || lastWriteBarrier != nil {
			// We must block until both conditions are resolved.
			select {
			case <-m.cancelCh:
				return nil
			case now = <-txTimeout.C:
				continue
			case <-lastWriteBarrier:
				lastWriteBarrier = nil
				continue
			case msg = <-maybeSrc:
				goto CONSUME_MSG
			case <-storeHintsInterval.C:
				goto STORE_HINTS
			}
		} else {
			// We have a transaction with at least one message, and the previous
			// write barrier has completed. We're able to commit at any time.
			// We attempt to consume additional ready messages, but do not block.
			select {
			case <-m.cancelCh:
				return nil
			case now = <-txTimeout.C:
				continue
			case msg = <-maybeSrc:
				goto CONSUME_MSG
			case <-storeHintsInterval.C:
				goto STORE_HINTS
			default:
				goto COMMIT_TX
			}
		}

	CONSUME_MSG:

		// Does this message begin a new transaction?
		if txCount == 0 {
			txBegin = time.Now()
			txTimeout.Reset(maxTransactionTime)
		}

		if err = runner.Consumer.Consume(msg, m, publisher{runner.Writer}); err != nil {
			return err
		}

		txCount += 1
		txOffsets[msg.Mark.Journal] = msg.Mark.Offset
		msg.Topic.PutMessage(msg.Value)

		if runner.ShardPostConsumeHook != nil {
			runner.ShardPostConsumeHook(msg, m)
		}
		continue // End of CONSUME_MSG.

	COMMIT_TX:

		if err = runner.Consumer.Flush(m, publisher{runner.Writer}); err != nil {
			return err
		}
		storeOffsets(m.database.writeBatch, txOffsets)
		clearOffsets(txOffsets)

		if lastWriteBarrier, err = m.database.commit(); err != nil {
			return err
		}

		// Record transaction metrics.
		if now.After(txBegin.Add(maxTransactionTime)) {
			varz.ObtainCount("gazette", "consumer", "stalls").Add(1)
		}
		varz.ObtainCount("gazette", "consumer", "messages").Add(int64(txCount))
		varz.ObtainCount("gazette", "consumer", "transactions").Add(1)

		// Reset for next transaction.
		txCount = 0
		txBegin = time.Time{}

		if runner.ShardPostCommitHook != nil {
			runner.ShardPostCommitHook(m)
		}
		continue // End of COMMIT_TX.

	STORE_HINTS:

		if err := storeHints(runner.KeysAPI(), m.database.recorder.BuildHints(),
			m.hintsPath); err != nil {
			return err
		}
		continue // End of STORE_HINTS.
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
