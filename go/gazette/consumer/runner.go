package consumer

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/varz"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/api-server/discovery"
	"github.com/pippio/gazette/journal"
)

var (
	localStateBase = flag.String("localStateBase", "/var/tmp/gazette-local-state",
		"Local base directory for all Gazette local state.")

	// TODO(johnny): Remove me when v2 consumers are ready.
	flushRate = flag.Int("consumerFlushRate", 1500,
		"Number of items between Gazette consumer flushes (deprecated)")
)

// Implements the details of the distributed consumer transaction flow.
// Currently this is an adapated interface to the V1 consumer API.
type Runner struct {
	consumer   Consumer
	contexts   map[journal.Name]*Context
	contextsMu sync.Mutex
	name       string
	sourceV1   *Source
	writer     journal.Writer
}

func NewRunner(name string, consumer Consumer, etcd discovery.EtcdService,
	getter journal.Getter, writer journal.Writer) (*Runner, error) {

	sourceV1, err := NewSource(name, consumer.Topics()[0], etcd, getter)
	if err != nil {
		return nil, err
	}
	return &Runner{
		consumer: consumer,
		contexts: make(map[journal.Name]*Context),
		name:     name,
		sourceV1: sourceV1,
		writer:   writer,
	}, nil
}

func (s *Runner) Run() error {
	concurrency := 1
	if cc, ok := s.consumer.(ConcurrentExecutor); ok {
		concurrency = cc.Concurrency()
	}

	// Used to signal termination of individual consumerLoop()s.
	done := make(chan struct{}, concurrency)

	// Install a signal handler to Stop() on external signal.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig, ok := <-interrupt
		if ok {
			log.WithField("signal", sig).Info("caught signal")
			s.sourceV1.Stop()
			// Once the consumer is cleaned up, just write to done to allow the
			// below for loop to complete and return from main, this kills all
			// running goroutines.
			for i := 0; i < concurrency; i++ {
				done <- struct{}{}
			}
		}
	}()

	for i := 0; i < concurrency; i++ {
		go s.consumerLoop(done)
	}
	for i := 0; i < concurrency; i++ {
		<-done
	}
	return nil
}

func (s *Runner) consumerLoop(done chan<- struct{}) {
	for i := 0; true; i++ {
		msg := s.sourceV1.Next()
		context := s.obtainContext(msg.Mark.Journal)

		topic := s.consumer.Topics()[0]
		s.consumer.Consume(msg, topic, context)
		s.sourceV1.Acknowledge(msg)

		varz.ObtainCount("consumer", s.name, "consumed", topic.Name).Add(1)

		if i%*flushRate == 0 {
			// Apply arbitrary, periodic flushes. Long term, this would be done
			// by a single process after draining all messages. For now, as flushes
			// are integrated into the consumer loop, we must lock.
			context.tmpMu.Lock()
			s.finishTransaction(context)
			context.tmpMu.Unlock()
		}
	}
	done <- struct{}{}
}

func (s *Runner) obtainContext(name journal.Name) *Context {
	s.contextsMu.Lock()
	defer s.contextsMu.Unlock()

	if context, ok := s.contexts[name]; ok {
		return context
	}

	var dbOpts *rocks.Options

	if optioner, ok := s.consumer.(RocksDBOptioner); ok {
		dbOpts = optioner.RocksDBOptions()
	} else {
		dbOpts = rocks.NewDefaultOptions()
	}

	defer dbOpts.Destroy()
	dbOpts.SetCreateIfMissing(true)

	dbPath := filepath.Join(*localStateBase, name.String())
	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		log.WithFields(log.Fields{"path": dbPath, "err": err}).Fatal("failed to mkdir")
	}
	db, err := rocks.OpenDb(dbOpts, dbPath)
	if err != nil {
		log.WithFields(log.Fields{"path": dbPath, "err": err}).Fatal("failed to open")
	}

	context := &Context{
		Shard:       ShardID(len(s.contexts)),
		Database:    db,
		Transaction: rocks.NewWriteBatch(),
		Writer:      s.writer,
	}
	s.contexts[name] = context

	// Let the consumer initialize the context, if desired.
	if initer, ok := s.consumer.(ContextIniter); ok {
		initer.InitContext(context)
	}

	return context
}

func (s *Runner) finishTransaction(context *Context) {
	s.consumer.Flush(context)

	var dbWriteOpts *rocks.WriteOptions
	if optioner, ok := s.consumer.(RocksWriteOptioner); ok {
		dbWriteOpts = optioner.RocksWriteOptions()
	} else {
		dbWriteOpts = rocks.NewDefaultWriteOptions()
	}
	defer dbWriteOpts.Destroy()

	if err := context.Database.Write(dbWriteOpts, context.Transaction); err != nil {
		log.WithField("err", err).Fatal("failed to flush database")
	}

	// Begin next transaction.
	context.Transaction.Destroy()
	context.Transaction = rocks.NewWriteBatch()

	varz.ObtainCount("consumer", s.name, "flushed").Add(1)
}
