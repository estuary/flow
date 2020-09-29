package main

import (
	"context"
	"fmt"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/estuary/flow/go/runtime"
	"github.com/estuary/flow/go/shuffle"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

// config configures the Flow application.
type config struct {
	runconsumer.BaseConfig

	// Flow application flags.
	Flow struct {
		BrokerRoot string `long:"broker-root" env:"BROKER_ROOT" default:"/gazette/cluster" description:"Broker Etcd base prefix"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`
}

// Flow implements the Estuary Flow consumer.Application.
type Flow struct {
	service   *consumer.Service
	journals  *keyspace.KeySpace
	extractor *flow.WorkerHost
}

var _ runconsumer.Application = (*Flow)(nil)
var _ consumer.Application = (*Flow)(nil)
var _ consumer.BeginFinisher = (*Flow)(nil)
var _ consumer.MessageProducer = (*Flow)(nil)

// NewStore selects an implementing runtime.Application for the shard, and returns a new instance.
func (f *Flow) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	// TODO - inspect label and dispatch to NewDeriveApp vs NewMaterializeApp.
	return runtime.NewDeriveApp(f.service, f.journals, f.extractor, shard, rec)
}

// NewMessage panics if called.
func (f *Flow) NewMessage(*pb.JournalSpec) (message.Message, error) {
	panic("NewMessage is never called")
}

// ConsumeMessage delegates to the Application.
func (f *Flow) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope, pub *message.Publisher) error {
	return store.(runtime.Application).ConsumeMessage(shard, env, pub)
}

// FinalizeTxn delegates to the Application.
func (f *Flow) FinalizeTxn(shard consumer.Shard, store consumer.Store, pub *message.Publisher) error {
	return store.(runtime.Application).FinalizeTxn(shard, pub)
}

// BeginTxn delegates to the Application.
func (f *Flow) BeginTxn(shard consumer.Shard, store consumer.Store) error {
	return store.(runtime.Application).BeginTxn(shard)
}

// FinishedTxn delegates to the Application.
func (f *Flow) FinishedTxn(shard consumer.Shard, store consumer.Store, future consumer.OpFuture) {
	store.(runtime.Application).FinishedTxn(shard, future)
}

// StartReadingMessages delegates to the Application.
func (f *Flow) StartReadingMessages(shard consumer.Shard, store consumer.Store, checkpoint pc.Checkpoint, envOrErr chan<- consumer.EnvelopeOrError) {
	store.(runtime.Application).StartReadingMessages(shard, checkpoint, envOrErr)
}

// ReplayRange delegates to the Application.
func (f *Flow) ReplayRange(shard consumer.Shard, store consumer.Store, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	return store.(runtime.Application).ReplayRange(shard, journal, begin, end)
}

// ReadThrough ensures the revision of |journals| reflects MinEtcdRevision,
// and then delgates to the Application.
func (f *Flow) ReadThrough(shard consumer.Shard, store consumer.Store, args consumer.ResolveArgs) (pb.Offsets, error) {
	f.journals.Mu.RLock()
	var err = f.journals.WaitForRevision(shard.Context(), args.MinEtcdRevision)
	f.journals.Mu.RUnlock()

	if err != nil {
		return nil, err
	}
	return store.(runtime.Application).ReadThrough(args.ReadThrough)
}

// NewConfig returns a new config instance.
func (f *Flow) NewConfig() runconsumer.Config { return new(config) }

// InitApplication starts shared services of the flow-consumer.
func (f *Flow) InitApplication(args runconsumer.InitArgs) error {
	var config = *args.Config.(*config)

	// Start shared extraction worker.
	var extractor, err = flow.NewWorkerHost("extract")
	if err != nil {
		return fmt.Errorf("starting extraction worker: %w", err)
	}
	// Load journals keyspace, and queue a task which will watch for updates.
	journals, err := flow.NewJournalsKeySpace(args.Tasks.Context(), args.Service.Etcd, config.Flow.BrokerRoot)
	if err != nil {
		return fmt.Errorf("loading journals keyspace: %w", err)
	}
	args.Tasks.Queue("journals.Watch", func() error {
		if err := f.journals.Watch(args.Tasks.Context(), args.Service.Etcd); err != context.Canceled {
			return err
		}
		return nil
	})

	pf.RegisterShufflerServer(args.Server.GRPCServer, shuffle.NewAPI(args.Service.Resolver))

	f.service = args.Service
	f.journals = journals
	f.extractor = extractor

	return nil
}

func main() {
	var flow = new(Flow)
	runconsumer.Main(flow)

	if flow.extractor != nil {
		_ = flow.extractor.Stop()
	}
}
