package main

import (
	"context"
	"fmt"

	"github.com/estuary/flow/go/derive"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocol"
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

func (f *Flow) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	// TODO - inspect label and dispatch to specific application runtime builder.
	return derive.NewApp(f.service, f.journals, f.extractor, shard, rec)
}

func (f *Flow) NewMessage(*pb.JournalSpec) (message.Message, error) {
	panic("NewMessage is never called")
}

func (f *Flow) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope, pub *message.Publisher) error {
	return store.(flow.FlowConsumer).ConsumeMessage(shard, env, pub)
}

func (f *Flow) FinalizeTxn(shard consumer.Shard, store consumer.Store, pub *message.Publisher) error {
	return store.(flow.FlowConsumer).FinalizeTxn(shard, pub)
}

func (f *Flow) BeginTxn(shard consumer.Shard, store consumer.Store) error {
	return store.(flow.FlowConsumer).BeginTxn(shard)
}

func (f *Flow) FinishedTxn(shard consumer.Shard, store consumer.Store, future consumer.OpFuture) {
	store.(flow.FlowConsumer).FinishedTxn(shard, future)
}

func (f *Flow) StartReadingMessages(shard consumer.Shard, store consumer.Store, checkpoint pc.Checkpoint, envOrErr chan<- consumer.EnvelopeOrError) {
	store.(flow.FlowConsumer).StartReadingMessages(shard, checkpoint, envOrErr)
}

func (f *Flow) ReplayRange(shard consumer.Shard, store consumer.Store, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	return store.(flow.FlowConsumer).ReplayRange(shard, journal, begin, end)
}

func (f *Flow) NewConfig() runconsumer.Config { return new(config) }

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
