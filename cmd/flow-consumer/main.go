package main

import (
	"github.com/estuary/flow/go/derive"
	"github.com/estuary/flow/go/flow"
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
		Journals string `long:"journals" description:"Journals root" env:"JOURNALS"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`
}

// Flow implements the Estuary Flow consumer.Application.
type Flow struct {
	cfg      config
	service  *consumer.Service
	journals *keyspace.KeySpace
}

var _ runconsumer.Application = (*Flow)(nil)
var _ consumer.Application = (*Flow)(nil)
var _ consumer.BeginFinisher = (*Flow)(nil)
var _ consumer.MessageProducer = (*Flow)(nil)

func (f *Flow) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	// TODO - inspect label and dispatch to specific application runtime builder.
	return derive.NewApp(f.service, f.journals, shard, rec)
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
	var err error

	f.cfg = *args.Config.(*config)
	f.service = args.Service
	if f.journals, err = flow.NewJournalsKeySpace(args.Tasks.Context(), args.Service.Etcd, f.cfg.Flow.Journals); err != nil {
		return err
	}
	return nil
}

func main() { runconsumer.Main(new(Flow)) }
