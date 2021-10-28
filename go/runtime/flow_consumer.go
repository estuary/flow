package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/shuffle"
	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

type FlowConfig struct {
	BuildsRoot string `long:"builds-root" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
	BrokerRoot string `long:"broker-root" env:"BROKER_ROOT" default:"/gazette/cluster/flow" description:"Broker Etcd base prefix"`
}

// FlowConsumerConfig configures the flow-consumer application.
type FlowConsumerConfig struct {
	runconsumer.BaseConfig
	Flow FlowConfig `group:"flow" namespace:"flow" env-namespace:"FLOW"`

	// DisableClockTicks is exposed for in-process testing, where we manually adjust the current Timepoint.
	DisableClockTicks bool
	// Poll is exposed for a non-blocking local develop / test workflow.
	Poll bool
	// ConnectorNetwork controls the network access of launched connectors. When
	// empty, connectors will be launched on their own isolated Docker network.
	// Otherwise, they will be given access to the named network. This is useful
	// for local develop / test workflows where connector sources/sinks may be
	// running on localhost.
	ConnectorNetwork string
}

// Execute delegates to runconsumer.Cmd.Execute.
func (c *FlowConsumerConfig) Execute(args []string) error {
	return runconsumer.Cmd{Cfg: c, App: new(FlowConsumer)}.Execute(args)
}

// FlowConsumer implements the Estuary Flow Consumer.
type FlowConsumer struct {
	// Configuration of this FlowConsumer.
	Config *FlowConsumerConfig
	// Running consumer.Service.
	Service *consumer.Service
	// Watched broker journals.
	Journals flow.Journals
	// Shared catalog builds.
	Builds *flow.BuildService
	// Timepoint that regulates shuffled reads of started shards.
	Timepoint struct {
		Now *flow.Timepoint
		Mu  sync.Mutex
	}
	// Allows publishing log events as documents in a Flow collection.
	LogService *LogService
}

var _ consumer.Application = (*FlowConsumer)(nil)
var _ consumer.BeginFinisher = (*FlowConsumer)(nil)
var _ consumer.MessageProducer = (*FlowConsumer)(nil)
var _ runconsumer.Application = (*FlowConsumer)(nil)
var _ pf.SplitterServer = (*FlowConsumer)(nil)

// NewStore selects an implementing Application for the shard, and returns a new instance.
func (f *FlowConsumer) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	var err = CompleteSplit(f.Service, shard, rec)
	if err != nil {
		return nil, fmt.Errorf("completing shard split: %w", err)
	}

	var taskType = shard.Spec().LabelSet.ValueOf(labels.TaskType)
	switch taskType {
	case labels.TaskTypeCapture:
		return NewCaptureApp(f, shard, rec)
	case labels.TaskTypeDerivation:
		return NewDeriveApp(f, shard, rec)
	case labels.TaskTypeMaterialization:
		return NewMaterializeApp(f, shard, rec)
	default:
		return nil, fmt.Errorf("don't know how to serve catalog task type %q", taskType)
	}
}

// NewMessage panics if called.
func (f *FlowConsumer) NewMessage(*pb.JournalSpec) (message.Message, error) {
	panic("NewMessage is never called")
}

// ConsumeMessage delegates to the Application.
func (f *FlowConsumer) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope, pub *message.Publisher) error {
	return store.(Application).ConsumeMessage(shard, env, pub)
}

// FinalizeTxn delegates to the Application.
func (f *FlowConsumer) FinalizeTxn(shard consumer.Shard, store consumer.Store, pub *message.Publisher) error {
	return store.(Application).FinalizeTxn(shard, pub)
}

// BeginTxn delegates to the Application.
func (f *FlowConsumer) BeginTxn(shard consumer.Shard, store consumer.Store) error {
	return store.(Application).BeginTxn(shard)
}

// FinishedTxn delegates to the Application.
func (f *FlowConsumer) FinishedTxn(shard consumer.Shard, store consumer.Store, future consumer.OpFuture) {
	store.(Application).FinishedTxn(shard, future)
}

// logTxnFinished spawns a goroutine that waits for the given op to complete and logs the error if
// it fails. All task types should delegate to this function so that the error logging is
// consistent.
func logTxnFinished(logger *LogPublisher, op consumer.OpFuture) {
	go func() {
		if err := op.Err(); err != nil {
			logger.Log(log.ErrorLevel, log.Fields{"error": err}, "shard failed")
		}
	}()
}

// StartReadingMessages delegates to the Application.
func (f *FlowConsumer) StartReadingMessages(shard consumer.Shard, store consumer.Store, checkpoint pc.Checkpoint, envOrErr chan<- consumer.EnvelopeOrError) {
	f.Timepoint.Mu.Lock()
	var tp = f.Timepoint.Now
	f.Timepoint.Mu.Unlock()

	store.(Application).StartReadingMessages(shard, checkpoint, tp, envOrErr)
}

// ReplayRange delegates to the Application.
func (f *FlowConsumer) ReplayRange(shard consumer.Shard, store consumer.Store, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	return store.(Application).ReplayRange(shard, journal, begin, end)
}

// ReadThrough delgates to the Application.
func (f *FlowConsumer) ReadThrough(shard consumer.Shard, store consumer.Store, args consumer.ResolveArgs) (pb.Offsets, error) {
	return store.(Application).ReadThrough(args.ReadThrough)
}

// NewConfig returns a new config instance.
func (f *FlowConsumer) NewConfig() runconsumer.Config { return new(FlowConsumerConfig) }

// ClearRegistersForTest is an in-process testing API that clears registers of derivation shards.
func (f *FlowConsumer) ClearRegistersForTest(ctx context.Context) error {
	var listing, err = consumer.ShardList(ctx, f.Service, &pc.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet(labels.TaskType, labels.TaskTypeDerivation),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to list shards: %w", err)
	}

	for _, shard := range listing.Shards {
		var res, err = f.Service.Resolver.Resolve(consumer.ResolveArgs{
			Context:  ctx,
			ShardID:  shard.Spec.Id,
			MayProxy: false,
		})
		if err != nil {
			return fmt.Errorf("resolving shard %s: %w", shard.Spec.Id, err)
		} else if res.Status != pc.Status_OK {
			return fmt.Errorf("shard %s !OK status %s", shard.Spec.Id, res.Status)
		}
		defer res.Done()

		if err := res.Store.(*Derive).ClearRegistersForTest(); err != nil {
			return fmt.Errorf("clearing registers of shard %s: %w", shard.Spec.Id, err)
		}
	}

	return nil
}

// InitApplication starts shared services of the flow-consumer.
func (f *FlowConsumer) InitApplication(args runconsumer.InitArgs) error {
	bindings.RegisterPrometheusCollector()
	var config = *args.Config.(*FlowConsumerConfig)

	var builds, err = flow.NewBuildService(config.Flow.BuildsRoot)
	if err != nil {
		return fmt.Errorf("catalog builds service: %w", err)
	}

	// Load journal keyspace, and queue task that watches for updates.
	journals, err := flow.NewJournalsKeySpace(args.Tasks.Context(), args.Service.Etcd, config.Flow.BrokerRoot)
	if err != nil {
		return fmt.Errorf("loading journals keyspace: %w", err)
	}
	args.Tasks.Queue("journals.Watch", func() error {
		if err := f.Journals.Watch(args.Tasks.Context(), args.Service.Etcd); err != context.Canceled {
			return err
		}
		return nil
	})

	pf.RegisterShufflerServer(args.Server.GRPCServer, shuffle.NewAPI(args.Service.Resolver))
	pf.RegisterSplitterServer(args.Server.GRPCServer, f)

	args.Service.ShardAPI.GetHints = func(c context.Context, s *consumer.Service, ghr *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {
		return shardGetHints(c, s, ghr)
	}
	// Wrap Shard Stat RPC to additionally synchronize on |journals| header.
	args.Service.ShardAPI.Stat = func(ctx context.Context, svc *consumer.Service, req *pc.StatRequest) (*pc.StatResponse, error) {
		return flow.ShardStat(ctx, svc, req, journals)
	}

	f.Config = &config
	f.Service = args.Service
	f.Builds = builds
	f.Journals = journals

	// Setup a logger that shards can use to publish logs and metrics
	var ajc = client.NewAppendService(args.Context, args.Service.Journals)
	f.Timepoint.Now = flow.NewTimepoint(time.Now())

	// Start a ticker of the shared *Timepoint.
	if !f.Config.DisableClockTicks {
		go func() {
			// When running flowctl test, clock ticks will be disabled and PublishClockDelta will be
			// used to manually adjust the clock. When running _normally_, we should only adjust the
			// clock using these ticks, and the PublishClockDelta should never be applied.
			if f.Service.PublishClockDelta > 0 {
				panic("PublishClockDelta must be 0 if DisableClockTicks is false, but was > 0")
			}
			for t := range time.Tick(time.Second) {
				f.Timepoint.Mu.Lock()
				f.Timepoint.Now.Next.Resolve(t)
				f.Timepoint.Now = f.Timepoint.Now.Next
				f.Timepoint.Mu.Unlock()
			}
		}()
	}
	f.LogService = &LogService{
		ctx:      args.Context,
		ajc:      ajc,
		journals: journals,
		// Passing a nil timepoint to NewPublisher means that the timepoint that's encoded in the
		// UUID of log documents will always reflect the current wall-clock time, even when those
		// log documents were produced during test runs, where `readDelay`s might normally cause
		// time to skip forward. This probably only matters in extremely outlandish test scenarios,
		// and so it doesn't seem worth the complexity to modify this timepoint during tests.
		messagePublisher: message.NewPublisher(ajc, nil),
	}

	return nil
}

func (f *FlowConsumer) Split(ctx context.Context, req *pf.SplitRequest) (*pf.SplitResponse, error) {
	return StartSplit(ctx, f.Service, req)
}
