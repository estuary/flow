package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/estuary/flow/go/shuffle"
	"go.gazette.dev/core/auth"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

// FlowConsumerConfig configures the Flow consumer application.
type FlowConsumerConfig struct {
	runconsumer.BaseConfig
	Flow struct {
		AllowLocal    bool        `long:"allow-local" env:"ALLOW_LOCAL" description:"Allow local connectors. True for local stacks, and false otherwise."`
		BuildsRoot    string      `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
		ControlAPI    pb.Endpoint `long:"control-api" env:"CONTROL_API" description:"Address of the control-plane API"`
		DataPlaneFQDN string      `long:"data-plane-fqdn" env:"DATA_PLANE_FQDN" description:"Fully-qualified domain name of the data-plane to which this reactor belongs"`
		Network       string      `long:"network" description:"The Docker network that connector containers are given access to. Defaults to the bridge network"`
		TestAPIs      bool        `long:"test-apis" description:"Enable APIs exclusively used while running catalog tests"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`
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
	// Shared catalog builds.
	Builds *flow.BuildService
	// Timepoint that regulates shuffled reads of started shards.
	Timepoint struct {
		Now *flow.Timepoint
		Mu  sync.Mutex
	}
	// OpsContext to use when appending messages to ops collections.
	// It's important that we use a Context that's scoped to the life of the process,
	// rather than the lives of individual shards, so we don't lose logs.
	OpsContext context.Context
}

// Application is the interface implemented by Flow shard task stores.
type Application interface {
	consumer.Store
	shuffle.Store

	BeginTxn(consumer.Shard) error
	ConsumeMessage(consumer.Shard, message.Envelope, *message.Publisher) error
	FinalizeTxn(consumer.Shard, *message.Publisher) error
	FinishedTxn(consumer.Shard, consumer.OpFuture)

	StartReadingMessages(consumer.Shard, pc.Checkpoint, *flow.Timepoint, chan<- consumer.EnvelopeOrError)
	ReplayRange(_ consumer.Shard, _ pb.Journal, begin, end pb.Offset) message.Iterator
	ReadThrough(pb.Offsets) (pb.Offsets, error)

	// proxyHook exposes a current Container and ops.Publisher
	// for use by the network proxy server.
	proxyHook() (*pr.Container, ops.Publisher)
}

var _ consumer.Application = (*FlowConsumer)(nil)
var _ consumer.BeginFinisher = (*FlowConsumer)(nil)
var _ consumer.MessageProducer = (*FlowConsumer)(nil)
var _ runconsumer.Application = (*FlowConsumer)(nil)

// NewStore selects an implementing Application for the shard, and returns a new instance.
func (f *FlowConsumer) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	var err = CompleteSplit(f.Service, shard, rec)
	if err != nil {
		return nil, fmt.Errorf("completing shard split: %w", err)
	}

	// Note that the if/else blocks below are REQUIRED.
	// We cannot return a *Capture directly, because it may be nil but a
	// naive return would convert it into a non-nil consumer.Store holding
	// a dynamic instance of (*Capture)(nil).

	var taskType = shard.Spec().LabelSet.ValueOf(labels.TaskType)
	switch taskType {
	case ops.TaskType_capture.String():
		if c, err := NewCaptureApp(f, shard, rec); err != nil {
			return nil, err
		} else {
			return c, nil
		}
	case ops.TaskType_derivation.String():
		if d, err := NewDeriveApp(f, shard, rec); err != nil {
			return nil, err
		} else {
			return d, nil
		}
	case ops.TaskType_materialization.String():
		if m, err := NewMaterializeApp(f, shard, rec); err != nil {
			return nil, err
		} else {
			return m, nil
		}
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

func (f *FlowConsumer) tickTimepoint(wallTime time.Time) {
	// Advance the |wallTime| by a synthetic positive delta,
	// which may be non-zero in testing contexts (only).
	var delta = time.Duration(atomic.LoadInt64((*int64)(&f.Service.PublishClockDelta)))
	var now = wallTime.Add(delta)

	f.Timepoint.Mu.Lock()
	f.Timepoint.Now.Next.Resolve(now)
	f.Timepoint.Now = f.Timepoint.Now.Next
	f.Timepoint.Mu.Unlock()
}

// InitApplication starts shared services of the flow-consumer.
func (f *FlowConsumer) InitApplication(args runconsumer.InitArgs) error {
	bindings.RegisterPrometheusCollector()
	var config = *args.Config.(*FlowConsumerConfig)

	var builds, err = flow.NewBuildService(config.Flow.BuildsRoot)
	if err != nil {
		return fmt.Errorf("catalog builds service: %w", err)
	}

	if keyedAuth, ok := args.Service.Authorizer.(*auth.KeyedAuth); ok && !config.Flow.TestAPIs {
		// Wrap the underlying KeyedAuth Authorizer to use the control-plane's Authorize API.
		args.Service.Authorizer = NewControlPlaneAuthorizer(
			keyedAuth,
			config.Flow.DataPlaneFQDN,
			config.Flow.ControlAPI,
		)

		// Unwrap the raw JournalClient from its current AuthJournalClient,
		// and then replace it with one built using our wrapped Authorizer.
		var rawClient = args.Service.Journals.(*pb.ComposedRoutedJournalClient).JournalClient.(*pb.AuthJournalClient).Inner
		args.Service.Journals.(*pb.ComposedRoutedJournalClient).JournalClient = pb.NewAuthJournalClient(rawClient, args.Service.Authorizer)
	}

	// Wrap Shard Hints RPC to support the Flow shard splitting workflow.
	args.Service.ShardAPI.GetHints = func(ctx context.Context, claims pb.Claims, svc *consumer.Service, req *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {
		return shardGetHints(ctx, claims, svc, req)
	}

	f.Config = &config
	f.Service = args.Service
	f.Builds = builds
	f.Timepoint.Now = flow.NewTimepoint(time.Now())
	f.OpsContext = args.Context

	// Start a ticker of the shared *Timepoint.
	go func() {
		for t := range time.Tick(time.Second) {
			f.tickTimepoint(t)
		}
	}()

	if config.Flow.TestAPIs {
		var ajc = client.NewAppendService(args.Tasks.Context(), args.Service.Journals)
		if testing, err := NewFlowTesting(args.Tasks.Context(), f, ajc); err != nil {
			return fmt.Errorf("creating testing service: %w", err)
		} else {
			pf.RegisterTestingServer(args.Server.GRPCServer, testing)
		}
	}

	pr.RegisterShufflerServer(args.Server.GRPCServer,
		pr.NewVerifiedShufflerServer(shuffle.NewAPI(args.Service.Resolver), f.Service.Verifier))

	pf.RegisterNetworkProxyServer(args.Server.GRPCServer,
		pf.NewVerifiedNetworkProxyServer(&proxyServer{resolver: args.Service.Resolver}, f.Service.Verifier))

	return nil
}
