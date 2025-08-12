package runtime

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/network"
	"github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/derive"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/estuary/flow/go/protocols/runtime"
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
	"google.golang.org/grpc"
)

// FlowConsumerConfig configures the Flow consumer application.
type FlowConsumerConfig struct {
	runconsumer.BaseConfig
	Flow struct {
		AllowLocal    bool        `long:"allow-local" env:"ALLOW_LOCAL" description:"Allow local connectors. True for local stacks, and false otherwise."`
		BuildsRoot    string      `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
		ControlAPI    pb.Endpoint `long:"control-api" env:"CONTROL_API" description:"Address of the control-plane API"`
		Dashboard     pb.Endpoint `long:"dashboard" env:"DASHBOARD" description:"Address of the Estuary dashboard"`
		DataPlaneFQDN string      `long:"data-plane-fqdn" env:"DATA_PLANE_FQDN" description:"Fully-qualified domain name of the data-plane to which this reactor belongs"`
		Network       string      `long:"network" env:"NETWORK" description:"The Docker network that connector containers are given access to. Defaults to the bridge network"`
		ProxyRuntimes int         `long:"proxy-runtimes" default:"2" description:"The number of proxy connector runtimes that may run concurrently"`
		TestAPIs      bool        `long:"test-apis" description:"Enable APIs exclusively used while running catalog tests"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`
}

// Execute delegates to runconsumer.Cmd.Execute.
func (c *FlowConsumerConfig) Execute(args []string) error {
	// Start pprof HTTP server for profiling (only in consumer process)
	go func() {
		log.Printf("Starting pprof server on :6060 for consumer process")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	var app = &FlowConsumer{
		tap: network.NewTap(),
	}
	return runconsumer.Cmd{
		Cfg:          c,
		App:          app,
		WrapListener: app.tap.Wrap,
	}.Execute(args)
}

// FlowConsumer implements the Estuary Flow Consumer.
type FlowConsumer struct {
	// Configuration of this FlowConsumer.
	config *FlowConsumerConfig
	// Running consumer.service.
	service *consumer.Service
	// Shared catalog builds.
	builds *flow.BuildService
	// timepoint that regulates shuffled reads of started shards.
	timepoint struct {
		Now *flow.Timepoint
		Mu  sync.Mutex
	}
	// opsContext to use when appending messages to ops collections.
	// It's important that we use a Context that's scoped to the life of the process,
	// rather than the lives of individual shards, so we don't lose logs.
	opsContext context.Context
	// Network listener tap.
	tap *network.Tap
}

// application is the interface implemented by Flow shard task stores.
type application interface {
	consumer.Store
	shuffle.Store

	BeginTxn(consumer.Shard) error
	ConsumeMessage(consumer.Shard, message.Envelope, *message.Publisher) error
	FinalizeTxn(consumer.Shard, *message.Publisher) error
	FinishedTxn(consumer.Shard, consumer.OpFuture)

	StartReadingMessages(consumer.Shard, pc.Checkpoint, *flow.Timepoint, chan<- consumer.EnvelopeOrError)
	ReplayRange(_ consumer.Shard, _ pb.Journal, begin, end pb.Offset) message.Iterator
	ReadThrough(pb.Offsets) (pb.Offsets, error)

	// ProxyHook exposes a current Container and ops.Publisher
	// for use by network.ProxyServer.
	ProxyHook() (*pr.Container, ops.Publisher)
}

var _ consumer.Application = (*FlowConsumer)(nil)
var _ consumer.BeginFinisher = (*FlowConsumer)(nil)
var _ consumer.MessageProducer = (*FlowConsumer)(nil)
var _ runconsumer.Application = (*FlowConsumer)(nil)

// NewStore selects an implementing Application for the shard, and returns a new instance.
func (f *FlowConsumer) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	var err = CompleteSplit(f.service, shard, rec)
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
		if c, err := newCaptureApp(f, shard, rec); err != nil {
			return nil, err
		} else {
			return c, nil
		}
	case ops.TaskType_derivation.String():
		if d, err := newDeriveApp(f, shard, rec); err != nil {
			return nil, err
		} else {
			return d, nil
		}
	case ops.TaskType_materialization.String():
		if m, err := newMaterializeApp(f, shard, rec); err != nil {
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
	return store.(application).ConsumeMessage(shard, env, pub)
}

// FinalizeTxn delegates to the Application.
func (f *FlowConsumer) FinalizeTxn(shard consumer.Shard, store consumer.Store, pub *message.Publisher) error {
	return store.(application).FinalizeTxn(shard, pub)
}

// BeginTxn delegates to the Application.
func (f *FlowConsumer) BeginTxn(shard consumer.Shard, store consumer.Store) error {
	return store.(application).BeginTxn(shard)
}

// FinishedTxn delegates to the Application.
func (f *FlowConsumer) FinishedTxn(shard consumer.Shard, store consumer.Store, future consumer.OpFuture) {
	store.(application).FinishedTxn(shard, future)
}

// StartReadingMessages delegates to the Application.
func (f *FlowConsumer) StartReadingMessages(shard consumer.Shard, store consumer.Store, checkpoint pc.Checkpoint, envOrErr chan<- consumer.EnvelopeOrError) {
	f.timepoint.Mu.Lock()
	var tp = f.timepoint.Now
	f.timepoint.Mu.Unlock()

	store.(application).StartReadingMessages(shard, checkpoint, tp, envOrErr)
}

// ReplayRange delegates to the Application.
func (f *FlowConsumer) ReplayRange(shard consumer.Shard, store consumer.Store, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	return store.(application).ReplayRange(shard, journal, begin, end)
}

// ReadThrough delgates to the Application.
func (f *FlowConsumer) ReadThrough(shard consumer.Shard, store consumer.Store, args consumer.ResolveArgs) (pb.Offsets, error) {
	return store.(application).ReadThrough(args.ReadThrough)
}

// NewConfig returns a new config instance.
func (f *FlowConsumer) NewConfig() runconsumer.Config { return new(FlowConsumerConfig) }

func (f *FlowConsumer) tickTimepoint(wallTime time.Time) {
	// Advance the |wallTime| by a synthetic positive delta,
	// which may be non-zero in testing contexts (only).
	var delta = time.Duration(atomic.LoadInt64((*int64)(&f.service.PublishClockDelta)))
	var now = wallTime.Add(delta)

	f.timepoint.Mu.Lock()
	f.timepoint.Now.Next.Resolve(now)
	f.timepoint.Now = f.timepoint.Now.Next
	f.timepoint.Mu.Unlock()
}

// InitApplication starts shared services of the flow-consumer.
func (f *FlowConsumer) InitApplication(args runconsumer.InitArgs) error {
	bindings.RegisterPrometheusCollector()
	var config = *args.Config.(*FlowConsumerConfig)

	var builds, err = flow.NewBuildService(config.Flow.BuildsRoot)
	if err != nil {
		return fmt.Errorf("catalog builds service: %w", err)
	}

	var controlPlane *controlPlane
	var localAuthorizer = args.Service.Authorizer

	if keyedAuth, ok := localAuthorizer.(*auth.KeyedAuth); ok && !config.Flow.TestAPIs {
		controlPlane = newControlPlane(
			keyedAuth,
			config.Flow.DataPlaneFQDN,
			config.Flow.ControlAPI,
		)

		// Wrap the underlying KeyedAuth Authorizer to use the control-plane's Authorize API.
		// Next unwrap the raw JournalClient from its current AuthJournalClient,
		// and then replace it with one built using our wrapped Authorizer.
		args.Service.Authorizer = newControlPlaneAuthorizer(controlPlane)
		var rawClient = args.Service.Journals.(*pb.ComposedRoutedJournalClient).JournalClient.(*pb.AuthJournalClient).Inner
		args.Service.Journals.(*pb.ComposedRoutedJournalClient).JournalClient = pb.NewAuthJournalClient(rawClient, args.Service.Authorizer)
	}

	// Wrap Shard Hints RPC to support the Flow shard splitting workflow.
	args.Service.ShardAPI.GetHints = func(ctx context.Context, claims pb.Claims, svc *consumer.Service, req *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {
		return shardGetHints(ctx, claims, svc, req)
	}

	f.config = &config
	f.service = args.Service
	f.builds = builds
	f.timepoint.Now = flow.NewTimepoint(time.Now())
	f.opsContext = args.Context

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
		pr.NewVerifiedShufflerServer(shuffle.NewAPI(args.Service.Resolver), f.service.Verifier))

	pf.RegisterNetworkProxyServer(args.Server.GRPCServer,
		pf.NewVerifiedNetworkProxyServer(&network.ProxyServer{Resolver: args.Service.Resolver}, f.service.Verifier))

	var connectorProxy = &connectorProxy{
		address:   args.Server.Endpoint(),
		host:      f,
		runtimes:  make(map[string]*grpc.ClientConn),
		semaphore: make(chan struct{}, config.Flow.ProxyRuntimes),
	}
	runtime.RegisterConnectorProxyServer(args.Server.GRPCServer, connectorProxy)
	capture.RegisterConnectorServer(args.Server.GRPCServer, connectorProxy)
	derive.RegisterConnectorServer(args.Server.GRPCServer, connectorProxy)
	materialize.RegisterConnectorServer(args.Server.GRPCServer, connectorProxy)

	networkProxy, err := network.NewFrontend(
		f.tap,
		config.Consumer.Host,
		config.Flow.ControlAPI.URL(),
		config.Flow.Dashboard.URL(),
		pf.NewAuthNetworkProxyClient(pf.NewNetworkProxyClient(args.Server.GRPCLoopback), localAuthorizer),
		pc.NewAuthShardClient(pc.NewShardClient(args.Server.GRPCLoopback), localAuthorizer),
		args.Service.Verifier,
	)
	if err != nil {
		return fmt.Errorf("failed to build network proxy: %w", err)
	}
	args.Tasks.Queue("network-proxy-frontend", func() error {
		var err = networkProxy.Serve(args.Tasks.Context())
		if args.Tasks.Context().Err() != nil {
			err = nil // Squelch accept error if we're tearing down.
		}
		return err
	})

	return nil
}
