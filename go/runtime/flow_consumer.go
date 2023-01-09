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
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/shuffle"
	"github.com/pkg/errors"
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
		BuildsRoot string `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
		BrokerRoot string `long:"broker-root" required:"true" env:"BROKER_ROOT" default:"/gazette/cluster" description:"Broker Etcd base prefix"`
		Network    string `long:"network" description:"The Docker network that connector containers are given access to, defaults to the bridge network"`
		TestAPIs   bool   `long:"test-apis" description:"Enable APIs exclusively used while running catalog tests"`
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
	// Watched broker journals.
	Journals flow.Journals
	// Shared catalog builds.
	Builds *flow.BuildService
	// Proxies network traffic to containers
	NetworkProxyServer *ProxyServer
	// Timepoint that regulates shuffled reads of started shards.
	Timepoint struct {
		Now *flow.Timepoint
		Mu  sync.Mutex
	}
	// LogAppendService is used to append log messages to the ops logs collections. It's important
	// that we use an AppendService with a context that's scoped to the life of the process, rather
	// than the lives of individual shards.
	LogAppendService *client.AppendService
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
func logTxnFinished(publisher ops.Publisher, op consumer.OpFuture) {
	go func() {
		if err := op.Err(); err != nil && errors.Cause(err) != context.Canceled {
			ops.PublishLog(publisher, pf.LogLevel_error,
				"shard failed",
				"error", err)
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

	// Wrap Shard Hints RPC to support the Flow shard splitting workflow.
	args.Service.ShardAPI.GetHints = func(c context.Context, s *consumer.Service, ghr *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {
		return shardGetHints(c, s, ghr)
	}
	// Wrap Shard Stat RPC to additionally synchronize on |journals| header.
	args.Service.ShardAPI.Stat = func(ctx context.Context, svc *consumer.Service, req *pc.StatRequest) (*pc.StatResponse, error) {
		return flow.ShardStat(ctx, svc, req, journals)
	}

	f.LogAppendService = client.NewAppendService(args.Context, args.Service.Journals)
	f.Config = &config
	f.Service = args.Service
	f.Builds = builds
	f.Journals = journals
	f.Timepoint.Now = flow.NewTimepoint(time.Now())

	// Start a ticker of the shared *Timepoint.
	go func() {
		for t := range time.Tick(time.Second) {
			f.tickTimepoint(t)
		}
	}()

	if config.Flow.TestAPIs {
		var ajc = client.NewAppendService(args.Context, args.Service.Journals)
		pf.RegisterTestingServer(args.Server.GRPCServer, NewFlowTesting(f, ajc))
	}

	pf.RegisterShufflerServer(args.Server.GRPCServer, shuffle.NewAPI(args.Service.Resolver))
	capture.RegisterRuntimeServer(args.Server.GRPCServer, f)

	f.NetworkProxyServer = NewProxyServer(args.Service.Resolver)
	pf.RegisterNetworkProxyServer(args.Server.GRPCServer, f.NetworkProxyServer)

	return nil
}
