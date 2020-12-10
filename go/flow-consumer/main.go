package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
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
		BrokerRoot   string        `long:"broker-root" env:"BROKER_ROOT" default:"/gazette/cluster" description:"Broker Etcd base prefix"`
		TickInterval time.Duration `long:"tick-interval" env:"TICK_INTERVAL" default:"1s" description:"Interval between clock ticks"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`
}

// Flow implements the Estuary Flow consumer.Application.
type Flow struct {
	service   *consumer.Service
	journals  *keyspace.KeySpace
	extractor *flow.WorkerHost
	timepoint struct {
		now *flow.Timepoint
		mu  sync.Mutex
	}
}

var _ runconsumer.Application = (*Flow)(nil)
var _ consumer.Application = (*Flow)(nil)
var _ consumer.BeginFinisher = (*Flow)(nil)
var _ consumer.MessageProducer = (*Flow)(nil)
var _ pf.TestingServer = (*Flow)(nil)

// NewStore selects an implementing runtime.Application for the shard, and returns a new instance.
func (f *Flow) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	isMaterialize := false
	for _, label := range shard.Spec().Labels {
		if label.Name == labels.MaterializationTarget {
			isMaterialize = true
		}
	}

	if isMaterialize {
		return runtime.NewMaterializeApp(f.service, f.journals, f.extractor, shard, rec)
	}
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
	f.timepoint.mu.Lock()
	var tp = f.timepoint.now
	f.timepoint.mu.Unlock()

	store.(runtime.Application).StartReadingMessages(shard, checkpoint, tp, envOrErr)
}

// ReplayRange delegates to the Application.
func (f *Flow) ReplayRange(shard consumer.Shard, store consumer.Store, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	return store.(runtime.Application).ReplayRange(shard, journal, begin, end)
}

// ReadThrough delgates to the Application.
func (f *Flow) ReadThrough(shard consumer.Shard, store consumer.Store, args consumer.ResolveArgs) (pb.Offsets, error) {
	return store.(runtime.Application).ReadThrough(args.ReadThrough)
}

// NewConfig returns a new config instance.
func (f *Flow) NewConfig() runconsumer.Config { return new(config) }

// AdvanceTime is a testing-only API that advances the current test time.
func (f *Flow) AdvanceTime(_ context.Context, req *pf.AdvanceTimeRequest) (*pf.AdvanceTimeResponse, error) {
	var add = uint64(time.Second) * req.AddClockDeltaSeconds
	var out = time.Duration(atomic.AddInt64((*int64)(&f.service.PublishClockDelta), int64(add)))

	f.timepoint.mu.Lock()
	f.timepoint.now.Next.Resolve(time.Now())
	f.timepoint.now = f.timepoint.now.Next
	f.timepoint.mu.Unlock()

	return &pf.AdvanceTimeResponse{ClockDeltaSeconds: uint64(out / time.Second)}, nil
}

// ClearRegisters is a testing-only API that clears the registers of a resolved Shard.
func (f *Flow) ClearRegisters(ctx context.Context, req *pf.ClearRegistersRequest) (*pf.ClearRegistersResponse, error) {
	var res, err = f.service.Resolver.Resolve(consumer.ResolveArgs{
		Context:     ctx,
		ShardID:     req.ShardId,
		ProxyHeader: req.Header,
		MayProxy:    false,
	})
	if err != nil {
		return new(pf.ClearRegistersResponse), err
	} else if res.Status != pc.Status_OK {
		return &pf.ClearRegistersResponse{
			Status: res.Status,
			Header: res.Header,
		}, nil
	}
	defer res.Done()

	resp, err := res.Store.(runtime.Application).ClearRegisters(ctx, req)
	if err == nil {
		resp.Status = pc.Status_OK
		resp.Header = res.Header
	}
	return resp, err
}

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
	pf.RegisterTestingServer(args.Server.GRPCServer, f)

	// Wrap Shard Stat RPC to additionally synchronize on |journals| header.
	args.Service.ShardAPI.Stat = func(ctx context.Context, svc *consumer.Service, req *pc.StatRequest) (*pc.StatResponse, error) {
		return flow.ShardStat(ctx, svc, req, journals)
	}

	f.service = args.Service
	f.journals = journals
	f.extractor = extractor
	f.timepoint.now = flow.NewTimepoint(time.Now())

	// Start a ticker of the shared *Timepoint.
	go func(d time.Duration) {
		for t := range time.Tick(d) {
			f.timepoint.mu.Lock()
			f.timepoint.now.Next.Resolve(t)
			f.timepoint.now = f.timepoint.now.Next
			f.timepoint.mu.Unlock()
		}
	}(config.Flow.TickInterval)

	return nil
}

func main() {
	var flow = new(Flow)
	runconsumer.Main(flow)

	if flow.extractor != nil {
		_ = flow.extractor.Stop()
	}
}
