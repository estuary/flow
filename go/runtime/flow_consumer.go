package runtime

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/shuffle"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

// FlowConsumerConfig configures the flow-consumer application.
type FlowConsumerConfig struct {
	runconsumer.BaseConfig

	// Flow application flags.
	Flow struct {
		BrokerRoot string `long:"broker-root" env:"BROKER_ROOT" default:"/gazette/cluster" description:"Broker Etcd base prefix"`
		LambdaJS   string `long:"lambda-uds-js" env:"LAMBDA_UDS_JS" default:"" description:"Path to JavaScript lambda Unix Domain Socket, or empty to start workers as needed"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`

	// DisableClockTicks is exposed for in-process testing, where we manually adjust the current Timepoint.
	DisableClockTicks bool
}

// FlowConsumer implements the Estuary Flow Consumer.
type FlowConsumer struct {
	// Configuration of this FlowConsumer.
	Config *FlowConsumerConfig
	// Running consumer.Service.
	Service *consumer.Service
	// Watched broker journals.
	Journals *keyspace.KeySpace
	// Timepoint that regulates shuffled reads of started shards.
	Timepoint struct {
		Now *flow.Timepoint
		Mu  sync.Mutex
	}
}

var _ consumer.Application = (*FlowConsumer)(nil)
var _ consumer.BeginFinisher = (*FlowConsumer)(nil)
var _ consumer.BeginRecoverer = (*FlowConsumer)(nil)
var _ consumer.MessageProducer = (*FlowConsumer)(nil)
var _ runconsumer.Application = (*FlowConsumer)(nil)

// BeginRecovery implements the BeginRecoverer interface, and creates a recovery log
// for the Shard if one doesn't already exists.
func (f *FlowConsumer) BeginRecovery(shard consumer.Shard) (pc.ShardID, error) {
	var shardSpec = shard.Spec()

	// Does the shard's recovery log already exist?
	var itemKey = path.Join(f.Journals.Root, shardSpec.RecoveryLog().String())
	f.Journals.Mu.RLock()
	var _, exists = f.Journals.Search(itemKey)
	f.Journals.Mu.RUnlock()

	if exists {
		return shardSpec.Id, nil // Nothing to do.
	}
	// We must attempt to create the recovery log.

	// Grab labeled catalog, and load journal rules.
	var catalog, err = flow.NewCatalog(shardSpec.LabelSet.ValueOf(labels.CatalogURL), "")
	if err != nil {
		return "", fmt.Errorf("opening catalog: %w", err)
	}
	defer catalog.Close()

	journalRules, err := catalog.LoadJournalRules()
	if err != nil {
		return "", fmt.Errorf("loading journal rules: %w", err)
	}

	// Construct the desired recovery log spec.
	var desired = flow.BuildRecoveryLogSpec(shardSpec, journalRules.Rules)
	_, err = client.ApplyJournals(shard.Context(), shard.JournalClient(), &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{
				Upsert:            &desired,
				ExpectModRevision: 0,
			},
		},
	})

	if err != nil {
		return "", fmt.Errorf("failed to create recovery log %q: %w", desired.Name, err)
	}

	log.WithFields(log.Fields{
		"name":  desired.Name,
		"shard": shardSpec.Id,
	}).Info("created recovery log")

	return shardSpec.Id, nil
}

// NewStore selects an implementing Application for the shard, and returns a new instance.
func (f *FlowConsumer) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	if shard.Spec().LabelSet.ValuesOf(labels.Materialization) != nil {
		return NewMaterializeApp(f.Service, f.Journals, shard, rec)
	} else if shard.Spec().LabelSet.ValuesOf(labels.Derivation) != nil {
		return NewDeriveApp(f.Service, f.Journals, shard, rec, f.Config.Flow.LambdaJS)
	}
	return nil, fmt.Errorf("unknown shard type")
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

// AdvanceTimeForTest is a in-process testing API that advances the current test time.
func (f *FlowConsumer) AdvanceTimeForTest(delta time.Duration) time.Duration {
	if !f.Config.DisableClockTicks {
		panic("expected DisableClockTicks to be set")
	}
	var add = uint64(delta)
	var out = time.Duration(atomic.AddInt64((*int64)(&f.Service.PublishClockDelta), int64(add)))

	// Tick timepoint to unblock any gated shuffled reads.
	f.Timepoint.Mu.Lock()
	f.Timepoint.Now.Next.Resolve(time.Now())
	f.Timepoint.Now = f.Timepoint.Now.Next
	f.Timepoint.Mu.Unlock()

	return time.Duration(out)
}

// ClearRegistersForTest is an in-process testing API that clears registers of derivation shards.
func (f *FlowConsumer) ClearRegistersForTest(ctx context.Context) error {
	var listing, err = consumer.ShardList(ctx, f.Service, &pc.ListRequest{
		Selector: pb.LabelSelector{
			// List derivation shards.
			Include: pb.MustLabelSet("estuary.dev/derivation", ""),
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
	var config = *args.Config.(*FlowConsumerConfig)

	// Load journals keyspace, and queue a task which will watch for updates.
	var journals, err = flow.NewJournalsKeySpace(args.Tasks.Context(), args.Service.Etcd, config.Flow.BrokerRoot)
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

	// Wrap Shard Stat RPC to additionally synchronize on |journals| header.
	args.Service.ShardAPI.Stat = func(ctx context.Context, svc *consumer.Service, req *pc.StatRequest) (*pc.StatResponse, error) {
		return flow.ShardStat(ctx, svc, req, journals)
	}

	f.Config = &config
	f.Service = args.Service
	f.Journals = journals
	f.Timepoint.Now = flow.NewTimepoint(time.Now())

	// Start a ticker of the shared *Timepoint.
	if !f.Config.DisableClockTicks {
		go func() {
			for t := range time.Tick(time.Second) {
				f.Timepoint.Mu.Lock()
				f.Timepoint.Now.Next.Resolve(t)
				f.Timepoint.Now = f.Timepoint.Now.Next
				f.Timepoint.Mu.Unlock()
			}
		}()
	}

	return nil
}
