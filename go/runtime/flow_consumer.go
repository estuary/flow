package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/shuffle"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

type FlowConfig struct {
	CatalogRoot string `long:"catalog-root" env:"CATALOG_ROOT" default:"/flow/catalog" description:"Flow Catalog Etcd base prefix"`
	BrokerRoot  string `long:"broker-root" env:"BROKER_ROOT" default:"/gazette/cluster/flow" description:"Broker Etcd base prefix"`
}

// FlowConsumerConfig configures the flow-consumer application.
type FlowConsumerConfig struct {
	runconsumer.BaseConfig
	Flow FlowConfig `group:"flow" namespace:"flow" env-namespace:"FLOW"`

	// DisableClockTicks is exposed for in-process testing, where we manually adjust the current Timepoint.
	DisableClockTicks bool
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
	// Watched catalog entities.
	Catalog flow.Catalog
	// Timepoint that regulates shuffled reads of started shards.
	Timepoint struct {
		Now *flow.Timepoint
		Mu  sync.Mutex
	}
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
	var config = *args.Config.(*FlowConsumerConfig)

	// Load catalog & journal keyspaces, and queue tasks that watch each for updates.
	catalog, err := flow.NewCatalog(args.Tasks.Context(), args.Service.Etcd, config.Flow.CatalogRoot)
	if err != nil {
		return fmt.Errorf("loading catalog keyspace: %w", err)
	}
	journals, err := flow.NewJournalsKeySpace(args.Tasks.Context(), args.Service.Etcd, config.Flow.BrokerRoot)
	if err != nil {
		return fmt.Errorf("loading journals keyspace: %w", err)
	}

	args.Tasks.Queue("catalog.Watch", func() error {
		if err := f.Catalog.Watch(args.Tasks.Context(), args.Service.Etcd); err != context.Canceled {
			return err
		}
		return nil
	})
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
	args.Service.ShardAPI.Apply = func(c context.Context, s *consumer.Service, ar *pc.ApplyRequest) (*pc.ApplyResponse, error) {
		return shardApply(c, s, ar, f.Journals, f.Catalog)
	}
	// Wrap Shard Stat RPC to additionally synchronize on |journals| header.
	args.Service.ShardAPI.Stat = func(ctx context.Context, svc *consumer.Service, req *pc.StatRequest) (*pc.StatResponse, error) {
		return flow.ShardStat(ctx, svc, req, journals)
	}

	f.Config = &config
	f.Service = args.Service
	f.Catalog = catalog
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

// shardApply delegates to consumer.ShardApply, but creates recovery logs as needed.
func shardApply(ctx context.Context, svc *consumer.Service,
	req *pc.ApplyRequest, journals flow.Journals,
	catalog flow.Catalog) (*pc.ApplyResponse, error) {

	var journalChanges []pb.ApplyRequest_Change

	for _, change := range req.Changes {
		var shardID = change.Delete
		if change.Upsert != nil {
			shardID = change.Upsert.Id
		}
		var nextSpec = change.Upsert

		// Fetch out the current specification of this shard (if any).
		var prevSpec *pc.ShardSpec
		var prevRevision int64

		svc.State.KS.Mu.RLock()
		_ = svc.State.KS.WaitForRevision(ctx, change.ExpectModRevision)

		var ind, ok = svc.State.Items.Search(allocator.ItemKey(svc.State.KS, shardID.String()))
		if ok {
			prevSpec = svc.State.Items[ind].Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)
			prevRevision = svc.State.Items[ind].Raw.ModRevision
		} else {
			prevRevision = 0
		}
		svc.State.KS.Mu.RUnlock()

		// Fetch out the current specification of the shard's recovery log (if any).
		var shardLog pb.Journal
		if prevSpec != nil {
			shardLog = prevSpec.RecoveryLog()
		} else {
			shardLog = nextSpec.RecoveryLog()
		}
		var logSpec, logRevision = journals.GetJournal(shardLog)

		// Revisions of the request must match our view of ShardSpecs.
		if change.ExpectModRevision != -1 && change.ExpectModRevision != prevRevision {
			return nil, fmt.Errorf("request expects shard %s revision @%d, but its @%d",
				shardID, change.ExpectModRevision, prevRevision)
		}
		// Disallow changing the recovery log of an existing shard.
		if prevSpec != nil && nextSpec != nil && prevSpec.RecoveryLog() != nextSpec.RecoveryLog() {
			return nil, fmt.Errorf("cannot change recovery log of shard %s", shardID)
		}

		// Does a recovery log not exist, but should?
		if nextSpec != nil && logSpec == nil {
			// Grab labeled catalog task and its journal rules.
			var name = nextSpec.LabelSet.ValueOf(labels.TaskName)
			var _, commons, _, err = catalog.GetTask(name)
			if err != nil {
				return nil, fmt.Errorf("looking up catalog task %q: %w", name, err)
			}

			// Construct the desired recovery log spec.
			journalChanges = append(journalChanges, pb.ApplyRequest_Change{
				Upsert: flow.BuildRecoveryLogSpec(nextSpec, commons.JournalRules.Rules)})

			log.WithFields(log.Fields{
				"shard": shardID,
				"log":   shardLog,
			}).Info("recovery log will be created")
		}

		// Does a recovery log exist, and shouldn't?
		if nextSpec == nil && logSpec != nil {
			journalChanges = append(journalChanges,
				pb.ApplyRequest_Change{Delete: shardLog, ExpectModRevision: logRevision})

			log.WithFields(log.Fields{
				"shard": shardID,
				"log":   shardLog,
			}).Info("recovery log will be deleted")
		}
	}

	if len(journalChanges) != 0 {
		var _, err = client.ApplyJournals(ctx, svc.Journals,
			&pb.ApplyRequest{Changes: journalChanges})
		if err != nil {
			return nil, fmt.Errorf("applying recovery logs (before applying shard specs): %w", err)
		}

		log.WithFields(log.Fields{
			"changes": len(journalChanges),
		}).Info("applied recovery log updates")
	}

	return consumer.ShardApply(ctx, svc, req)
}

func (f *FlowConsumer) Split(ctx context.Context, req *pf.SplitRequest) (*pf.SplitResponse, error) {
	return StartSplit(ctx, f.Service, req)
}
