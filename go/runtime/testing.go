package runtime

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/protocol/ext"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// FlowTesting adapts a FlowConsumer to additionally provide testing-centric APIs.
type FlowTesting struct {
	*FlowConsumer
	// Append service used by Publisher and for writing ACKs.
	ajc *client.AppendService
	// Publisher of test ingest fixture documents.
	pub *message.Publisher
	// Clock held by the Publisher.
	pubClock *message.Clock
}

// NewFlowTesting builds a *FlowTesting which will ingest using the given AppendService.
func NewFlowTesting(inner *FlowConsumer, ajc *client.AppendService) *FlowTesting {
	var pubClock = new(message.Clock)

	return &FlowTesting{
		FlowConsumer: inner,
		ajc:          ajc,
		pub:          message.NewPublisher(ajc, pubClock),
		pubClock:     pubClock,
	}
}

// ResetState is a testing API that clears registers of derivation shards.
func (f *FlowTesting) ResetState(ctx context.Context, _ *pf.ResetStateRequest) (*pf.ResetStateResponse, error) {
	var listing, err = consumer.ShardList(ctx, f.Service, &pc.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet(labels.TaskType, labels.TaskTypeDerivation),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list shards: %w", err)
	}

	for _, shard := range listing.Shards {
		var res, err = f.Service.Resolver.Resolve(consumer.ResolveArgs{
			Context:  ctx,
			ShardID:  shard.Spec.Id,
			MayProxy: false,
		})
		if err != nil {
			return nil, fmt.Errorf("resolving shard %s: %w", shard.Spec.Id, err)
		} else if res.Status != pc.Status_OK {
			return nil, fmt.Errorf("shard %s !OK status %s", shard.Spec.Id, res.Status)
		}
		defer res.Done()

		if err := res.Store.(*Derive).ClearRegistersForTest(); err != nil {
			return nil, fmt.Errorf("clearing registers of shard %s: %w", shard.Spec.Id, err)
		}
	}

	return new(pf.ResetStateResponse), nil
}

// AdvanceTime increments the synthetic positive delta applied to the real-world clock.
func (f *FlowTesting) AdvanceTime(_ context.Context, req *pf.AdvanceTimeRequest) (*pf.AdvanceTimeResponse, error) {
	var advance = time.Duration(req.AdvanceSeconds) * time.Second
	var delta = time.Duration(
		atomic.AddInt64((*int64)(&f.Service.PublishClockDelta), int64(advance)))

	f.tickTimepoint(time.Now())

	log.WithFields(log.Fields{"advance": advance, "delta": delta}).Debug("advanced test time")
	return &pf.AdvanceTimeResponse{}, nil
}

// Ingest publishes a fixture of documents to a collection.
//
// Documents are published as CONTINUE followed by an immediate ACK, which models
// how Flow tasks (captures and derivations) write documents. This property is
// important because shuffled reads broadcast ACKs but not CONTINUE messages,
// and an ACK is therefore required to ensure all shard splits have read through
// the ingest.
//
// Unlike real tasks, however, this published ACK intent is not committed to a
// transactional store, and this API is thus *only* appropriate for testing.
func (f *FlowTesting) Ingest(ctx context.Context, req *pf.IngestRequest) (*pf.IngestResponse, error) {
	var build = f.Builds.Open(req.BuildId)
	defer build.Close()

	// Load the ingested collection.
	var err error
	var collection *pf.CollectionSpec
	if err = build.Extract(func(db *sql.DB) error {
		collection, err = catalog.LoadCollection(db, req.Collection.String())
		return err
	}); err != nil {
		return nil, fmt.Errorf("loading collection: %w", err)
	}

	var publisher = ops.NewLocalPublisher(labels.ShardLabeling{
		Build:    req.BuildId,
		TaskName: req.Collection.String(),
		TaskType: labels.TaskTypeCapture,
	})

	// Build a combiner of documents for this collection.
	combiner, err := bindings.NewCombine(publisher)
	if err != nil {
		return nil, fmt.Errorf("creating combiner: %w", err)
	}
	defer combiner.Destroy()

	if err := combiner.Configure(
		collection.Collection.String(),
		collection.Collection,
		collection.SchemaJson,
		collection.UuidPtr,
		collection.KeyPtrs,
		flow.PartitionPointers(collection),
	); err != nil {
		return nil, fmt.Errorf("configuring combiner: %w", err)
	}

	// Feed fixture documents into the combiner.
	var documents = bytes.Split([]byte(req.DocsJsonLines), []byte{'\n'})
	for d := range documents {
		var err = combiner.CombineRight(json.RawMessage(documents[d]))
		if err != nil {
			return nil, fmt.Errorf("combine-right failed: %w", err)
		}
	}

	// Update our publisher's clock to the current test time.
	var delta = time.Duration(atomic.LoadInt64((*int64)(&f.Service.PublishClockDelta)))
	f.pubClock.Update(time.Now().Add(delta))

	// Drain the combiner, mapping documents to logical partitions and writing
	// them as uncommitted messages.
	var mapper = flow.NewMapper(ctx, f.Service.Etcd, f.Journals, f.Service.State.LocalKey)
	// Ignore combine stats for testing
	if _, err = combiner.Drain(func(full bool, doc json.RawMessage, packedKey, packedPartitions []byte) error {
		if full {
			panic("ingest produces only partially combined documents")
		}

		var partitions, err = tuple.Unpack(packedPartitions)
		if err != nil {
			return fmt.Errorf("unpacking partitions: %w", err)
		}
		_, err = f.pub.PublishUncommitted(mapper.Map, flow.Mappable{
			Spec:       collection,
			Doc:        doc,
			PackedKey:  packedKey,
			Partitions: partitions,
		})
		return err
	}); err != nil {
		return nil, fmt.Errorf("combiner.Drain: %w", err)
	}

	// Build and immediately write all ACK intents.
	// (In a non-testing context, this would be committed to a transactional store first).
	var acks = make(map[pf.Journal]*client.AsyncAppend)

	intents, err := f.pub.BuildAckIntents()
	if err != nil {
		panic(err) // Cannot fail.
	}
	for _, intent := range intents {
		var aa = f.ajc.StartAppend(pb.AppendRequest{Journal: intent.Journal}, nil)
		_, _ = aa.Writer().Write(intent.Intent)

		if err := aa.Release(); err != nil {
			panic(err) // Cannot fail (we never call Require).
		}
		acks[intent.Journal] = aa
	}

	// Our |mapper| above used |f.Journals| to resolve and create dynamic
	// collection partitions. It may have created and read-through journal keys
	// which must be reflected in the logical Clock.
	f.Journals.Mu.RLock()
	var journalEtcd = ext.FromEtcdResponseHeader(f.Journals.Header)
	f.Journals.Mu.RUnlock()

	// Await async ACK appends, and collect their commit end offsets.
	var writeHeads = make(pb.Offsets, len(acks))
	for journal, aa := range acks {
		if err = aa.Err(); err != nil {
			return nil, err
		}
		writeHeads[journal] = aa.Response().Commit.End
	}

	if err = build.Close(); err != nil {
		return nil, fmt.Errorf("closing build: %w", err)
	}

	return &pf.IngestResponse{
		JournalWriteHeads: writeHeads,
		JournalEtcd:       journalEtcd,
	}, nil
}
