package flow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	pf "github.com/estuary/flow/go/protocol"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/task"
)

// Ingester is a shared service for transactional ingestion into flow collections.
type Ingester struct {
	// Collections is an index over all ingest-able (i.e., captured) collections.
	Collections map[pf.Collection]*pf.CollectionSpec
	// Combiner is a flow-worker Combine service client for Ingester use.
	Combiner pf.CombineClient
	// Mapper is a flow.Mapper for Ingester use.
	Mapper *Mapper
	// Delta to apply to message.Clocks used by Ingestion RPCs to sequence
	// published documents, with respect to real time.
	PublishClockDelta time.Duration

	pubCh chan ingestPublisher
}

// Ingestion manages the lifetime of a single ingest transaction.
type Ingestion struct {
	// Owning Ingester.
	ingester *Ingester
	// Started combine RPCs of this Ingestion.
	rpcs map[pf.Collection]*Combine
	// Offsets mark journals actually written by this Ingestion,
	// and are used to filter and collect their append offsets
	// upon commit.
	offsets pb.Offsets
	// Commit which this Ingestion was prepared into.
	txn *ingestCommit
}

// ingestPublisher coordinates the sequencing of documents written
// to journals by an Ingester.
type ingestPublisher struct {
	client.AsyncJournalClient
	// Publisher used to sequence collection journal documents.
	*message.Publisher
	// Clock used by message.Publisher, and updated at the beginning
	// of each ingest transaction.
	clock *message.Clock
	// Next ingestCommit, which will be used by one or more Ingestions.
	nextCommit *ingestCommit
}

// ingestCommit represents a Ingester transaction, and is shared by
// multiple *Ingestion instances to amortize their commit cost.
type ingestCommit struct {
	// Commit is a future which is resolved when ACKs of this ingest
	// tranaction have been committed to stable, re-playable storage.
	commit *client.AsyncOperation
	// AsyncAppends under which transaction ACKs are written.
	// This is nil until initialized by the first Ingestion of the
	// commit, and then remains empty until the commit is finalized.
	acks map[pb.Journal]*client.AsyncAppend
}

// QueueTasks queues a service loop which drives ingest transaction commits.
func (i *Ingester) QueueTasks(tasks *task.Group, jc pb.RoutedJournalClient) {
	var (
		ajc       = client.NewAppendService(tasks.Context(), jc)
		clock     = new(message.Clock)
		publisher = message.NewPublisher(ajc, clock)
	)
	// It's important that this not be buffered. The task loop below
	// uses channel sends to determine when to drive a commits.
	i.pubCh = make(chan ingestPublisher)

	tasks.Queue("ingesterCommitLoop", func() error {
		// Very first send of |ingestPublisher| into |pubCh|.
		select {
		case i.pubCh <- ingestPublisher{
			AsyncJournalClient: ajc,
			Publisher:          publisher,
			clock:              clock,
			nextCommit: &ingestCommit{
				commit: client.NewAsyncOperation(),
				acks:   nil,
			},
		}: // Pass.
		case <-tasks.Context().Done():
			return nil
		}

		for {
			// Wait for a prepared Ingestion to pass |pub| back to us.
			var pub ingestPublisher
			select {
			case pub = <-i.pubCh: // Pass.
			case <-tasks.Context().Done():
				return nil
			}

			var (
				next         = pub.nextCommit
				intents, err = pub.BuildAckIntents()
				waitFor      = client.OpFutures{next.commit: struct{}{}}
			)

			if err != nil {
				panic(err) // Marshalling cannot fail.
			} else if next.acks == nil {
				panic("expected next.appends != nil")
			}

			for _, intent := range intents {
				var aa = ajc.StartAppend(pb.AppendRequest{Journal: intent.Journal}, waitFor)
				_, _ = aa.Writer().Write(intent.Intent)

				if err := aa.Release(); err != nil {
					panic(err) // Cannot fail (we never call Require).
				}
				next.acks[intent.Journal] = aa
			}

			// TODO: |intents| should be committed to stable, re-playable storage before
			// we release the |next.commit| future. For now this is stubbed out.
			next.commit.Resolve(nil)

			pub.nextCommit = &ingestCommit{
				commit: client.NewAsyncOperation(),
				acks:   nil,
			}

			// Pass |pub| to next ready Ingestion.
			select {
			case i.pubCh <- pub: // Pass.
			case <-tasks.Context().Done():
				return nil
			}
		}
	})
}

// Start a new Ingestion.
func (i *Ingester) Start() *Ingestion {
	return &Ingestion{
		ingester: i,
		rpcs:     make(map[pf.Collection]*Combine),
		offsets:  make(pb.Offsets),
		txn:      nil, // Set by Prepare().
	}
}

// Add a document to the Collection within this Ingestion's transaction scope.
func (i *Ingestion) Add(collection pf.Collection, doc json.RawMessage) error {
	if rpc, ok := i.rpcs[collection]; ok {
		return rpc.Add(doc)
	}
	// Ingestion never prunes, since we're combining over a limited window
	// of all documents ingested into the collection.
	const prune = false

	// Must start a new RPC.
	if spec, ok := i.ingester.Collections[collection]; !ok {
		return fmt.Errorf("%q is not an ingestable collection", collection)
	} else if rpc, err := NewCombine(context.Background(), i.ingester.Combiner, spec); err != nil {
		return fmt.Errorf("while starting combiner RPC for %q: %w", collection, err)
	} else if err = rpc.Open(FieldPointersForMapper(spec), prune); err != nil {
		return fmt.Errorf("while sending RPC open %q: %w", collection, err)
	} else {
		i.rpcs[collection] = rpc
		return rpc.Add(doc)
	}
}

// Done completes the RPC, draining underlying in-progress RPCs.
func (i *Ingestion) Done() {
	for _, rpc := range i.rpcs {
		rpc.Finish(func(p pf.IndexedCombineResponse) error { return nil })
	}
}

// Prepare this Ingestion for commit.
func (i *Ingestion) Prepare() error {
	// Close send-side of RPCs now, to allow flow-worker to emit
	// roll-ups while we await the ingestPublisher.
	for c, rpc := range i.rpcs {
		if err := rpc.CloseSend(); err != nil {
			return fmt.Errorf("flushing collection %q combine RPC: %w", c, err)
		}
	}

	var combined []pf.IndexedCombineResponse

	// Gather all combine responses from RPCs. This could fail due to user error,
	// so we must read all combined documents before we begin to publish any.
	for c, rpc := range i.rpcs {
		if err := rpc.Finish(func(icr pf.IndexedCombineResponse) error {
			combined = append(combined, icr)
			return nil
		}); err != nil {
			return fmt.Errorf("ingestion of collection %q: %w", c, err)
		}
	}

	// Acquire the (singular) ingestPublisher.
	var pub = <-i.ingester.pubCh
	i.txn = pub.nextCommit

	// Is this the first queued Ingestion of this ingestCommit?
	if i.txn.acks == nil {
		i.txn.acks = make(map[pb.Journal]*client.AsyncAppend)

		// Update adjusted publisher Clock.
		var delta = atomic.LoadInt64((*int64)(&i.ingester.PublishClockDelta))
		pub.clock.Update(time.Now().Add(time.Duration(delta)))
	}

	for _, combined := range combined {
		var aa, err = pub.PublishUncommitted(i.ingester.Mapper.Map, combined)
		if err != nil {
			panic(err)
		}
		i.offsets[aa.Request().Journal] = 0 // Track journal under this Ingestion.
	}

	i.ingester.pubCh <- pub
	return nil
}

// Await the commit of this Ingestion transaction.
func (i *Ingestion) Await() (pb.Offsets, error) {
	if err := i.txn.commit.Err(); err != nil {
		return nil, err
	}
	// Gather commit offsets of the subset of journals included in |i.offsets|.
	for journal := range i.offsets {
		if aa, ok := i.txn.acks[journal]; !ok {
			panic(journal)
		} else if err := aa.Err(); err != nil {
			return nil, err
		} else {
			i.offsets[journal] = aa.Response().Commit.End
		}
	}
	return i.offsets, nil
}

// PrepareAndAwait the Ingestion.
func (i *Ingestion) PrepareAndAwait() (pb.Offsets, error) {
	if err := i.Prepare(); err != nil {
		return nil, err
	}
	return i.Await()
}
