package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/task"
)

// Ingester is a shared service for transactional ingestion into flow collections.
type Ingester struct {
	Catalog       flow.Catalog
	Journals      flow.Journals
	JournalClient pb.JournalClient
	// Delta to apply to message.Clocks used by Ingestions to sequence
	// published documents, with respect to real time.
	// This is used only in testing contexts, and is otherwise always zero.
	PublishClockDeltaForTest time.Duration

	// Un-buffered channel through which the single ingestPublisher is passed.
	pubCh chan ingestPublisher
	// Channel which is closed upon the exit of the Ingester commit loop.
	exitCh chan struct{}
}

// Ingestion manages the lifetime of a single ingest transaction.
type Ingestion struct {
	// Owning Ingester.
	ingester *Ingester
	// Started combine combines of this Ingestion.
	combines map[pf.Collection]ingestionCombine
	// Offsets mark journals actually written by this Ingestion,
	// and are used to filter and collect their append offsets
	// upon commit.
	offsets pb.Offsets
	// Commit which this Ingestion was prepared into.
	txn *ingestCommit
}

type ingestionCombine struct {
	*bindings.Combine
	spec    *pf.CollectionSpec
	commons *pf.CatalogCommons
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
	// Terminal error.
	failed error
}

// ingestCommit represents a Ingester transaction, and is shared by
// multiple *Ingestion instances to amortize their commit cost.
type ingestCommit struct {
	// Commit is a future which is resolved when ACKs of this ingest
	// transaction have been committed to stable, re-playable storage.
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
	i.exitCh = make(chan struct{})

	tasks.Queue("ingesterCommitLoop", func() error {
		// Awaken blocked concurrent Prepare calls on our exit.
		defer close(i.exitCh)
		// We only exit before tasks.Context().Done() if there's an error,
		// in which case we Cancel() for a controlled shutdown.
		defer tasks.Cancel()

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
			case pub = <-i.pubCh:
				// We're now the sole owner.
			case <-tasks.Context().Done():
				return nil
			}

			if pub.failed != nil {
				return fmt.Errorf("ingest publisher had terminal error: %w", pub.failed)
			}

			var (
				next         = pub.nextCommit
				intents, err = pub.BuildAckIntents()
				waitFor      = client.OpFutures{next.commit: struct{}{}}
			)

			if err != nil {
				return fmt.Errorf("failed to marshal ACK intents: %w", err)
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
		combines: make(map[pf.Collection]ingestionCombine),
		offsets:  make(pb.Offsets),
		txn:      nil, // Set by Prepare().
	}
}

// Add a document to the Collection within this Ingestion's transaction scope.
func (i *Ingestion) Add(collection pf.Collection, doc json.RawMessage) error {
	if combine, ok := i.combines[collection]; ok {
		return combine.CombineRight(doc)
	}

	// Must start a new stream.
	var spec, commons, err = i.ingester.Catalog.GetIngestion(collection.String())
	if err != nil {
		return fmt.Errorf("fetching specification for %q: %w", collection, err)
	}

	schemaIndex, err := commons.SchemaIndex()
	if err != nil {
		return err
	}

	combine, err := bindings.NewCombine(
		schemaIndex,
		spec.SchemaUri,
		spec.KeyPtrs,
		flow.PartitionPointers(spec),
		spec.UuidPtr,
	)
	if err != nil {
		return fmt.Errorf("building combiner for %q: %w", collection, err)
	}

	i.combines[collection] = ingestionCombine{
		Combine: combine,
		spec:    spec,
		commons: &commons.CatalogCommons,
	}
	return combine.CombineRight(doc)
}

// Prepare this Ingestion for commit.
func (i *Ingestion) Prepare() error {
	// Close send-side of streams, triggering final roll-ups and serialization
	// now *before* we acquire exclusive access to the ingestPublisher.
	// We'll also encounter any remaining user-caused errors at this time
	// (e.x., due to document that doesn't pass the collection schema).
	for c, combine := range i.combines {
		if err := combine.PrepareToDrain(); err != nil {
			return fmt.Errorf("ingestion of collection %q: %w", c, err)
		}
	}

	// Blocking acquire of the (single) ingestPublisher.
	var pub ingestPublisher
	select {
	case pub = <-i.ingester.pubCh:
		// We're now the sole owner.
	case <-i.ingester.exitCh:
		return ErrIngesterExiting
	}
	// Always return ingestPublisher on exit.
	defer func() { i.ingester.pubCh <- pub }()

	// Is this the first queued Ingestion of this ingestCommit?
	if pub.nextCommit.acks == nil {
		pub.nextCommit.acks = make(map[pb.Journal]*client.AsyncAppend)

		// Update adjusted publisher Clock.
		var delta = atomic.LoadInt64((*int64)(&i.ingester.PublishClockDeltaForTest))
		pub.clock.Update(time.Now().Add(time.Duration(delta)))
	}

	for _, combine := range i.combines {
		var mapper = flow.Mapper{
			Ctx:           context.Background(),
			JournalClient: i.ingester.JournalClient,
			JournalRules:  combine.commons.JournalRules.Rules,
			Journals:      i.ingester.Journals,
		}

		var err = combine.Drain(func(full bool, doc json.RawMessage, packedKey, packedPartitions []byte) error {
			if full {
				panic("ingestion produces only partially combined documents")
			}

			partitions, err := tuple.Unpack(packedPartitions)
			if err != nil {
				return fmt.Errorf("unpacking partitions: %w", err)
			}
			aa, err := pub.PublishUncommitted(mapper.Map, flow.Mappable{
				Spec:       combine.spec,
				Doc:        doc,
				PackedKey:  packedKey,
				Partitions: partitions,
			})
			if err != nil {
				return err
			}
			i.offsets[aa.Request().Journal] = 0 // Track journal under this Ingestion.
			return nil
		})

		if err != nil {
			pub.failed = err // Invalidate the ingestPublisher.
			return err
		}
	}

	i.txn = pub.nextCommit
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

// ErrIngesterExiting is returned by Ingestion Prepare() invocations
// when the Ingester is shutting down.
var ErrIngesterExiting = fmt.Errorf("this ingester is exiting")
