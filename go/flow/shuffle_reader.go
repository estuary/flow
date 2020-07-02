package flow

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	pf "github.com/estuary/flow/go/protocol"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

type shuffleReader struct {
	ctx        context.Context
	cancel     context.CancelFunc
	polledList *client.PolledList

	envCh       chan<- consumer.EnvelopeOrError
	idleOffsets map[pb.Journal]pb.Offset
	activeReads map[pb.Journal]context.CancelFunc

	resolver *consumer.Resolver

	ring      pf.Ring
	ringIndex int
	shuffles  []pf.ShuffleConfig_Shuffle

	journalClient pb.RoutedJournalClient
	shuffleClient pf.ShufflerClient

	wg sync.WaitGroup
	mu sync.Mutex
}

func newShuffleReader(offsets map[pb.Journal]pb.Offset) {

}

func (sr *shuffleReader) convergeLoop() {
	defer sr.wg.Done()

	for range sr.polledList.UpdateCh() {
		sr.mu.Lock()
		sr.converge()
		sr.mu.Unlock()
	}
}

func (sr *shuffleReader) converge() {
	var (
		// Construct a new map of CancelFunc, to enable detection of
		// journals are actively read but but no longer in the listing.
		prevFns = sr.activeReads
		nextFns = make(map[pb.Journal]context.CancelFunc)
	)
	for _, journal := range sr.polledList.List().Journals {
		if fn, ok := prevFns[journal.Spec.Name]; ok {
			// A read has already been started for this journal.
			nextFns[journal.Spec.Name] = fn
			delete(prevFns, journal.Spec.Name)
			continue
		}

		var offset = sr.idleOffsets[journal.Spec.Name]
		delete(sr.idleOffsets, journal.Spec.Name)

		// TODO(johnny): Lower-bound offset using configurable fragment time horizon.

		var subCtx, subCancelFn = context.WithCancel(sr.ctx)

		sr.wg.Add(1)
		go sr.journalReadLoop(subCtx, &journal.Spec, offset)

		nextFns[journal.Spec.Name] = subCancelFn
	}
	// Cancel any prior readers which are no longer in the listing.
	// Keep entries: read loops will clear them on exit.
	for j, fn := range prevFns {
		fn()
		nextFns[j] = fn
	}
	sr.activeReads = nextFns
}

func (sr *shuffleReader) journalReadLoop(ctx context.Context, spec *pb.JournalSpec, offset pb.Offset) {
	defer func() {
		sr.mu.Lock()
		sr.idleOffsets[spec.Name] = offset
		delete(sr.activeReads, spec.Name)
		sr.mu.Unlock()

		sr.wg.Done()
	}()

	// Hash and map journal to a coordinating participant of the current ring.
	// We use SHA-2 for its strong collision avoidance properties, and not for security.
	// Journals often differ from one another by a single bit (eg, 'foo-002' vs 'foo-003'),
	// which can cause grouping with FNV-a and other standard non-secure hashes.
	var hash = sha256.Sum256([]byte(spec.Name))

	var cfg = pf.ShuffleConfig{
		Journal:     spec.Name,
		Ring:        sr.ring,
		Coordinator: binary.LittleEndian.Uint32(hash[:]) % uint32(len(sr.ring.Members)),
		Shuffles:    sr.shuffles,
	}

	var log = log.WithFields(log.Fields{
		"ring":        cfg.Ring.Name,
		"journal":     cfg.Journal,
		"coordinator": cfg.Coordinator,
		"offset":      offset,
		"ringIndex":   sr.ringIndex,
	})
	log.Info("starting shuffled journal read")

	var stream pf.Shuffler_ShuffleClient
	var resp pf.ShuffleResponse

	for attempt := 0; ctx.Err() == nil; attempt++ {
		// Wait for backoff timer or context cancellation.
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff(attempt)):
		}

		// Apply MinMsgClock to identify a lower-bound fragment & offset to begin reading from.
		if bound := cfg.Ring.Members[sr.ringIndex].MinMsgClock; offset == 0 && bound != 0 {
			var list, err = client.ListAllFragments(ctx, sr.journalClient, pb.FragmentsRequest{
				Journal: cfg.Journal,
				// If the fragment was persisted _before_ our time bound (with a small adjustment
				// to account for clock drift), it cannot possibly contain messages published
				// _after_ the time bound.
				BeginModTime: bound.Time().Add(-time.Minute).Unix(),
			})
			if err != nil {
				log.WithField("err", err).Warn("failed to list fragments to identify lower-bound offset")
				continue
			} else if l := len(list.Fragments); l != 0 {
				offset = list.Fragments[0].Spec.Begin
			} else {
				log.Info("empty fragment listing; reading from offset zero")
			}
		}

		// Start a new shuffle stream, if none currently exists.
		if stream == nil {
			// Resolve coordinator shard to a current member process.
			var resolution, err = sr.resolver.Resolve(consumer.ResolveArgs{
				Context:  ctx,
				ShardID:  cfg.CoordinatorShard(),
				MayProxy: true,
			})
			if err != nil && resolution.Status != pc.Status_OK {
				log.WithField("err", err).
					WithField("status", resolution.Status.String()).
					Warn("failed to resolve shard (will retry)")
				continue
			}
			var ctx = pb.WithDispatchRoute(ctx, resolution.Header.Route, resolution.Header.ProcessId)

			var req = pf.ShuffleRequest{
				Config:     cfg,
				RingIndex:  int64(sr.ringIndex),
				Offset:     offset,
				Resolution: &resolution.Header,
			}
			if stream, err = sr.shuffleClient.Shuffle(ctx, &req); err != nil {
				log.WithField("err", err).Warn("failed to start shuffle RPC (will retry)")
				stream = nil
				continue
			}
		}

		// Read next ShuffleResponse.
		if err := stream.RecvMsg(&resp); err != nil || resp.Status != pc.Status_OK {
			if err == nil {
				// Server sent a !OK status, and will close. Read EOF to free resources.
				_, _ = stream.Recv()
			}
			log.WithField("err", err).
				WithField("status", resp.Status).
				Warn("shuffle stream failed (will retry)")
			stream = nil
			continue
		}

		attempt = 0 // Reset backoff timer.

		for _, doc := range resp.Documents {
			offset = doc.JournalBeginOffset + pb.Offset(len(doc.JournalBytes))

			if msg, err := NewRawJSONMessage(spec); err != nil {
				sr.envCh <- consumer.EnvelopeOrError{
					Error: fmt.Errorf("NewRawJSONMessage: %w", err),
				}
				return
			} else if err = msg.(json.Unmarshaler).UnmarshalJSON(doc.JournalBytes); err != nil {
				sr.envCh <- consumer.EnvelopeOrError{
					Error: fmt.Errorf("unmarshal of RawJSONMessage: %w", err),
				}
				return
			} else {
				sr.envCh <- consumer.EnvelopeOrError{
					Envelope: message.Envelope{
						Journal: spec,
						Begin:   doc.JournalBeginOffset,
						End:     offset,
						Message: msg,
					},
				}
			}
		}
	}
}

func (sr *shuffleReader) Stop() map[pb.Journal]pb.Offset {
	sr.cancel()
	sr.wg.Wait()

	if len(sr.activeReads) != 0 {
		log.WithField("activeReads", sr.activeReads).
			Panic("expected all active reads to have stopped")
	}
	return sr.idleOffsets
}

func backoff(attempt int) time.Duration {
	switch attempt {
	case 0:
		return 0
	case 1:
		return time.Millisecond * 10
	case 2, 3, 4, 5:
		return time.Second * time.Duration(attempt-1)
	default:
		return 5 * time.Second
	}
}
