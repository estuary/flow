package flow

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	pf "github.com/estuary/flow/go/protocol"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
)

type shuffleReader struct {
	ctx        context.Context
	cancel     context.CancelFunc
	polledList *client.PolledList

	rawJSONMeta *RawJSONMeta
	envCh       chan<- consumer.EnvelopeOrError
	idleOffsets map[pb.Journal]pb.Offset
	activeReads map[pb.Journal]context.CancelFunc

	ring      []pf.Ring
	ringName  string
	ringIndex int
	shuffles  []pf.ShuffleRequest_Shuffle
	resolver  *consumer.Resolver

	client pf.ShufflerClient

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
		go sr.journalReadLoop(subCtx, journal, offset)

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

func (sr *shuffleReader) journalReadLoop(ctx context.Context, journal pb.ListResponse_Journal, offset pb.Offset) {
	defer func() {
		sr.mu.Lock()
		sr.idleOffsets[journal.Spec.Name] = offset
		delete(sr.activeReads, journal.Spec.Name)
		sr.mu.Unlock()

		sr.wg.Done()
	}()

	// Hash and map journal to a coordinating participant of the current ring.
	// We use SHA-2 for it's strong collision avoidance properties, and not for security.
	// Journals often differ from one another by a single bit (eg, 'foo-002' vs 'foo-003'),
	// which can cause grouping with FNV-a and other standard non-secure hashes.
	var hash = sha256.Sum256([]byte(journal.Spec.Name))
	var index = binary.LittleEndian.Uint32(hash[:]) % sr.ring[len(sr.ring)-1].TotalReaders
	var coordinator = pc.ShardID(fmt.Sprintf("%s-%03d", sr.ringName, index))

	var log = log.WithFields(log.Fields{
		"ring":        sr.ringName,
		"ringIndex":   sr.ringIndex,
		"journal":     journal.Spec.Name,
		"coordinator": coordinator,
		"offset":      offset,
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

		if stream == nil {
			// Resolve |coordinator| to a current member process.
			var resolution, err = sr.resolver.Resolve(consumer.ResolveArgs{
				Context:  ctx,
				ShardID:  coordinator,
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
				Journal:     journal.Spec.Name,
				ContentType: journal.Spec.LabelSet.ValueOf(labels.ContentType),
				Offset:      offset,
				ReaderIndex: int64(sr.ringIndex),
				ReaderRing:  sr.ring,
				Shuffles:    sr.shuffles,
				Coordinator: coordinator,
				Resolution:  &resolution.Header,
			}
			if stream, err = sr.client.Shuffle(ctx, &req); err != nil {
				log.WithField("err", err).Warn("failed to start shuffle RPC (will retry)")
				stream = nil
				continue
			}
		}

		if err := stream.RecvMsg(&resp); err != nil || resp.Status != pc.Status_OK {
			if resp.Status != pc.Status_OK {
				_, _ = stream.Recv() // Read stream EOF to free resources.
			}
			log.WithField("err", err).
				WithField("status", resp.Status).
				Warn("shuffle stream failed (will retry)")
			stream = nil
			continue
		}

		attempt = 0
		for _, doc := range resp.Documents {
			offset = doc.JournalBeginOffset + pb.Offset(len(doc.JournalBytes))

			sr.envCh <- consumer.EnvelopeOrError{
				Envelope: message.Envelope{
					Journal: &journal.Spec,
					Begin:   doc.JournalBeginOffset,
					End:     offset,
					Message: &RawJSONMessage{
						Meta:               sr.rawJSONMeta,
						RawMessage:         doc.JournalBytes,
						ShuffledTransforms: doc.ShuffleIds,
					},
				},
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
