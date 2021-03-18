package shuffle

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// TransformShuffles converts a slice of TransformSpecs to a slice of contained *Shuffles.
func TransformShuffles(transforms []pf.TransformSpec) []*pf.Shuffle {
	var shuffles = make([]*pf.Shuffle, len(transforms))
	for i := range transforms {
		shuffles[i] = &transforms[i].Shuffle
	}
	return shuffles
}

// ReadBuilder builds instances of shuffled reads.
type ReadBuilder struct {
	service  *consumer.Service
	journals flow.Journals
	ranges   pf.RangeSpec
	shuffles []*pf.Shuffle

	// Members may change over the life of a ReadBuilder.
	// We're careful not to assume that values are stable. If they change,
	// that will flow through to changes in the selected Coordinator of
	// JournalShuffle configs, which will cause reads to be drained and
	// re-started with updated configurations.
	members func() []*pc.ShardSpec
}

// NewReadBuilder builds a new ReadBuilder.
func NewReadBuilder(
	service *consumer.Service,
	journals flow.Journals,
	shard consumer.Shard,
	shuffles []*pf.Shuffle,
) (*ReadBuilder, error) {

	// Build a RangeSpec from shard labels.
	var ranges, err = labels.ParseRangeSpec(shard.Spec().LabelSet)
	if err != nil {
		return nil, fmt.Errorf("extracting RangeSpec from shard: %w", err)
	}

	// Prefix is the "directory" portion of the ShardID,
	// up-to and including a final '/'.
	var prefix = shard.Spec().Id.String()
	prefix = prefix[:strings.LastIndexByte(prefix, '/')+1]
	prefix = allocator.ItemKey(service.State.KS, prefix)

	var members = func() (out []*pc.ShardSpec) {
		service.State.KS.Mu.RLock()
		for _, m := range service.State.Items.Prefixed(prefix) {
			out = append(out, m.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec))
		}
		service.State.KS.Mu.RUnlock()
		return
	}

	return &ReadBuilder{
		service:  service,
		journals: journals,
		ranges:   ranges,
		shuffles: shuffles,
		members:  members,
	}, nil
}

// StartReplayRead builds and starts a read of the given journal and offset range.
func (rb *ReadBuilder) StartReplayRead(ctx context.Context, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	var r *read

	return message.IteratorFunc(func() (_ message.Envelope, err error) {
		var attempt int
		for {
			if r != nil {
				// Fall through to keep using |r|.
			} else if r, err = rb.buildReplayRead(journal, begin, end); err != nil {
				return message.Envelope{}, err
			} else if err = rb.start(ctx, r); err != nil {
				return message.Envelope{}, err
			}

			if sr := r.resp.ShuffleResponse; r.resp.Index != len(sr.DocsJson) {
				return r.dequeue(), nil
			}

			// We must receive from the stream.
			if err = r.onRead(r.doRead()); err == nil && r.resp.TerminalError == "" {
				continue
			} else if err == io.EOF {
				return message.Envelope{}, err
			} else if err == nil {
				return message.Envelope{}, errors.New(r.resp.TerminalError)
			}

			// Stream is broken, but may be retried.
			r.log().WithFields(log.Fields{
				"err":     err,
				"attempt": attempt,
			}).Warn("failed to receive shuffled replay read (will retry)")

			switch attempt {
			case 0, 1: // Don't wait.
			default:
				time.Sleep(5 * time.Second)
			}
			attempt++

			begin, r = r.req.Offset, nil
		}
	})
}

// ReadThrough filters the input |offsets| to those journals and offsets which are
// actually read by this ReadBuilder. It powers the shard Stat RPC.
func (rb *ReadBuilder) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	var out = make(pb.Offsets, len(offsets))
	var err = walkReads(rb.members(), rb.journals, rb.shuffles,
		func(spec pb.JournalSpec, _ *pf.Shuffle, _ pc.ShardID) {
			if offset := offsets[spec.Name]; offset != 0 {
				// Prefer an offset that exactly matches our journal + metadata extension.
				out[spec.Name] = offset
			} else if offset = offsets[spec.Name.StripMeta()]; offset != 0 {
				// Otherwise, if there's an offset that matches the Journal name,
				// then project it to our metadata extension.
				out[spec.Name] = offset
			}
		})
	return out, err
}

type read struct {
	ctx    context.Context
	cancel context.CancelFunc
	spec   pb.JournalSpec
	req    pf.ShuffleRequest
	resp   pf.IndexedShuffleResponse
	stream pf.Shuffler_ShuffleClient

	// Positive delta by which documents are effectively delayed w.r.t. other
	// documents, as well as literally delayed (by gating) w.r.t current wall-time.
	readDelay message.Clock
	pollCh    chan readResult
}

type readResult struct {
	resp *pf.ShuffleResponse
	err  error
}

func (rb *ReadBuilder) buildReplayRead(journal pb.Journal, begin, end pb.Offset) (*read, error) {
	var out *read
	var err = walkReads(rb.members(), rb.journals, rb.shuffles,
		func(spec pb.JournalSpec, shuffle *pf.Shuffle, coordinator pc.ShardID) {
			if spec.Name != journal {
				return
			}

			var journalShuffle = pf.JournalShuffle{
				Journal:     spec.Name,
				Coordinator: coordinator,
				Shuffle:     shuffle,
				Replay:      true,
			}
			out = &read{
				spec: spec,
				req: pf.ShuffleRequest{
					Shuffle:   journalShuffle,
					Range:     rb.ranges,
					Offset:    begin,
					EndOffset: end,
				},
				resp:      pf.IndexedShuffleResponse{Shuffle: shuffle},
				readDelay: 0, // Not used during replay.
			}
		})

	if err != nil {
		return nil, err
	} else if out == nil {
		return nil, fmt.Errorf("journal not matched for replay: %s", journal)
	}
	return out, nil
}

func (rb *ReadBuilder) buildReads(existing map[pb.Journal]*read, offsets pb.Offsets,
) (added map[pb.Journal]*read, drain map[pb.Journal]*read, err error) {

	added = make(map[pb.Journal]*read)
	// Initialize |drain| with all active reads, so that any read we do /not/
	// see during our walk below is marked as needing to be drained.
	drain = make(map[pb.Journal]*read, len(existing))
	for j, r := range existing {
		drain[j] = r
	}

	err = walkReads(rb.members(), rb.journals, rb.shuffles,
		func(spec pb.JournalSpec, shuffle *pf.Shuffle, coordinator pc.ShardID) {
			// Build the configuration under which we'll read.
			var journalShuffle = pf.JournalShuffle{
				Journal:     spec.Name,
				Coordinator: coordinator,
				Shuffle:     shuffle,
				Replay:      false,
			}

			var r, ok = existing[spec.Name]
			if ok {
				// A *read for this journal & transform already exists. If it's
				// JournalShuffle hasn't changed, keep it active (i.e., don't drain).
				if r.req.Shuffle.Equal(&journalShuffle) {
					delete(drain, spec.Name)
				}
				return
			}

			// A *read of this journal doesn't exist. Start one.
			var readDelay = message.NewClock(time.Unix(int64(shuffle.ReadDelaySeconds), 0)) -
				message.NewClock(time.Unix(0, 0))

			added[spec.Name] = &read{
				spec: spec,
				req: pf.ShuffleRequest{
					Shuffle: journalShuffle,
					Range:   rb.ranges,
					Offset:  offsets[spec.Name],
				},
				resp:      pf.IndexedShuffleResponse{Shuffle: shuffle},
				readDelay: readDelay,
			}
		})

	return
}

func (rb *ReadBuilder) start(ctx context.Context, r *read) error {
	r.log().Debug("starting shuffled journal read")
	r.ctx, r.cancel = context.WithCancel(ctx)

	// Resolve coordinator shard to a current member process.
	var resolution, err = rb.service.Resolver.Resolve(consumer.ResolveArgs{
		Context:  r.ctx,
		ShardID:  r.req.Shuffle.Coordinator,
		MayProxy: true,
	})
	if err == nil && resolution.Status != pc.Status_OK {
		err = fmt.Errorf(resolution.Status.String())
	}
	if err != nil {
		return fmt.Errorf("resolving coordinating shard: %w", err)
	}
	r.req.Resolution = &resolution.Header

	// We're the primary for the coordinating shard.
	if resolution.Store != nil {
		// TODO: Pluck out coordinator and dispatch request directly to it,
		// without going through gRPC. E.g.:
		//
		// var coordinator = resolution.Store.(Store).Coordinator()
		//   ... do stuff with coordinator ...

		resolution.Done()
	}

	ctx = pb.WithDispatchRoute(r.ctx, resolution.Header.Route, resolution.Header.ProcessId)
	r.stream, err = pf.NewShufflerClient(rb.service.Loopback).Shuffle(ctx, &r.req)
	return err
}

func (r *read) doRead() (out readResult) {
	out.resp, out.err = r.stream.Recv()
	return
}

func (r *read) onRead(p readResult) error {
	if p.err != nil {
		return p.err
	}

	if p.resp != nil {
		r.resp.ShuffleResponse = *p.resp
	} else {
		r.resp.ShuffleResponse = pf.ShuffleResponse{}
	}
	r.resp.Index = 0 // Reset.

	// Update Offset as responses are read, so that a retry
	// of this *read knows where to pick up reading from.
	if l := len(r.resp.End); l != 0 {
		r.req.Offset = r.resp.ShuffleResponse.End[l-1]
	}
	return nil
}

// dequeue the next ready message from the current Response.
// There must be one, or dequeue panics.
func (r *read) dequeue() message.Envelope {
	var env = message.Envelope{
		Journal: &r.spec,
		Begin:   r.resp.Begin[r.resp.Index],
		End:     r.resp.End[r.resp.Index],
		Message: r.resp,
	}
	r.resp.Index++

	return env
}

func (r *read) pump(readyCh chan<- struct{}) {
	for {
		var rx = r.doRead()

		select {
		case <-r.ctx.Done():
			rx.err = r.ctx.Err()
		case r.pollCh <- rx:
		}

		if rx.err != nil {
			close(r.pollCh)
			return
		}

		// Signal to wake a blocked poll().
		select {
		case readyCh <- struct{}{}:
		default:
			// Don't block.
		}
	}
}

func (r *read) log() *log.Entry {
	return log.WithFields(log.Fields{
		"journal":     r.req.Shuffle.Journal,
		"coordinator": r.req.Shuffle.Coordinator,
		"offset":      r.req.Offset,
		"endOffset":   r.req.EndOffset,
		"range":       &r.req.Range,
	})
}

type readHeap []*read

// Len is the number of elements in the heap.
func (h *readHeap) Len() int { return len(*h) }

// Swap swaps the elements with indexes i and j.
func (h *readHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

// Less orders *reads by their relative priorities,
// then by the adjusted Clocks of their next Document.
func (h *readHeap) Less(i, j int) bool {
	var lhs, rhs = (*h)[i], (*h)[j]

	// Prefer a read with higher priority.
	if lhs.req.Shuffle.Priority != rhs.req.Shuffle.Priority {
		return lhs.req.Shuffle.Priority > rhs.req.Shuffle.Priority
	}
	// Then prefer a document with an earlier adjusted clock.
	var lc = lhs.resp.UuidParts[lhs.resp.Index].Clock + lhs.readDelay
	var rc = rhs.resp.UuidParts[rhs.resp.Index].Clock + rhs.readDelay
	return lc < rc
}

func (h *readHeap) Push(x interface{}) {
	*h = append(*h, x.(*read))
}

func (h *readHeap) Pop() interface{} {
	var n = len(*h)
	var x = (*h)[n-1]
	*h = (*h)[0 : n-1]
	return x
}

type shardsByKey []*pc.ShardSpec

func (s shardsByKey) len() int                 { return len(s) }
func (s shardsByKey) getKeyBegin(i int) []byte { return []byte(s[i].LabelSet.ValueOf(labels.KeyBegin)) }
func (s shardsByKey) getKeyEnd(i int) []byte   { return []byte(s[i].LabelSet.ValueOf(labels.KeyEnd)) }

func walkReads(members []*pc.ShardSpec, allJournals flow.Journals, shuffles []*pf.Shuffle,
	cb func(_ pb.JournalSpec, _ *pf.Shuffle, coordinator pc.ShardID)) error {

	// Generate hashes for each of |members| derived from IDs.
	var memberHashes = make([]uint32, len(members))
	for m := range members {
		memberHashes[m] = hashString(members[m].Id.String())
	}

	allJournals.Mu.RLock()
	defer allJournals.Mu.RUnlock()

	for _, shuffle := range shuffles {
		var sources = allJournals.Prefixed(allJournals.Root + "/" + shuffle.SourceCollection.String() + "/")

		for _, kv := range sources {
			var source = kv.Decoded.(*pb.JournalSpec)

			if !shuffle.SourcePartitions.Matches(source.LabelSet) {
				continue
			}

			var start, stop int
			if shuffle.UsesSourceKey {
				// This tranform uses the source's natural key, which means that the key ranges
				// present on JournalSpecs refer to the same keys as ShardSpecs. As an optimization
				// to reduce data movement, select only from ShardSpecs which overlap the journal.
				// Notice we're operating over the hex-encoded values here (which is order-preserving).
				start, stop = rangeSpan(shardsByKey(members),
					[]byte(source.LabelSet.ValueOf(labels.KeyBegin)),
					[]byte(source.LabelSet.ValueOf(labels.KeyEnd)),
				)
			} else {
				start, stop = 0, len(members)
			}

			// Augment JournalSpec to capture the shuffle group name, as a Journal metadata path segment.
			var copied = *source
			copied.Name = pb.Journal(fmt.Sprintf("%s;%s", source.Name.String(), shuffle.GroupName))

			if start == stop {
				return fmt.Errorf("none of %d shards cover journal %s", len(members), copied.Name)
			}
			var m = pickHRW(hashString(copied.Name.String()), memberHashes, start, stop)
			cb(copied, shuffle, members[m].Id)
		}
	}
	return nil
}

func hashString(s string) uint32 {
	var h = fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func pickHRW(h uint32, from []uint32, start, stop int) int {
	var max uint32
	var at int
	for i := start; i != stop; i++ {
		if n := from[i] ^ h; max < n {
			max, at = n, i
		}
	}
	return at
}

const shuffleListingInterval = time.Second * 30
