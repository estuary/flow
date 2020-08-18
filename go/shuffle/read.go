package shuffle

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// ReadBuilder builds instances of shuffled reads.
type ReadBuilder struct {
	service *consumer.Service
	ranges  pf.RangeSpec
	// Transforms and members may change over the life of a ReadBuilder.
	// We're careful not to assume that values are stable. If they change,
	// that will flow through to changes of ShuffleConfigs, which will
	// cause reads to be drained and re-started with updated configurations.
	transforms func() []pf.TransformSpec
	members    func() []*pc.ShardSpec

	// These closures are simple wrappers which are easily mocked in testing.
	listJournals func(pb.ListRequest) *pb.ListResponse
	// journalsUpdateCh is signalled with each refresh of listJournals.
	// Journals must be inspected to determine if any have changed.
	journalsUpdateCh <-chan struct{}
}

// NewReadBuilder builds a new ReadBuilder.
func NewReadBuilder(
	service *consumer.Service,
	shard consumer.Shard,
	transforms func() []pf.TransformSpec,
) (*ReadBuilder, error) {

	// Build a RangeSpec from shard labels.
	var ranges, err = labels.ParseRangeSpec(shard.Spec().LabelSet)
	if err != nil {
		return nil, fmt.Errorf("extracting RangeSpec from shard: %w", err)
	}

	list, err := client.NewPolledList(
		shard.Context(),
		shard.JournalClient(),
		shuffleListingInterval,
		buildListRequest(transforms()))
	if err != nil {
		return nil, fmt.Errorf("initial journal listing failed: %w", err)
	}

	// Prefix is the "directory" portion of the ShardID,
	// up-to and including a final '/'.
	var prefix = shard.Spec().Id.String()
	prefix = prefix[:strings.LastIndexByte(prefix, '/')+1]
	prefix = allocator.ItemKey(service.State.KS, prefix)

	return &ReadBuilder{
		service:    service,
		ranges:     ranges,
		transforms: transforms,

		members: func() (out []*pc.ShardSpec) {
			service.State.KS.Mu.RLock()
			for _, m := range service.State.Items.Prefixed(prefix) {
				out = append(out, m.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec))
			}
			service.State.KS.Mu.RUnlock()
			return
		},
		listJournals: func(req pb.ListRequest) *pb.ListResponse {
			list.UpdateRequest(req)
			return list.List()
		},
		journalsUpdateCh: list.UpdateCh(),
	}, nil
}

// StartReplayRead builds and starts a read of the given journal and offset range.
func (rb *ReadBuilder) StartReplayRead(ctx context.Context, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	var r, err = rb.buildReplayRead(journal, begin, end)
	if err != nil {
		return message.IteratorFunc(func() (message.Envelope, error) {
			return message.Envelope{}, err
		})
	}
	rb.start(ctx, r)
	return r
}

type read struct {
	ctx    context.Context
	cancel context.CancelFunc
	spec   pb.JournalSpec
	req    pf.ShuffleRequest
	resp   pf.IndexedShuffleResponse
	stream pf.Shuffler_ShuffleClient

	pollAdjust message.Clock
	pollCh     chan *pf.ShuffleResponse
}

func (rb *ReadBuilder) buildReplayRead(journal pb.Journal, begin, end pb.Offset) (*read, error) {
	var (
		transforms = rb.transforms()
		journals   = rb.listJournals(buildListRequest(transforms))
	)

	var out *read
	var err = walkReads(rb.members(), journals.Journals, transforms,
		func(spec pb.JournalSpec, transform pf.TransformSpec, coordinator pc.ShardID) {
			if spec.Name != journal {
				return
			}

			var shuffle = pf.JournalShuffle{
				Journal:     spec.Name,
				Coordinator: coordinator,
				Shuffle:     transform.Shuffle,
			}
			out = &read{
				spec: spec,
				req: pf.ShuffleRequest{
					Shuffle:   shuffle,
					Range:     rb.ranges,
					Offset:    begin,
					EndOffset: end,
				},
				pollAdjust: 0, // Not used during replay.
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
	var (
		transforms = rb.transforms()
		members    = rb.members()
		journals   = rb.listJournals(buildListRequest(transforms))
	)

	added = make(map[pb.Journal]*read)
	// Initialize |drain| with all active reads, so that any read we do /not/
	// see during our walk below is marked as needing to be drained.
	drain = make(map[pb.Journal]*read, len(existing))
	for j, r := range existing {
		drain[j] = r
	}

	err = walkReads(members, journals.Journals, transforms,
		func(spec pb.JournalSpec, transform pf.TransformSpec, coordinator pc.ShardID) {
			// Build the configuration under which we'll read.
			var shuffle = pf.JournalShuffle{
				Journal:     spec.Name,
				Coordinator: coordinator,
				Shuffle:     transform.Shuffle,
			}

			var r, ok = existing[spec.Name]
			if ok {
				// A *read for this journal & transform already exists. If it's
				// JournalShuffle hasn't changed, keep it active (i.e., don't drain).
				if r.req.Shuffle.Equal(&shuffle) {
					delete(drain, spec.Name)
				}
				return
			}

			// A *read of this journal doesn't exist. Start one.
			var adjust = message.NewClock(time.Unix(int64(shuffle.ReadDelaySeconds), 0)) -
				message.NewClock(time.Unix(0, 0))

			added[spec.Name] = &read{
				spec: spec,
				req: pf.ShuffleRequest{
					Shuffle: shuffle,
					Range:   rb.ranges,
					Offset:  offsets[spec.Name],
				},
				pollAdjust: adjust,
			}
		})

	return
}

func (rb *ReadBuilder) start(ctx context.Context, r *read) error {
	r.log().Info("starting shuffled journal read")
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

	ctx = pb.WithDispatchRoute(r.ctx, resolution.Header.Route, resolution.Header.ProcessId)
	r.stream, err = pf.NewShufflerClient(rb.service.Loopback).Shuffle(ctx, &r.req)
	return err
}

// Next implements the message.Iterator interface.
func (r *read) Next() (env message.Envelope, err error) {
	// Note that this loop is used in replay mode, but not in polling mode.
	for r.resp.Index == len(r.resp.Begin) {
		if r.resp.ShuffleResponse, err = r.stream.Recv(); err != nil {
			return
		}
		r.resp.Index = 0
	}

	env = message.Envelope{
		Journal: &r.spec,
		Begin:   r.resp.Begin[r.resp.Index],
		End:     r.resp.End[r.resp.Index],
		Message: r.resp,
	}
	r.resp.Index++

	return env, nil
}

func (r *read) pump(ch chan<- struct{}) (err error) {
	defer func() {
		if err != nil {
			r.pollCh <- &pf.ShuffleResponse{TerminalError: err.Error()}
		}
		close(r.pollCh)
	}()

	for {
		var resp, err = r.stream.Recv()
		if err != nil {
			return fmt.Errorf("reading ShuffleResponse: %w", err)
		}

		select {
		case <-r.ctx.Done():
			return nil
		case r.pollCh <- resp:
		}

		// Signal to wake a blocked poll().
		select {
		case ch <- struct{}{}:
		default:
			// Don't block.
		}

		if l := len(resp.End); l != 0 {
			r.req.Offset = resp.End[l-1]
		}
	}
}

func (r *read) log() *log.Entry {
	return log.WithFields(log.Fields{
		"journal":     r.req.Shuffle.Journal,
		"coordinator": r.req.Shuffle.Coordinator,
		"transform":   r.req.Shuffle.Transform,
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

// Less orders *reads with respect to the adjusted Clocks of their next Document.
func (h *readHeap) Less(i, j int) bool {
	var lhs, rhs = (*h)[i], (*h)[j]

	var lc = lhs.resp.UuidParts[lhs.resp.Index].Clock + lhs.pollAdjust
	var rc = rhs.resp.UuidParts[rhs.resp.Index].Clock + rhs.pollAdjust
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

// buildListRequest returns a ListRequest which enumerates all journals of each
// collection serving as a source of one the provided Transforms.
func buildListRequest(transforms []pf.TransformSpec) pb.ListRequest {
	var sources = make(map[pf.Collection]struct{})
	for _, transform := range transforms {
		sources[transform.Source.Name] = struct{}{}
	}
	var out pb.ListRequest
	for source := range sources {
		out.Selector.Include.AddValue(labels.Collection, source.String())
	}
	return out
}

type shardsByKey []*pc.ShardSpec

func (s shardsByKey) len() int                 { return len(s) }
func (s shardsByKey) getKeyBegin(i int) []byte { return []byte(s[i].LabelSet.ValueOf(labels.KeyBegin)) }
func (s shardsByKey) getKeyEnd(i int) []byte   { return []byte(s[i].LabelSet.ValueOf(labels.KeyEnd)) }

func walkReads(members []*pc.ShardSpec, journals []pb.ListResponse_Journal, transforms []pf.TransformSpec,
	cb func(_ pb.JournalSpec, _ pf.TransformSpec, coordinator pc.ShardID)) error {

	// Sort |members| on ascending KeyBegin.
	sort.SliceStable(members, func(i, j int) bool {
		return members[i].LabelSet.ValueOf(labels.KeyBegin) < members[j].LabelSet.ValueOf(labels.KeyEnd)
	})

	// Generate hashes for each of |members| and |journals|, on their IDs/Names.
	var memberHashes = make([]uint32, len(members))
	for m := range members {
		memberHashes[m] = hashString(members[m].Id.String())
	}
	var journalHashes = make([]uint32, len(journals))
	for j := range journals {
		journalHashes[j] = hashString(journals[j].Spec.Name.String())
	}

	for j, journal := range journals {
		for _, transform := range transforms {
			if !transform.Source.Partitions.Matches(journal.Spec.LabelSet) {
				continue
			}

			var start, stop int
			if transform.Shuffle.UsesSourceKey {
				// This tranform uses the source's natural key, which means that the key ranges
				// present on JournalSpecs refer to the same keys as ShardSpecs. As an optimization
				// to reduce data movement, select only from ShardSpecs which overlap the journal.
				// Notice we're operating over the hex-encoded values here (which is order-preserving).
				start, stop = rangeSpan(shardsByKey(members),
					[]byte(journal.Spec.LabelSet.ValueOf(labels.KeyBegin)),
					[]byte(journal.Spec.LabelSet.ValueOf(labels.KeyEnd)),
				)
			} else {
				start, stop = 0, len(members)
			}

			// Augment JournalSpec to capture derivation and transform name on
			// whose behalf the read is being done.
			var spec = journal.Spec
			spec.Name = pb.Journal(fmt.Sprintf("%s?derivation=%s&transform=%s",
				journal.Spec.Name.String(),
				url.QueryEscape(transform.Derivation.Name.String()),
				url.QueryEscape(transform.Shuffle.Transform.String()),
			))

			if start == stop {
				return fmt.Errorf("none of %d shards cover journal %s", len(members), journal.Spec.Name)
			}
			var m = pickHRW(journalHashes[j], memberHashes, start, stop)
			cb(spec, transform, members[m].Id)
		}
	}
	return nil
}

func hashString(s string) uint32 {
	// This doesn't need to be cryptographic, but we use MD5 because FNV has a
	// pretty terrible avalanche factor (eg, very close inputs have outputs with
	// many co-occurring bits).
	var h = sha1.New()
	h.Write([]byte(s))

	var b [sha1.BlockSize]byte
	return binary.LittleEndian.Uint32(h.Sum(b[:0]))
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

const shuffleListingInterval = time.Second * 30
