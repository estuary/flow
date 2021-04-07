package shuffle

import (
	"context"
	"fmt"
	"sort"
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

// TaskShuffles extracts a []*pf.Shuffle slice from a CatalogTask.
func TaskShuffles(task *pf.CatalogTask) []*pf.Shuffle {
	if task.Derivation != nil {
		var shuffles = make([]*pf.Shuffle, len(task.Derivation.Transforms))
		for i := range task.Derivation.Transforms {
			shuffles[i] = &task.Derivation.Transforms[i].Shuffle
		}
		return shuffles
	}
	if task.Materialization != nil {
		return []*pf.Shuffle{&task.Materialization.Shuffle}
	}
	return nil
}

// ReadBuilder builds instances of shuffled reads.
type ReadBuilder struct {
	service    *consumer.Service
	journals   flow.Journals
	shardID    pc.ShardID
	shuffles   []*pf.Shuffle
	commonsId  string
	commonsRev int64
	drainCh    chan struct{}

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
	shardID pc.ShardID,
	shuffles []*pf.Shuffle,
	commonsID string,
	commonsRev int64,
) (*ReadBuilder, error) {
	// Prefix is the "directory" portion of the ShardID,
	// up-to and including a final '/'.
	var prefix = shardID.String()
	prefix = prefix[:strings.LastIndexByte(prefix, '/')+1]

	var members = func() (out []*pc.ShardSpec) {
		var prefix = allocator.ItemKey(service.State.KS, prefix)

		service.State.KS.Mu.RLock()
		for _, m := range service.State.Items.Prefixed(prefix) {
			out = append(out, m.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec))
		}
		service.State.KS.Mu.RUnlock()
		return
	}

	return &ReadBuilder{
		service:    service,
		journals:   journals,
		shardID:    shardID,
		shuffles:   shuffles,
		commonsId:  commonsID,
		commonsRev: commonsRev,
		drainCh:    make(chan struct{}),
		members:    members,
	}, nil
}

// ReadThrough filters the input |offsets| to those journals and offsets which are
// actually read by this ReadBuilder. It powers the shard Stat RPC.
func (rb *ReadBuilder) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	var out = make(pb.Offsets, len(offsets))
	var err = walkReads(rb.shardID, rb.members(), rb.journals, rb.shuffles,
		func(_ pf.RangeSpec, spec pb.JournalSpec, _ *pf.Shuffle, _ pc.ShardID) {
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

// Drain the ReadBuilder, causing it to converge to a drained state with no active reads.
func (rb *ReadBuilder) Drain() {
	log.WithFields(log.Fields{
		"shard":    rb.shardID,
		"revision": rb.commonsRev,
	}).Debug("draining shuffled reads")

	close(rb.drainCh)
}

type read struct {
	spec      pb.JournalSpec
	req       pf.ShuffleRequest
	resp      pf.IndexedShuffleResponse
	readDelay message.Clock

	// Fields filled when a read is start()'d.
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan readResult
}

type readResult struct {
	resp *pf.ShuffleResponse
	err  error
}

func (rb *ReadBuilder) buildReplayRead(journal pb.Journal, begin, end pb.Offset) (*read, error) {
	var out *read
	var err = walkReads(rb.shardID, rb.members(), rb.journals, rb.shuffles,
		func(range_ pf.RangeSpec, spec pb.JournalSpec, shuffle *pf.Shuffle, coordinator pc.ShardID) {
			if spec.Name != journal {
				return
			}

			var journalShuffle = pf.JournalShuffle{
				Journal:         spec.Name,
				Coordinator:     coordinator,
				Shuffle:         shuffle,
				Replay:          true,
				CommonsId:       rb.commonsId,
				CommonsRevision: rb.commonsRev,
			}
			out = &read{
				spec: spec,
				req: pf.ShuffleRequest{
					Shuffle:   journalShuffle,
					Range:     range_,
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

func (rb *ReadBuilder) buildReads(
	existing map[pb.Journal]*read,
	offsets pb.Offsets,
) (
	added map[pb.Journal]*read,
	drain map[pb.Journal]*read,
	err error,
) {
	added = make(map[pb.Journal]*read)
	// Initialize |drain| with all active reads, so that any read we do /not/
	// see during our walk below is marked as needing to be drained.
	drain = make(map[pb.Journal]*read, len(existing))
	for j, r := range existing {
		drain[j] = r
	}

	// Poll to check if we've been signaled to drain.
	select {
	case <-rb.drainCh:
		rb.drainCh = nil
	default:
		// Pass.
	}

	// If we've been signaled to drain, no reads are |added|
	// and all existing reads are |drain|.
	if rb.drainCh == nil {
		return
	}

	err = walkReads(rb.shardID, rb.members(), rb.journals, rb.shuffles,
		func(range_ pf.RangeSpec, spec pb.JournalSpec, shuffle *pf.Shuffle, coordinator pc.ShardID) {
			// Build the configuration under which we'll read.
			var journalShuffle = pf.JournalShuffle{
				Journal:         spec.Name,
				Coordinator:     coordinator,
				Shuffle:         shuffle,
				Replay:          false,
				CommonsId:       rb.commonsId,
				CommonsRevision: rb.commonsRev,
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
					Range:   range_,
					Offset:  offsets[spec.Name],
				},
				resp:      pf.IndexedShuffleResponse{Shuffle: shuffle},
				readDelay: readDelay,
			}
		})

	return
}

func (r *read) start(
	ctx context.Context,
	resolveFn resolveFn,
	shuffler pf.ShufflerClient,
	wakeCh chan<- struct{},
) {
	r.log().Debug("starting shuffled journal read")
	r.ctx, r.cancel = context.WithCancel(ctx)

	// Use a minimal buffer to quickly back-pressure to the server coordinator,
	// so that it packs more data into each ShuffleResponse.
	r.ch = make(chan readResult, 1)

	// Resolve coordinator shard to a current member process.
	var resolution, err = resolveFn(consumer.ResolveArgs{
		Context:  r.ctx,
		ShardID:  r.req.Shuffle.Coordinator,
		MayProxy: true,
	})
	if err == nil && resolution.Status != pc.Status_OK {
		err = fmt.Errorf(resolution.Status.String())
	}
	if err != nil {
		r.ch <- readResult{err: fmt.Errorf("resolving coordinating shard: %w", err)}
		close(r.ch)
		return
	}
	r.req.Resolution = &resolution.Header

	if resolution.Store != nil {
		// We're the primary for the coordinating shard. We can directly
		// subscribe to the Store.Coordinator without going through gRPC.
		defer resolution.Done()

		resolution.Store.(Store).Coordinator().Subscribe(
			r.ctx,
			&r.req,
			func(resp *pf.ShuffleResponse, err error) error {
				select {
				case r.ch <- readResult{resp: resp, err: err}:
				case <-r.ctx.Done(): // Drop on the floor.
				}

				if err != nil {
					// Coordinator.Subscribe contract is that err != nil
					// is always the finall callback.
					close(r.ch)
				}

				select {
				case wakeCh <- struct{}{}:
				default:
				}

				return nil
			},
		)
	} else {
		// Coordinator is a remote shard. We must read over gRPC.
		ctx = pb.WithDispatchRoute(r.ctx, resolution.Header.Route, resolution.Header.ProcessId)

		go func(ch chan<- readResult) (err error) {
			defer func() {
				ch <- readResult{err: err}
				close(ch)

				select {
				case wakeCh <- struct{}{}:
				default:
				}
			}()

			stream, err := shuffler.Shuffle(ctx, &r.req)
			if err != nil {
				return fmt.Errorf("opening Shuffle gRPC: %w", err)
			}

			for {
				var resp, err = stream.Recv()
				if err != nil {
					return err
				}

				ch <- readResult{resp: resp}

				select {
				case wakeCh <- struct{}{}:
				default:
				}
			}

		}(r.ch)
	}
}

// Next returns the next message.Envelope in the read sequence,
// or an EOF if none remain, or another encountered error.
// It's only used for replay reads and easier testing;
// ongoing reads poll the read channel directly.
func (r *read) next() (message.Envelope, error) {
	for r.resp.Index == len(r.resp.DocsJson) {
		// We must receive from the channel.
		if err := r.onRead(<-r.ch); err == nil {
			continue
		} else if err != nil {
			return message.Envelope{}, err
		}
	}
	return r.dequeue(), nil
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
	if l := len(r.resp.Offsets); l != 0 {
		r.req.Offset = r.resp.ShuffleResponse.Offsets[l-1]
	}
	return nil
}

// dequeue the next ready message from the current Response.
// There must be one, or dequeue panics.
func (r *read) dequeue() message.Envelope {
	var env = message.Envelope{
		Journal: &r.spec,
		Begin:   r.resp.Offsets[2*r.resp.Index],
		End:     r.resp.Offsets[2*r.resp.Index+1],
		Message: r.resp,
	}
	r.resp.Index++

	return env
}

func (r *read) log() *log.Entry {
	return log.WithFields(log.Fields{
		"journal":     r.req.Shuffle.Journal,
		"coordinator": r.req.Shuffle.Coordinator,
		"offset":      r.req.Offset,
		"endOffset":   r.req.EndOffset,
		"range":       &r.req.Range,
		"revision":    r.req.Shuffle.CommonsRevision,
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

func walkReads(id pc.ShardID, shardSpecs []*pc.ShardSpec, allJournals flow.Journals, shuffles []*pf.Shuffle,
	cb func(_ pf.RangeSpec, _ pb.JournalSpec, _ *pf.Shuffle, coordinator pc.ShardID)) error {

	var members, err = newShuffleMembers(shardSpecs)
	if err != nil {
		return fmt.Errorf("shuffle member ShardSpecs: %w", err)
	}
	var index = sort.Search(len(members), func(i int) bool {
		return id <= members[i].spec.Id
	})
	if index == len(members) || id != members[index].spec.Id {
		return fmt.Errorf("shard %s not found among shuffle members", id)
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

			// Extract owned key range from journal labels.
			sourceBegin, err := labels.ParseHexU32Label(labels.KeyBegin, source.LabelSet)
			if err != nil {
				return fmt.Errorf("shuffle JournalSpec: %w", err)
			}
			sourceEnd, err := labels.ParseHexU32Label(labels.KeyEnd, source.LabelSet)
			if err != nil {
				return fmt.Errorf("shuffle JournalSpec: %w", err)
			}

			var start, stop int
			if shuffle.UsesSourceKey {
				// This tranform uses the source's natural key, which means that the key ranges
				// present on JournalSpecs refer to the same keys as ShardSpecs. As an optimization
				// to reduce data movement, select only from ShardSpecs which overlap the journal.
				start, stop = rangeSpan(members, sourceBegin, sourceEnd)
			} else {
				start, stop = 0, len(members)
			}

			// Augment JournalSpec to capture the shuffle group name, as a Journal metadata path segment.
			var copied = *source
			copied.Name = pb.Journal(fmt.Sprintf("%s;%s", source.Name.String(), shuffle.GroupName))

			if start == stop {
				return fmt.Errorf("none of %d shards overlap the key-range of journal %s, %08x-%08x",
					len(members), source.Name, sourceBegin, sourceEnd)
			}

			var m = pickHRW(hrwHash(copied.Name.String()), members, start, stop)
			cb(members[index].range_, copied, shuffle, members[m].spec.Id)
		}
	}
	return nil
}

// shuffleMember is a parsed ShardSpec representation used for walking reads.
type shuffleMember struct {
	spec    *pc.ShardSpec
	range_  pf.RangeSpec
	hrwHash uint32
}

// newShuffleMembers builds shuffleMembers from ShardSpecs.
func newShuffleMembers(specs []*pc.ShardSpec) ([]shuffleMember, error) {
	var out = make([]shuffleMember, 0, len(specs))

	for i, spec := range specs {
		var range_, err = labels.ParseRangeSpec(spec.LabelSet)
		if err != nil {
			return nil, fmt.Errorf("shard %s: %w", spec.Id, err)
		}

		// We expect |specs| to be strictly ordered on ascending RangeSpec.
		if i != 0 && !out[i-1].range_.Less(&range_) {
			return nil, fmt.Errorf("shard %s range %s is not less-than shard %s range %s",
				spec.Id,
				range_,
				out[i-1].spec.Id,
				out[i-1].range_)
		}

		out = append(out, shuffleMember{
			spec:    spec,
			range_:  range_,
			hrwHash: hrwHash(spec.Id.String()),
		})
	}

	return out, nil
}

// rangeSpan locates the span of []shuffleMember having owned key ranges
// which overlap the given range.
func rangeSpan(s []shuffleMember, begin, end uint32) (start, stop int) {
	// Find the index of the first subscriber having |begin| < |keyEnd|.
	start = sort.Search(len(s), func(i int) bool {
		return begin < s[i].range_.KeyEnd
	})
	// Walk forwards while |keyBegin| < |end|.
	for stop = start; stop != len(s) && s[stop].range_.KeyBegin < end; stop++ {
	}
	return
}

func hrwHash(s string) uint32 {
	// We use HH64 for convenience. This could be any reasonable hash function
	// and is unrelated to the hash applied to shuffle keys.
	return flow.PackedKeyHash_HH64([]byte(s))
}

func pickHRW(h uint32, from []shuffleMember, start, stop int) int {
	var max uint32
	var at int
	for i := start; i != stop; i++ {
		if n := from[i].hrwHash ^ h; max < n {
			max, at = n, i
		}
	}
	return at
}
