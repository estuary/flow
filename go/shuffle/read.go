package shuffle

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// ReadBuilder builds instances of shuffled reads.
type ReadBuilder struct {
	buildID   string
	drainCh   <-chan struct{}
	journals  flow.Journals
	publisher ops.Publisher
	service   *consumer.Service
	shardID   pc.ShardID
	shuffles  []*pf.Shuffle

	// Members may change over the life of a ReadBuilder.
	// We're careful not to assume that values are stable. If they change,
	// that will flow through to changes in the selected Coordinator of
	// JournalShuffle configs, which will cause reads to be drained and
	// re-started with updated configurations.
	members func() []*pc.ShardSpec
}

// NewReadBuilder builds a new ReadBuilder of task |shuffles|
// using the given |buildID|, |journals|, and |service|,
// and scoped to the context of the given |shardID|.
// When |drainCh| closes, the ReadBuilder will gracefully converge
// to a drained state with no active reads.
func NewReadBuilder(
	buildID string,
	drainCh <-chan struct{},
	journals flow.Journals,
	publisher ops.Publisher,
	service *consumer.Service,
	shardID pc.ShardID,
	shuffles []*pf.Shuffle,
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
		buildID:   buildID,
		drainCh:   drainCh,
		journals:  journals,
		members:   members,
		publisher: publisher,
		service:   service,
		shardID:   shardID,
		shuffles:  shuffles,
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

type read struct {
	publisher ops.Publisher
	readDelay message.Clock
	req       pf.ShuffleRequest
	resp      pf.IndexedShuffleResponse
	spec      pb.JournalSpec

	// Fields filled when a read is start()'d.
	ctx       context.Context
	cancel    context.CancelFunc       // Cancel this read.
	ch        chan *pf.ShuffleResponse // Read responses.
	drainedCh chan struct{}            // Signaled when |ch| is emptied.

	// Terminal error, which is set immediately prior to |ch| being closed,
	// and which may be accessed only after reading a |ch| close.
	// If a |ch| close is read, |err| must be non-nil and will have
	// the value io.EOF under a nominal closure.
	chErr error
}

func (rb *ReadBuilder) buildReplayRead(journal pb.Journal, begin, end pb.Offset) (*read, error) {
	var out *read
	var err = walkReads(rb.shardID, rb.members(), rb.journals, rb.shuffles,
		func(range_ pf.RangeSpec, spec pb.JournalSpec, shuffle *pf.Shuffle, coordinator pc.ShardID) {
			if spec.Name != journal {
				return
			}

			var journalShuffle = pf.JournalShuffle{
				Journal:     spec.Name,
				Coordinator: coordinator,
				Shuffle:     shuffle,
				Replay:      true,
				BuildId:     rb.buildID,
			}
			out = &read{
				publisher: rb.publisher,
				spec:      spec,
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
				Journal:     spec.Name,
				Coordinator: coordinator,
				Shuffle:     shuffle,
				Replay:      false,
				BuildId:     rb.buildID,
			}

			var r, ok = existing[spec.Name]
			if ok {
				// A *read for this journal & transform already exists. If it's
				// JournalShuffle hasn't changed, keep it active (i.e., don't drain).
				if r.req.Shuffle.Equal(&journalShuffle) {
					delete(drain, spec.Name)
				} else {
					r.log(pf.LogLevel_debug,
						"draining read because its shuffle has changed",
						"next", map[string]interface{}{
							"build":       journalShuffle.BuildId,
							"coordinator": journalShuffle.Coordinator,
							"journal":     journalShuffle.Journal,
							"replay":      journalShuffle.Replay,
						},
					)
				}
				return
			}

			// A *read of this journal doesn't exist. Start one.
			var readDelay = message.NewClock(time.Unix(int64(shuffle.ReadDelaySeconds), 0)) -
				message.NewClock(time.Unix(0, 0))

			added[spec.Name] = &read{
				publisher: rb.publisher,
				spec:      spec,
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
	attempt int,
	resolveFn resolveFn,
	shuffler pf.ShufflerClient,
	wakeCh chan<- struct{},
) {
	// Wait for a back-off timer, or context cancellation.
	select {
	case <-ctx.Done(): // Fall through to error.
	case <-time.After(backoff(attempt)):
	}

	r.log(pf.LogLevel_debug, "started shuffle read", "attempt", attempt)

	ctx = pprof.WithLabels(ctx, pprof.Labels(
		"build", r.req.Shuffle.BuildId,
		"journal", r.req.Shuffle.Journal.String(),
		"replay", fmt.Sprint(r.req.Shuffle.Replay),
		"endOffset", fmt.Sprint(r.req.EndOffset),
		"offset", fmt.Sprint(r.req.Offset),
	))

	r.ctx, r.cancel = context.WithCancel(ctx)
	r.ch = make(chan *pf.ShuffleResponse, readChannelCapacity)
	r.drainedCh = make(chan struct{}, 1)

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
		r.sendReadResult(nil, fmt.Errorf("resolving coordinator: %w", err), wakeCh)
		return
	}
	r.req.Resolution = &resolution.Header

	if resolution.Store != nil {
		// We're the primary for the coordinating shard. We can directly
		// subscribe to the Store.Coordinator without going through gRPC.
		defer resolution.Done()

		resolution.Store.(Store).Coordinator().Subscribe(
			r.ctx,
			r.req,
			func(resp *pf.ShuffleResponse, err error) error {
				// Subscribe promises that that the last call (only) will deliver
				// a final error. This matches sendReadResult's expectation.
				return r.sendReadResult(resp, err, wakeCh)
			},
		)
	} else {
		// Coordinator is a remote shard. We must read over gRPC.
		ctx = pb.WithDispatchRoute(r.ctx, resolution.Header.Route, resolution.Header.ProcessId)

		go func() (err error) {
			pprof.SetGoroutineLabels(r.ctx)
			defer func() {
				// Deliver final non-nil error.
				_ = r.sendReadResult(nil, err, wakeCh)
			}()

			stream, err := shuffler.Shuffle(ctx, &r.req)
			if err != nil {
				return fmt.Errorf("opening Shuffle gRPC: %w", err)
			}

			for {
				if resp, err := stream.Recv(); err != nil {
					return err
				} else if err = r.sendReadResult(resp, nil, wakeCh); err != nil {
					return err
				}
			}
		}()
	}
}

// sendReadResult sends a ShuffleResponse or final non-nil error and close to the
// read's channel. It back-pressures to the caller using an exponential delay,
// and if the channel buffer would overflow it cancels the read's context.
//
// It's important that this doesn't naively stuff the read's channel and block
// indefinitely as this can cause a distributed read deadlock. Consider
// shard A & B, and journals X & Y:
//
//  - A's channel reading from X is stuffed
//  - B's channel reading from Y is stuffed
//  - A must read a next (non-tailing) Y to proceed.
//  - B must read a next (non-tailing) X to proceed, BUT
//  - X is blocked sending to the (stuffed) A, and
//  - Y is blocked sending to the (stuffed) B.
//  - Result: deadlock.
//
// The strategy we employ to avoid this is to use exponential time delays
// as the channel becomes full, up to the channel capacity, after which we
// cancel the read to release its server-side resources and prevent the server
// from blocking on send going forward.
func (r *read) sendReadResult(resp *pf.ShuffleResponse, err error, wakeCh chan<- struct{}) error {
	if err != nil {
		// This is a final call, delivering a terminal error.
		r.chErr = err
		close(r.ch)

		select {
		case wakeCh <- struct{}{}:
		default:
		}

		return nil
	}

	var queue, cap = len(r.ch), cap(r.ch)
	if queue == cap {
		r.log(pf.LogLevel_warn,
			"cancelling shuffle read due to full channel timeout",
			"queue", queue,
			"cap", cap,
		)
		r.cancel()
		return context.Canceled
	}

	if queue != 0 {
		var dur = time.Millisecond << (queue - 1)
		var timer = time.NewTimer(dur)

		select {
		case <-r.drainedCh:
			// Our channel was emptied while awaiting an (uncompleted) backoff,
			// which is now aborted.
			//
			// Or we read a stale notification (since cap(drainedCh) == 1),
			// and we're aborting the first (1ms) backoff interval prematurely.
			// This is an acceptable race and can only happen when len(ch) == 1.
			_ = timer.Stop() // Cleanup.
			// Fall through to send to channel.

		case <-timer.C:
			if queue > 13 { // Log values > 8s.
				r.log(pf.LogLevel_debug,
					"backpressure timer elapsed on a slow shuffle read",
					"queue", queue,
					"backoff", dur.Seconds(),
				)
			}
			// Fall through to send to channel.

		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}

	select {
	case r.ch <- resp:
	default:
		panic("cannot block: channel isn't full and we're the only sender")
	}

	select {
	case wakeCh <- struct{}{}:
	default:
	}

	return nil
}

// Next returns the next message.Envelope in the read sequence,
// or an EOF if none remain, or another encountered error.
// It's only used for replay reads and easier testing;
// ongoing reads poll the read channel directly.
func (r *read) next() (message.Envelope, error) {
	for r.resp.Index == len(r.resp.DocsJson) {
		// We must receive from the channel.
		var rr, ok = <-r.ch
		if err := r.onRead(rr, ok); err == nil {
			continue
		} else if err != nil {
			return message.Envelope{}, err
		}
	}
	return r.dequeue(), nil
}

func (r *read) onRead(p *pf.ShuffleResponse, ok bool) error {
	if !ok && r.chErr != nil {
		return r.chErr
	} else if !ok {
		panic("read !ok but chErr is nil")
	}

	r.resp.ShuffleResponse = *p
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

func (r *read) log(lvl pf.LogLevel, message string, fields ...interface{}) {
	if lvl > r.publisher.Labels().LogLevel {
		return
	}

	fields = append(fields,
		"request", map[string]interface{}{
			"build":       r.req.Shuffle.BuildId,
			"coordinator": r.req.Shuffle.Coordinator,
			"endOffset":   r.req.EndOffset,
			"journal":     r.req.Shuffle.Journal,
			"offset":      r.req.Offset,
			"range":       r.req.Range.String(),
		},
	)
	ops.PublishLog(r.publisher, lvl, message, fields...)
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
		var prefix = allocator.ItemKey(allJournals.KeySpace, shuffle.SourceCollection.String()) + "/"
		var sources = allJournals.Prefixed(prefix)

		for _, kv := range sources {
			var source = kv.Decoded.(allocator.Item).ItemValue.(*pb.JournalSpec)

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

	for _, spec := range specs {
		if len(spec.LabelSet.ValuesOf(labels.SplitSource)) != 0 {
			continue // Ignore shards which are splitting from parents.
		}

		var range_, err = labels.ParseRangeSpec(spec.LabelSet)
		if err != nil {
			return nil, fmt.Errorf("shard %s: %w", spec.Id, err)
		}

		// We expect |specs| to be strictly ordered on ascending RangeSpec.
		if l := len(out); l != 0 && !out[l-1].range_.Less(&range_) {
			return nil, fmt.Errorf("shard %s range %s is not less-than shard %s range %s",
				spec.Id,
				range_,
				out[l-1].spec.Id,
				out[l-1].range_)
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
	// Find the index of the first subscriber having |begin| <= |keyEnd|.
	start = sort.Search(len(s), func(i int) bool {
		return begin <= s[i].range_.KeyEnd
	})
	// Walk forwards while |keyBegin| < |end|.
	for stop = start; stop != len(s) && s[stop].range_.KeyBegin <= end; stop++ {
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

// readChannelCapacity is sized so that sendReadResult will overflow and
// cancel the read after ~35 minutes of no progress (1<<20 + 1<<19 + 1<<18 ... millis).
var readChannelCapacity = 22

func backoff(attempt int) time.Duration {
	// The choices of backoff time reflect that we're usually waiting for the
	// cluster to converge on a shared understanding of ownership, and that
	// involves a couple of Nagle-like read delays (~30ms) as Etcd watch
	// updates are applied by participants.
	switch attempt {
	case 0:
		return 0
	case 1:
		return time.Millisecond * 50
	case 2, 3:
		return time.Millisecond * 100
	case 4, 5:
		return time.Second
	default:
		return 5 * time.Second
	}
}
