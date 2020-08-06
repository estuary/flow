package shuffle

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// ReadBuilder builds instances of shuffled reads.
type ReadBuilder struct {
	service *consumer.Service

	// Transforms and ring may change over the life of a ReadBuilder
	// (though |ringIndex| may not). We're careful not to assume that
	// these values are stable. If they change, that will flow through
	// to changes of ShuffleConfigs, which will cause reads to be drained
	// and re-started with updated configurations.
	transforms func() []pf.TransformSpec
	ring       func() pf.Ring
	ringIndex  uint32

	// These closures are simple wrappers which are easily mocked in testing.
	listJournals  func(pb.ListRequest) *pb.ListResponse
	listFragments func(pb.FragmentsRequest) (*pb.FragmentsResponse, error)
	// journalsUpdateCh is signalled with each refresh of listJournals.
	// Journals must be inspected to determine if any have changed.
	journalsUpdateCh <-chan struct{}
}

// NewReadBuilder builds a new ReadBuilder.
func NewReadBuilder(
	service *consumer.Service,
	shard consumer.Shard,
	ring func() pf.Ring,
	transforms func() []pf.TransformSpec,
) (*ReadBuilder, error) {

	// Determine the ring index of this shard.
	var ringIndex, err = strconv.Atoi(shard.Spec().LabelSet.ValueOf(labels.WorkerIndex))
	if err != nil {
		return nil, fmt.Errorf("failed to extract shard worker index: %w", err)
	}

	list, err := client.NewPolledList(
		shard.Context(),
		shard.JournalClient(),
		shuffleListingInterval,
		buildListRequest(transforms()))
	if err != nil {
		return nil, fmt.Errorf("initial journal listing failed: %w", err)
	}

	return &ReadBuilder{
		service:    service,
		transforms: transforms,
		ring:       ring,
		ringIndex:  uint32(ringIndex),

		listJournals: func(req pb.ListRequest) *pb.ListResponse {
			list.UpdateRequest(req)
			return list.List()
		},
		listFragments: func(req pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
			return client.ListAllFragments(shard.Context(), shard.JournalClient(), req)
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
		ring       = rb.ring()
		transforms = rb.transforms()
		journals   = rb.listJournals(buildListRequest(transforms))
	)

	var out *read
	walkReads(len(ring.Members), journals.Journals, transforms,
		func(spec pb.JournalSpec, transform pf.TransformSpec, coordinator int) {
			if spec.Name != journal {
				return
			}

			var config = pf.ShuffleConfig{
				Journal:     spec.Name,
				Ring:        ring,
				Coordinator: uint32(coordinator),
				Shuffle:     transform.Shuffle,
			}
			out = &read{
				spec: spec,
				req: pf.ShuffleRequest{
					Config:    config,
					RingIndex: rb.ringIndex,
					Offset:    begin,
					EndOffset: end,
				},
				pollAdjust: 0, // Not used during replay.
			}
		})

	if out == nil {
		return nil, fmt.Errorf("journal not matched for replay: %s", journal)
	}
	return out, nil
}

func (rb *ReadBuilder) buildReads(existing map[pb.Journal]*read, offsets pb.Offsets,
) (added map[pb.Journal]*read, drain map[pb.Journal]*read) {
	var (
		ring       = rb.ring()
		transforms = rb.transforms()
		journals   = rb.listJournals(buildListRequest(transforms))
	)

	added = make(map[pb.Journal]*read)
	// Initialize |drain| with all active reads, so that any read we do /not/
	// see during our walk below is marked as needing to be drained.
	drain = make(map[pb.Journal]*read, len(existing))
	for j, r := range existing {
		drain[j] = r
	}

	walkReads(len(ring.Members), journals.Journals, transforms,
		func(spec pb.JournalSpec, transform pf.TransformSpec, coordinator int) {
			// Build the configuration under which we'll read.
			var config = pf.ShuffleConfig{
				Journal:     spec.Name,
				Ring:        ring,
				Coordinator: uint32(coordinator),
				Shuffle:     transform.Shuffle,

				// TODO(johnny): Include ReadDelaySecs to incorporate into equality checks.
			}

			var r, ok = existing[spec.Name]
			if ok {
				// A *read for this journal & transform already exists. If it's
				// ShuffleConfig hasn't changed, keep it active (i.e., don't drain).
				if r.req.Config.Equal(&config) {
					delete(drain, spec.Name)
				}
				return
			}

			// A *read of this journal doesn't exist. Start one.
			var adjust = message.NewClock(time.Unix(int64(config.Shuffle.ReadDelaySeconds), 0)) -
				message.NewClock(time.Unix(0, 0))

			r = &read{
				spec: spec,
				req: pf.ShuffleRequest{
					Config:    config,
					RingIndex: rb.ringIndex,
					Offset:    offsets[spec.Name],
				},
				pollAdjust: adjust,
			}

			// Potentially increment the read offset by lower-bounding via the member clock.
			if list, err := rb.listFragments(buildFragmentBoundRequest(&r.req)); err == nil {
				applyFragmentBoundResponse(&r.req, list)
			} else {
				r.pollCh <- &pf.ShuffleResponse{
					TerminalError: fmt.Sprintf("failed to list fragments (for bounding read offset): %s", err),
				}
			}

			added[spec.Name] = r
		})

	return
}

func (rb *ReadBuilder) start(ctx context.Context, r *read) error {
	r.log().Info("starting shuffled journal read")
	r.ctx, r.cancel = context.WithCancel(ctx)

	// Resolve coordinator shard to a current member process.
	var resolution, err = rb.service.Resolver.Resolve(consumer.ResolveArgs{
		Context:  r.ctx,
		ShardID:  r.req.Config.CoordinatorShard(),
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
		"journal":     r.req.Config.Journal,
		"coordinator": r.req.Config.Coordinator,
		"transform":   r.req.Config.Shuffle.Transform,
		"offset":      r.req.Offset,
		"endOffset":   r.req.EndOffset,
		"ring":        r.req.Config.Ring.Name,
		"ringIndex":   r.req.RingIndex,
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

// buildFragmentBoundRequest returns a FragmentsRequest which identifies a
// lower-bound fragment for the request's minimum ring clock.
func buildFragmentBoundRequest(req *pf.ShuffleRequest) pb.FragmentsRequest {
	var beginModTime int64
	if c := req.Config.Ring.Members[req.RingIndex].MinMsgClock; c != 0 {
		// If we have a MinMsgClock, and the fragment was persisted _before_ it
		// (with a small adjustment to account for clock drift), it cannot possibly
		// contain messages published _after_ the minimum clock.
		beginModTime = c.Time().Add(-time.Minute).Unix()
	}

	return pb.FragmentsRequest{
		Journal:      req.Config.Journal,
		BeginModTime: beginModTime,
		// Return only the first fragment with a larger ModTime.
		PageLimit: 1,
	}
}

// applyFragmentBoundResponse constraints the request offset by the
// given lower-bound fragment.
func applyFragmentBoundResponse(req *pf.ShuffleRequest, list *pb.FragmentsResponse) {
	if l := len(list.Fragments); l != 0 && list.Fragments[0].Spec.Begin > req.Offset {
		log.WithFields(log.Fields{
			"journal":  req.Config.Journal,
			"offset":   req.Offset,
			"fragment": list.Fragments[0].String(),
		}).Info("skipping forward offset to that of matched fragment bound")

		req.Offset = list.Fragments[0].Spec.Begin
	}
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

func walkReads(coordinators int, journals []pb.ListResponse_Journal, transforms []pf.TransformSpec,
	cb func(_ pb.JournalSpec, _ pf.TransformSpec, coordinator int)) {

	for _, partition := range groupLogicalPartitions(journals) {
		for j, journal := range journals[partition.begin:partition.end] {
			for _, transform := range transforms {
				if !transform.Source.Partitions.Matches(journal.Spec.LabelSet) {
					continue
				}

				// Augment JournalSpec to capture derivation and transform on
				// whose behalf the read is being done.
				// TODO(johnny): THIS NEEDS TO BE A STABLE NAME!.
				var spec = journal.Spec
				spec.Name = pb.Journal(fmt.Sprintf("%s?derivation=%s&transform=%s",
					journal.Spec.Name.String(),
					url.QueryEscape(transform.Derivation.Name.String()),
					url.QueryEscape(transform.Shuffle.Transform.String()),
				))

				// Map each physical partition to its corresponding member shard.
				// TODO(johnny): This minimizes data shuffles if
				// len(physical partitions) ~= len(ring.Members), but can result in
				// uneven shuffle coordination otherwise. It's unclear right now whether
				// that's an issue.
				cb(spec, transform, j%coordinators)
			}
		}
	}
}

// groupLogicalPartitions groups ordered journals by their "directory"
// (the prefix of the journal name through its final '/').
func groupLogicalPartitions(journals []pb.ListResponse_Journal) (out []struct{ begin, end int }) {
	for i, j := 0, 1; i != len(journals); j++ {
		if j == len(journals) ||
			path.Dir(journals[i].Spec.Name.String()) !=
				path.Dir(journals[j].Spec.Name.String()) {

			// Range [i,j) are physical partitions of a shared logical partition.
			out = append(out, struct{ begin, end int }{i, j})
			i = j
		}
	}
	return
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
