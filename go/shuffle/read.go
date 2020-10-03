package shuffle

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

// Converts a slice of TransformSpecs to a slice of the more generic ReadSpecs.
func ReadSpecsFromTransforms(transforms []pf.TransformSpec) []pf.ReadSpec {
	rs := make([]pf.ReadSpec, len(transforms))
	for i, t := range transforms {
		rs[i] = pf.ReadSpec{
			SourceName:        t.Source.Name.String(),
			SourcePartitions:  t.Source.Partitions,
			Shuffle:           t.Shuffle,
			ReaderType:        "transform",
			ReaderNames:       []string{t.Derivation.Name.String(), t.Name.String()},
			ReaderCatalogDbId: t.CatalogDbId,
		}
	}
	return rs
}

// ReadBuilder builds instances of shuffled reads.
type ReadBuilder struct {
	service    *consumer.Service
	journals   *keyspace.KeySpace
	ranges     pf.RangeSpec
	transforms []pf.ReadSpec

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
	journals *keyspace.KeySpace,
	shard consumer.Shard,
	transforms []pf.ReadSpec,
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
		service:    service,
		journals:   journals,
		ranges:     ranges,
		transforms: transforms,
		members:    members,
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

// ReadThrough filters the input |offsets| to those journals and offsets which are
// read by this ReadBuilder.
func (rb *ReadBuilder) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	var out = make(pb.Offsets, len(offsets))
	var err = walkReads(rb.members(), rb.journals, rb.transforms,
		func(spec pb.JournalSpec, transform pf.ReadSpec, coordinator pc.ShardID) {
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
	pollAdjust message.Clock
	pollCh     chan *pf.ShuffleResponse
}

func (rb *ReadBuilder) buildReplayRead(journal pb.Journal, begin, end pb.Offset) (*read, error) {
	var out *read
	var err = walkReads(rb.members(), rb.journals, rb.transforms,
		func(spec pb.JournalSpec, transform pf.ReadSpec, coordinator pc.ShardID) {
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
				resp:       pf.IndexedShuffleResponse{Transform: &transform},
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

	added = make(map[pb.Journal]*read)
	// Initialize |drain| with all active reads, so that any read we do /not/
	// see during our walk below is marked as needing to be drained.
	drain = make(map[pb.Journal]*read, len(existing))
	for j, r := range existing {
		drain[j] = r
	}

	err = walkReads(rb.members(), rb.journals, rb.transforms,
		func(spec pb.JournalSpec, transform pf.ReadSpec, coordinator pc.ShardID) {
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
				resp:       pf.IndexedShuffleResponse{Transform: &transform},
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

type shardsByKey []*pc.ShardSpec

func (s shardsByKey) len() int                 { return len(s) }
func (s shardsByKey) getKeyBegin(i int) []byte { return []byte(s[i].LabelSet.ValueOf(labels.KeyBegin)) }
func (s shardsByKey) getKeyEnd(i int) []byte   { return []byte(s[i].LabelSet.ValueOf(labels.KeyEnd)) }

func addQueryParameters(readSpec *pf.ReadSpec, journalName string) string {
	return fmt.Sprintf("%s;%s/%s", journalName, readSpec.ReaderType, strings.Join(readSpec.ReaderNames, "/"))
}

func walkReads(members []*pc.ShardSpec, allJournals *keyspace.KeySpace, transforms []pf.ReadSpec,
	cb func(_ pb.JournalSpec, _ pf.ReadSpec, coordinator pc.ShardID)) error {

	// Generate hashes for each of |members| derived from IDs.
	var memberHashes = make([]uint32, len(members))
	for m := range members {
		memberHashes[m] = hashString(members[m].Id.String())
	}

	allJournals.Mu.RLock()
	defer allJournals.Mu.RUnlock()

	for _, transform := range transforms {
		var sources = allJournals.Prefixed(allJournals.Root + "/" + transform.SourceName)

		for _, kv := range sources {
			var source = kv.Decoded.(*pb.JournalSpec)

			if !transform.SourcePartitions.Matches(source.LabelSet) {
				continue
			}

			var start, stop int
			if transform.Shuffle.UsesSourceKey {
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

			// Augment JournalSpec to capture the derivation and transform name on
			// whose behalf the read is being done, as a Journal metadata path segment.
			var copied = *source
			newName := addQueryParameters(&transform, source.Name.String())
			copied.Name = pb.Journal(newName)

			if start == stop {
				return fmt.Errorf("none of %d shards cover journal %s", len(members), copied.Name)
			}
			var m = pickHRW(hashString(copied.Name.String()), memberHashes, start, stop)
			cb(copied, transform, members[m].Id)
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
