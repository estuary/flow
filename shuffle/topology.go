package shuffle

/*
import (
	"path"
	"time"

	"github.com/pkg/errors"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	pc "go.gazette.dev/core/consumer/protocol"
)

type shuffle struct {
	topology Topology
}

func buildTopology(state *allocator.State) (Topology, error) {
	var out = Topology{
		Etcd: pbx.FromEtcdResponseHeader(state.KS.Header),
	}

	for _, kv := range state.Items {
		var spec = kv.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)

		var minClock, maxClock, err = shardClockBounds(spec)
		if err != nil {
			return Topology{}, errors.WithMessagef(err, "shard %s", spec.Id)
		}

		// Extract Route with attached member endpoints.
		var assignments = state.Assignments.Prefixed(
			allocator.ItemAssignmentsPrefix(state.KS, spec.Id.String()))

		var route pb.Route
		pbx.Init(&route, assignments)
		pbx.AttachEndpoints(&route, state.KS)

		var status pc.ReplicaStatus
		for _, asn := range assignments {
			status.Reduce(asn.Decoded.(allocator.Assignment).AssignmentValue.(*pc.ReplicaStatus))
		}

		out.Indicies = append(out.Indicies, Topology_Index{
			Shard:       spec.Id,
			Route:       route,
			Status:      status.Code,
			MinClockSec: minClock,
			MaxClockSec: maxClock,
		})
	}
	return out, nil
}

func shardClockBounds(spec *pc.ShardSpec) (int64, int64, error) {
	var clocks [2]int64

	for i, label := range []string{"minClock", "maxClock"} {
		for _, v := range spec.LabelSet.ValuesOf(label) {
			if t, err := time.Parse(time.RFC3339, v); err != nil {
				return 0, 0, errors.WithMessagef(err, "parsing %s label %q", label, v)
			} else {
				clocks[i] = t.Unix()
			}
		}
	}
	return clocks[0], clocks[1], nil
}

/*
type reader struct {
	shard    pc.ShardID
	journal  pb.Journal
	minClock message.Clock
}

func NewTopology(groups [][]pb.ListResponse_Journal, state *allocator.State) Topology {
	var out map[pb.Journal][]reader

	for _, kv := range state.Items {
		var spec = kv.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)

		// TODO(johnny) - extract minOffset from ShardSpec label.
		var minClock message.Clock // = spec.LabelSet.ValueOf(kNotBeforeLabel) ??

		for _, group := range groups {
			for _, journal := range group {

			}
		}
	}

	// Given a journal message, which shard should receive it? (Is it me?)
	// Given a journal, which shard should be reading it?
	//   A: align with the journal under the shuffle key? Do we need partitions?

	return out
}

*/
/*
	// Scenario:
	shard-000
	shard-001
	shard-002

	/foo/lp=A/part=000 => s000 (perfect hash)
	/foo/lp=A/part=001 => s001
	/foo/lp=A/part=002 => s002

	/foo/lp=B/part=000 => s000 (1/3 of time, it's for s002)
	/foo/lp=B/part=001 => s001 (1/3 of time, tt's for s002)

	// Scenario:
	shard-000
	shard-001

	/foo/lp=A/part=000 => s000 (perfect hash)
	/foo/lp=A/part=001 => s001 (perfect hash)
	/foo/lp=A/part=002 => s000 (1/2 of time, it's for s001)

	/foo/lp=B/part=000 => s000 (perfect hash)
	/foo/lp=B/part=001 => s001 (perfect hash)

	// Strategy:
	// Poll all journals.
	//  - Group on logical partition keys.
	//  - Order on 'part'.
	//  - *Keys are rendezvous-hashed to partitions on index order.*
	// Group all shards.
	//  - Extract minClock label from each shard.
	//  - Require they're monotonically increasing with shard ID. <= do we need this?
	//      - generalize as a predicate over shards
	//  - Order on shard ID.
	//  - *Keys are rendezvous-hashed to shards on index order.*
	//
	// Given a journal message, which shard receives it?
	//  - Map message to shuffle key
	//  - Hash shuffle key & map to shard index
	//  - Identify index of highest hash *which has a acceptable minClock*.
	//
	// Given a journal, which shard should be reading it?
	//  - Identify index of journal within logical partition group.
	//  - Map index to corresponding ordered shard ID, using modulo arithmetic.
	//
	// What's my read offset for a new journal?
	//  - Perform a fragment listing of the journal.
	//  - Take start offset of last fragment having a created timestamp less than minClock.
	//
	// A shard is asked to read on behalf of another shard (client) at an offset
	//  - If there's an ongoing read, tell the client of our current offset, and stream new messages.
	//  - Otherwise, start from the client-requested offset.
	//
	// I ask another shard to read for me, but it returns a future offset
	//  - Locally issue a catch-up read between my desired offset and the future offset.
	//  - On completion, continue streaming messages from the sender.
	//  - *Sender will naturally back-pressure as this catch-up read is completed*.
	//
	// I have an ongoing read, but the coordinator assignment is different (eg, a shard was added)
	//  - Cancel my current reader, and restart at the new coordinator.
	//
	// I discover a new journal I should be reading (same as startup case)
	//  - List fragments to find a min offset. Take max with an offset in my checkpoint.
	//  - Map to coordinator.
	//
* /

// GroupCommonDirs groups journal of a listing by their "directory"
// (the prefix of the journal name through its final '/').
func GroupCommonDirs(listing *pb.ListResponse) [][]pb.ListResponse_Journal {
	if len(listing.Journals) == 0 {
		return nil
	}

	var journals = listing.Journals
	var groups = make([][]pb.ListResponse_Journal, 0, len(journals))

	for i, j := 0, 1; i != len(journals); j++ {
		if j == len(journals) ||
			path.Dir(journals[i].Spec.Name.String()) !=
				path.Dir(journals[j].Spec.Name.String()) {
			// |i| and |j| are different logical partitions.
			groups = append(groups, journals[i:j])
			i = j
		}
	}
	return groups
}

*/
