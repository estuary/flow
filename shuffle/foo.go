package shuffle

import (
	"go.gazette.dev/core/allocator"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// Coordinator indexes ShardSpecs to provide a mapping from Messages
// to a responsible shard.
type Coordinator struct {
	state *allocator.State
}

// NewCoordinator constructs a new Coordinator.
func NewCoordinator(state *allocator.State) *Coordinator {
	var c = &Coordinator{
		state: state,
	}

	c.state.KS.Mu.Lock()
	c.state.KS.Observers = append(c.state.KS.Observers, c.updateIndex)
	c.state.KS.Mu.Unlock()

	return c
}

type journalShardCfg struct {
	pc.ShardSpec_Source
	notBefore message.Clock
	Shard     pc.ShardID
}

func (c *Coordinator) updateIndex() {

	// For each source journal:
	//  For each disjoint UUID clock space:
	//    Keep a set of shard specs.

	var next = make(map[pb.Journal][]*pc.ShardSpec, len(r.state.Items))

	for _, kv := range c.state.Items {
		var spec = kv.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)

		spec.LabelSet.ValueOf(kNotBeforeLabel)

		for _, src := range spec.Sources {
			next[src.Journal] = append(next[src.Journal], spec)
		}
	}

}

const kNotBeforeLabel = "notBeforeClock"
