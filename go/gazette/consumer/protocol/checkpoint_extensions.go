package protocol

import (
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// BuildCheckpointArgs are arguments of BuildCheckpoint.
type BuildCheckpointArgs struct {
	ReadThrough    pb.Offsets
	ProducerStates []message.ProducerState
	AckIntents     []message.AckIntent
}

// BuildCheckpoint builds a Checkpoint message instance from the arguments.
func BuildCheckpoint(args BuildCheckpointArgs) Checkpoint {
	var cp = Checkpoint{
		Sources:    make(map[pb.Journal]*Checkpoint_Source, len(args.ReadThrough)),
		AckIntents: make(map[pb.Journal][]byte, len(args.AckIntents)),
	}
	for j, o := range args.ReadThrough {
		cp.Sources[j] = &Checkpoint_Source{
			ReadThrough: o,
		}
	}
	for _, p := range args.ProducerStates {
		cp.Sources[p.Journal].Producers = append(cp.Sources[p.Journal].Producers,
			Checkpoint_Source_ProducerEntry{
				Id: append([]byte(nil), p.Producer[:]...),
				State: Checkpoint_ProducerState{
					LastAck: p.LastAck,
					Begin:   p.Begin,
				},
			})
	}
	for _, ack := range args.AckIntents {
		cp.AckIntents[ack.Journal] = ack.Intent
	}
	return cp
}

// FlattenProducerStates returns a []ProducerState drawn from the Checkpoint.
func FlattenProducerStates(cp Checkpoint) []message.ProducerState {
	var out []message.ProducerState

	for j, s := range cp.Sources {
		for _, p := range s.Producers {
			var producer message.ProducerID
			copy(producer[:], p.Id)

			out = append(out, message.ProducerState{
				JournalProducer: message.JournalProducer{
					Journal:  j,
					Producer: producer,
				},
				LastAck: p.State.LastAck,
				Begin:   p.State.Begin,
			})
		}
	}
	return out
}

// FlattenReadThrough returns Offsets drawn from the Checkpoint.
func FlattenReadThrough(cp Checkpoint) pb.Offsets {
	var out = make(pb.Offsets, len(cp.Sources))
	for j, s := range cp.Sources {
		out[j] = s.ReadThrough
	}
	return out
}
