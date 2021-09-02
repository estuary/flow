package flow

import (
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

// Re-export common types of the Gazette broker and consumer protocols,
// so that this package can be used as a drop-in for the 80% of cases where
// only these types are needed, without requiring a new dependency on Gazette.

type Endpoint = pb.Endpoint
type Journal = pb.Journal
type JournalSpec = pb.JournalSpec
type Label = pb.Label
type LabelSelector = pb.LabelSelector
type LabelSet = pb.LabelSet

type ShardSpec = pc.ShardSpec
type ShardID = pc.ShardID
