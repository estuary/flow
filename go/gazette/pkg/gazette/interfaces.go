package gazette

import (
	"github.com/LiveRamp/gazette/pkg/journal"
)

type AppendOpHandler interface {
	Append(journal.AppendOp)
}

type ReadOpHandler interface {
	Read(journal.ReadOp)
}

type ReplicateOpHandler interface {
	Replicate(journal.ReplicateOp)
}

// See journal.Replica.
type JournalReplica interface {
	AppendOpHandler
	ReadOpHandler
	ReplicateOpHandler
	Shutdown()
	StartBrokeringWithPeers(journal.RouteToken, []journal.Replicator)
	StartReplicating(journal.RouteToken)
}
