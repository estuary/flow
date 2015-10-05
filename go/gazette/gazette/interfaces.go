package gazette

import (
	"github.com/pippio/gazette/journal"
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
	StartBrokeringWithPeers(routeToken string, peers []journal.Replicator)
	StartReplicating(routeToken string)
}

// Creates a new Replica instance.
type ReplicaFactory interface {
	NewReplica(journal.Name) JournalReplica
}
