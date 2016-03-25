package consumer

import (
	"path/filepath"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
)

type shardState string

const (
	shardStateInit      shardState = "init"
	shardStateReplica   shardState = "replica"
	shardStateMaster    shardState = "master"
	shardStateCancelled shardState = "cancelled"
)

// Models the state-machine of how a shard transitions from replica, to master,
// to cancelled. Delegates out the interesting bits to `replica` and `master`.
type shard struct {
	id       ShardID
	localDir string
	state    shardState

	replica *replica
	master  *master

	// cancelCh is plumbed through replica & master, and acts as a single
	// channel by which to signal cancellation of all shard processing.
	cancelCh chan struct{}
}

func newShard(id ShardID, runner *Runner) *shard {
	return &shard{
		id:       id,
		localDir: filepath.Join(runner.LocalDir, id.String()),
		state:    shardStateInit,
		cancelCh: make(chan struct{}),
	}
}

// Called from Allocate() goroutine. Cannot block.
func (s *shard) transitionReplica(runner *Runner, tree *etcd.Node) {
	switch s.state {
	case shardStateInit:
		s.state = shardStateReplica
		// Fall-through.
	case shardStateReplica:
		return // No-op.
	default:
		log.WithFields(log.Fields{"shard": s.id, "state": s.state}).
			Panic("invalid replica transaction")
	}

	var err error
	if s.replica, err = newReplica(s, runner, tree); err != nil {
		log.WithFields(log.Fields{"shard": s.id, "err": err}).Error("failed to init replica")
		go abort(runner, s.id)
		return
	}

	go s.replica.serve(runner)
}

// Called from Allocate() goroutine. Cannot block.
func (s *shard) transitionMaster(runner *Runner, tree *etcd.Node) {
	switch s.state {
	case shardStateInit:
		s.transitionReplica(runner, tree)
		s.transitionMaster(runner, tree)
		return
	case shardStateReplica:
		s.state = shardStateMaster
	case shardStateMaster:
		return // No-op.
	default:
		log.WithFields(log.Fields{"shard": s.id, "state": s.state}).
			Panic("invalid master transition")
	}

	var err error
	if s.master, err = newMaster(s, tree); err != nil {
		log.WithFields(log.Fields{"shard": s.id, "err": err}).Error("failed to init master")
		go abort(runner, s.id)
		return
	}

	go s.master.serve(runner, s.replica)
}

// Called from Allocate() goroutine. Cannot block.
func (s *shard) transitionCancel() {
	switch s.state {
	case shardStateInit, shardStateReplica, shardStateMaster:
		s.state = shardStateCancelled
	case shardStateCancelled:
		return // No-op.
	default:
		log.WithFields(log.Fields{"shard": s.id, "state": s.state}).
			Panic("invalid replica transaction")
	}

	close(s.cancelCh)
}

// Returns only after replica & master have completed.
func (s *shard) blockUntilHalted() {
	if s.replica != nil {
		<-s.replica.servingCh
	}
	if s.master != nil {
		<-s.master.servingCh
	}
}

// Returns whether replica & master have halted, without blocking.
func (s *shard) hasHalted() bool {
	if s.master != nil {
		select {
		case <-s.master.servingCh:
		default:
			return false
		}
	}
	if s.replica != nil {
		select {
		case <-s.replica.servingCh:
		default:
			return false
		}
	}
	return true
}
