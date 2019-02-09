package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	// Frequency with which current FSM hints are written to Etcd.
	storeHintsInterval = 5 * time.Minute
	// Size of the channel used between message decode & consumption. Needs to
	// be rather large, to minimize processing stalls. The current value will
	// tolerate a data delay of up to 82ms @ 100K messages / sec without stalling.
	messageBufferSize = 1 << 13 // 8192.
)

// Replica of a shard which is processed locally.
type Replica struct {
	// Context tied to processing lifetime of this shard replica by this
	// consumer. Cancelled when it is no longer responsible for the shard.
	ctx    context.Context
	cancel context.CancelFunc
	// Most recent transitioned ShardSpec and Assignment of the replica.
	spec       *ShardSpec
	assignment keyspace.KeyValue
	// consumer Application and local processing state.
	app          Application
	store        Store
	storeReadyCh chan struct{} // Closed when |store| is ready.
	player       *recoverylog.Player
	// Clients retained for Replica's use during processing.
	ks            *keyspace.KeySpace
	etcd          *clientv3.Client
	journalClient client.AsyncJournalClient
	// Synchronizes over goroutines referencing the Replica.
	wg sync.WaitGroup
}

// NewReplica returns a Replica in its initial state. The Replica must be
// transitioned by Resolver to begin processing of a shard.
func NewReplica(app Application, ks *keyspace.KeySpace, etcd *clientv3.Client, rjc pb.RoutedJournalClient) *Replica {
	var ctx, cancel = context.WithCancel(context.Background())

	var r = &Replica{
		ctx:           ctx,
		cancel:        cancel,
		app:           app,
		storeReadyCh:  make(chan struct{}),
		player:        recoverylog.NewPlayer(),
		ks:            ks,
		etcd:          etcd,
		journalClient: client.NewAppendService(context.Background(), rjc),
	}
	return r
}

// Context of the Replica.
func (r *Replica) Context() context.Context { return r.ctx }

// Spec of the Replica shard.
func (r *Replica) Spec() *ShardSpec {
	defer r.ks.Mu.RUnlock()
	r.ks.Mu.RLock()

	return r.spec
}

// Assignment of the ShardSpec to the local ConsumerSpec, which motivates this Replica.
func (r *Replica) Assignment() keyspace.KeyValue {
	defer r.ks.Mu.RUnlock()
	r.ks.Mu.RLock()

	return r.assignment
}

// JournalClient for broker operations performed in the course of processing this Replica.
func (r *Replica) JournalClient() client.AsyncJournalClient { return r.journalClient }

// transition is called by Resolver with the current ShardSpec and allocator
// Assignment of the replica, and transitions the Replica from its initial
// state to a standby or primary state. |spec| and |assignment| must always be
// non-zero-valued, and r.Mu.Lock must be held.
var transition = func(r *Replica, spec *ShardSpec, assignment keyspace.KeyValue) {
	var isSlot0 = assignment.Decoded.(allocator.Assignment).Slot == 0
	var wasSlot0 = r.spec != nil && r.assignment.Decoded.(allocator.Assignment).Slot == 0

	if r.spec == nil && !isSlot0 {
		r.wg.Add(1) // Transition initial => standby.
		go r.serveStandby()
	} else if r.spec == nil && isSlot0 {
		r.wg.Add(2) // Transition initial => primary.
		go r.serveStandby()
		go r.servePrimary()
	} else if r.spec != nil && isSlot0 && !wasSlot0 {
		r.wg.Add(1) // Transition standby => primary.
		go r.servePrimary()
	}
	r.spec, r.assignment = spec, assignment
}

// serveStandby recovers and tails the shard recovery log, until the Replica is
// cancelled or promoted to primary.
func (r *Replica) serveStandby() {
	defer r.wg.Done()

	go func() {
		tryUpdateStatus(r, r.ks, r.etcd, ReplicaStatus{Code: ReplicaStatus_BACKFILL})

		// When the player completes back-fill, advertise that we're tailing the log.
		select {
		case <-r.Context().Done():
			return
		case <-r.player.Tailing():
			tryUpdateStatus(r, r.ks, r.etcd, ReplicaStatus{Code: ReplicaStatus_TAILING})
		}
	}()

	if err := playLog(r, r.player, r.etcd); err != nil {
		err = r.logFailure(extendErr(err, "playLog"))
		tryUpdateStatus(r, r.ks, r.etcd, newErrorStatus(err))
	}
}

// servePrimary completes playback of the recovery log, pumps messages from
// shard journals, and runs consumer transactions.
func (r *Replica) servePrimary() {
	defer r.wg.Done()

	var store, offsets, err = completePlayback(r, r.app, r.player, r.etcd)
	if err != nil {
		err = r.logFailure(extendErr(err, "completePlayback"))
		tryUpdateStatus(r, r.ks, r.etcd, newErrorStatus(err))
		return
	}

	r.store = store
	close(r.storeReadyCh)
	tryUpdateStatus(r, r.ks, r.etcd, ReplicaStatus{Code: ReplicaStatus_PRIMARY})

	// Spawn service loops to read & decode messages.
	var msgCh = make(chan message.Envelope, messageBufferSize)

	for _, src := range r.Spec().Sources {
		r.wg.Add(1)
		go func(journal pb.Journal, offset int64) {
			if err := pumpMessages(r, r.app, journal, offset, msgCh); err != nil {
				err = r.logFailure(extendErr(err, "pumpMessages"))
				tryUpdateStatus(r, r.ks, r.etcd, newErrorStatus(err))
			}
			r.wg.Done()
		}(src.Journal, offsets[src.Journal])
	}

	// Consume messages from |msgCh| until an error occurs (such as context.Cancelled).
	var hintsTimer = time.NewTimer(storeHintsInterval)
	defer hintsTimer.Stop()

	if err = consumeMessages(r, r.store, r.app, r.etcd, msgCh, hintsTimer.C); err != nil {
		err = r.logFailure(extendErr(err, "consumeMessages"))
		tryUpdateStatus(r, r.ks, r.etcd, newErrorStatus(err))
	}
}

// waitAndTearDown waits for all outstanding goroutines which are accessing
// the Replica, and for all pending Appends to complete, and then tears down
// the store.
func (r *Replica) waitAndTearDown(done func()) {
	r.wg.Wait()
	client.WaitForPendingAppends(r.journalClient.PendingExcept(""))

	if r.store != nil {
		r.store.Destroy()
	}
	done()
}

func (r *Replica) logFailure(err error) error {
	if errors.Cause(err) == context.Canceled {
		return err
	}
	log.WithFields(log.Fields{
		"err":   err,
		"shard": r.Spec().Id,
	}).Error("shard processing failed")
	return err
}

// updateStatus publishes |status| under the Shard Assignment key in a checked
// transaction. An existing ReplicaStatus is reduced into |status| prior to update.
func updateStatus(shard Shard, ks *keyspace.KeySpace, etcd *clientv3.Client, status ReplicaStatus) error {
	var asn = shard.Assignment()
	status.Reduce(asn.Decoded.(allocator.Assignment).AssignmentValue.(*ReplicaStatus))

	var key = string(asn.Raw.Key)
	var val = status.MarshalString()

	var resp, err = etcd.Txn(shard.Context()).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", asn.Raw.ModRevision)).
		Then(clientv3.OpPut(key, val, clientv3.WithIgnoreLease())).
		Commit()

	if err == nil && !resp.Succeeded {
		err = errors.Errorf("transaction failed")
	}
	if err == nil {
		// Block until the update is observed in the KeySpace.
		ks.Mu.RLock()
		_ = ks.WaitForRevision(shard.Context(), resp.Header.Revision)
		ks.Mu.RUnlock()
	}
	return err
}

// tryUpdateStatus wraps updateStatus with retry behavior.
func tryUpdateStatus(shard Shard, ks *keyspace.KeySpace, etcd *clientv3.Client, status ReplicaStatus) {
	for attempt := 0; true; attempt++ {
		if shard.Context().Err() != nil {
			return // Already cancelled.
		}
		select {
		case <-shard.Context().Done():
			return // Cancelled while waiting to retry.
		case <-time.After(backoff(attempt)):
			// Pass.
		}

		var err = updateStatus(shard, ks, etcd, status)
		if err == nil {
			return
		}
		log.WithFields(log.Fields{"err": err, "attempt": attempt}).
			Error("failed to advertise Etcd shard status (will retry)")
	}
}

// newErrorStatus returns a FAILED ReplicaStatus which encodes the error.
func newErrorStatus(err error) ReplicaStatus {
	return ReplicaStatus{Code: ReplicaStatus_FAILED, Errors: []string{fmt.Sprintf("%v", err)}}
}

func backoff(attempt int) time.Duration {
	switch attempt {
	case 0, 1:
		return 0
	case 2:
		return time.Millisecond * 5
	case 3, 4, 5:
		return time.Second * time.Duration(attempt-1)
	default:
		return 5 * time.Second
	}
}
