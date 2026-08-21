package runtime

import (
	"context"
	"sync"
	"time"

	pr "github.com/estuary/flow/go/protocols/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The controller half of the sync-now commit barrier, which the reactor front
// door drives on behalf of a user (see task_control.go). The leader half is
// `CloseNow` and `Synced`, documented in runtime.proto.

// syncedCounts holds the Synced counts most recently reported by a task's
// leader, and wakes callers parked on them. Counts are of one leader session,
// so `session` increments with each, and a caller which recorded a target
// under a session which has ended must discard it.
type syncedCounts struct {
	mu      sync.Mutex
	session uint64
	counts  *pr.Synced    // Latest of `session`, or nil if none yet.
	changed chan struct{} // Closed and replaced on every update.
}

// load returns the current session and its counts, plus a channel which closes
// on the next update of either.
func (s *syncedCounts) load() (uint64, *pr.Synced, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.session, s.counts, s.changed
}

// reload reads fresh counts on behalf of a caller working within `session`,
// failing once they belong to a later one -- whose counts restart from zero
// and so say nothing about a target recorded under its predecessor.
func (s *syncedCounts) reload(session uint64) (*pr.Synced, <-chan struct{}, error) {
	var next, counts, changed = s.load()
	if next != session {
		return nil, nil, status.Error(codes.Unavailable,
			"the task's leader session ended before the awaited transaction was acknowledged; retry")
	}
	return counts, changed, nil
}

// update records `counts` of the current leader session, or -- for a nil
// `counts` -- ends that session, discarding its counts.
func (s *syncedCounts) update(counts *pr.Synced) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if counts == nil {
		s.session, s.counts = s.session+1, nil
	} else {
		s.counts = counts
	}
	close(s.changed)
	s.changed = make(chan struct{})
}

// syncNowHeartbeat is the cadence of heartbeats to a parked SyncNow caller.
// Coarse: beats exist only to hold an hour-long stream open through
// load-balancer idle timeouts.
const syncNowHeartbeat = 15 * time.Second

// syncNow forces an immediate commit of the task's open transaction and
// returns once that transaction is fully acknowledged -- committed and
// queryable in the endpoint -- driving the caller's response stream via
// `send`. It implements syncNower for the reactor front door.
//
// It is the CloseNow / Synced commit barrier documented in runtime.proto:
// send a close request, wait for the leader to echo its sequence back, and
// await `acknowledged_count` reaching the sum reported alongside that echo.
// Waiting for our own echo is what makes Ack mean what it promises -- the
// leader has the request and the commit is forced -- and what keeps a
// transaction which opened while our counts were in flight from being missed.
func (m *materializeAppV2) syncNow(ctx context.Context, send func(*pr.SyncNowResponse) error) error {
	var session, counts, changed = m.synced.load()

	// Claim a sequence and wake the session loop, which sends CloseNow
	// carrying the highest sequence claimed. A pending wake-up therefore
	// already carries ours, so there's nothing to do when one is queued.
	var seq = m.closeSeq.Add(1)
	select {
	case m.closeNowCh <- struct{}{}:
	default:
	}

	// `counts` is nil until the leader's first report -- which this very
	// request prompts, since Synced is demand-driven (see runtime.proto).
	// Nil can also mean no leader session at all yet: the queued wake-up
	// awaits one forming, and a session which ends mid-wait fails us with
	// a retriable status.
	for counts == nil || counts.CloseRequestSeq < seq {
		var err error
		select {
		case <-changed:
			if counts, changed, err = m.synced.reload(session); err != nil {
				return err
			}
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
	// These counts were reported after our request landed, so they cover
	// every transaction the leader held when it read it.
	var target = counts.AcknowledgedCount + uint64(counts.PendingCount)

	if err := send(&pr.SyncNowResponse{
		Response: &pr.SyncNowResponse_Ack_{Ack: &pr.SyncNowResponse_Ack{}},
	}); err != nil {
		return err
	}

	var beat = time.NewTicker(syncNowHeartbeat)
	defer beat.Stop()

	for counts.AcknowledgedCount < target {
		var err error
		select {
		case <-changed:
			if counts, changed, err = m.synced.reload(session); err != nil {
				return err
			}
		case <-beat.C:
			if err = send(&pr.SyncNowResponse{
				Response: &pr.SyncNowResponse_Heartbeat_{Heartbeat: &pr.SyncNowResponse_Heartbeat{}},
			}); err != nil {
				return err
			}
		case <-ctx.Done():
			// As a gRPC status, so that the claims deadline which bounds a
			// long wait reaches the caller as DeadlineExceeded (which it
			// resolves by retrying with a fresh token) rather than Unknown.
			return status.FromContextError(ctx.Err()).Err()
		}
	}
	return send(&pr.SyncNowResponse{
		Response: &pr.SyncNowResponse_Done_{Done: &pr.SyncNowResponse_Done{}},
	})
}
