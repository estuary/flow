package runtime

import (
	"context"
	"testing"
	"time"

	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// syncNowCaller is one in-flight syncNow: what it has sent, and what it
// finally returned.
type syncNowCaller struct {
	sent chan *pr.SyncNowResponse
	done chan error
}

// assertParked asserts the caller has neither sent anything further nor
// returned.
func (c syncNowCaller) assertParked(t *testing.T) {
	t.Helper()
	select {
	case resp := <-c.sent:
		t.Fatalf("caller advanced unexpectedly, sending %v", resp)
	case err := <-c.done:
		t.Fatalf("caller returned unexpectedly: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestSyncNowBarrier drives the barrier over a bare materializeAppV2: syncNow
// reads only `synced`, `closeSeq`, and `closeNowCh`, so no session, shard, or
// connector is needed to exercise it. `echo` stands in for the leader.
func TestSyncNowBarrier(t *testing.T) {
	var newApp = func(counts *pr.Synced) *materializeAppV2 {
		var app = &materializeAppV2{
			closeNowCh: make(chan struct{}, 1),
			synced:     &syncedCounts{changed: make(chan struct{})},
		}
		if counts != nil {
			app.synced.update(counts)
		}
		return app
	}
	var call = func(ctx context.Context, app *materializeAppV2) syncNowCaller {
		var c = syncNowCaller{
			sent: make(chan *pr.SyncNowResponse, 8),
			done: make(chan error, 1),
		}
		go func() {
			c.done <- app.syncNow(ctx, func(resp *pr.SyncNowResponse) error {
				c.sent <- resp
				return nil
			})
		}()
		return c
	}
	// echo plays the leader: consume the session loop's wake-up (blocking
	// until a close request is actually pending), then report `counts` back
	// stamped with the sequence the loop would have sent.
	var echo = func(app *materializeAppV2, counts *pr.Synced) {
		<-app.closeNowCh
		counts.CloseRequestSeq = app.closeSeq.Load()
		app.synced.update(counts)
	}

	t.Run("no-counts-yet-parks-until-the-echo", func(t *testing.T) {
		// Synced is demand-driven: a session which hasn't been asked has
		// reported nothing, and this caller's own request is what prompts
		// the first report. It parks on the echo rather than failing.
		var app = newApp(nil)
		var c = call(context.Background(), app)

		c.assertParked(t)

		echo(app, &pr.Synced{AcknowledgedCount: 5, PendingCount: 0})
		require.NotNil(t, (<-c.sent).GetAck())
		require.NotNil(t, (<-c.sent).GetDone())
		require.NoError(t, <-c.done)
	})

	t.Run("a-session-ending-while-un-synced-breaks-the-wait", func(t *testing.T) {
		// A caller parked before the leader's first report is still failed
		// by that session ending: its queued close request may never have
		// been sent.
		var app = newApp(nil)
		var c = call(context.Background(), app)

		c.assertParked(t)
		app.synced.update(nil)

		require.Equal(t, codes.Unavailable, status.Code(<-c.done))
		require.Empty(t, c.sent, "an unforced commit is never acknowledged")
	})

	t.Run("ack-waits-for-the-leader-to-take-the-request", func(t *testing.T) {
		// The Ack contract: the dashboard may hang up on it, so it must not
		// be sent until the leader has the close request in hand.
		var app = newApp(&pr.Synced{AcknowledgedCount: 5, PendingCount: 0})
		var c = call(context.Background(), app)

		c.assertParked(t)

		echo(app, &pr.Synced{AcknowledgedCount: 5, PendingCount: 0})
		require.NotNil(t, (<-c.sent).GetAck())
		require.NotNil(t, (<-c.sent).GetDone())
		require.NoError(t, <-c.done)
	})

	t.Run("the-echo-catches-a-transaction-opened-behind-our-counts", func(t *testing.T) {
		// Counts travel leader -> shard -> here, so the ones we start from
		// can be stale. The barrier is taken from the echo, which the leader
		// reported after reading our request -- so a transaction which opened
		// in that window is still awaited.
		var app = newApp(&pr.Synced{AcknowledgedCount: 5, PendingCount: 0})
		var c = call(context.Background(), app)

		echo(app, &pr.Synced{AcknowledgedCount: 5, PendingCount: 1})
		require.NotNil(t, (<-c.sent).GetAck())

		// Had we trusted the stale counts, the target would be 5 and we would
		// already be done.
		c.assertParked(t)

		app.synced.update(&pr.Synced{AcknowledgedCount: 6, CloseRequestSeq: 1})
		require.NotNil(t, (<-c.sent).GetDone())
		require.NoError(t, <-c.done)
	})

	t.Run("awaits-every-pending-transaction", func(t *testing.T) {
		var app = newApp(&pr.Synced{AcknowledgedCount: 5, PendingCount: 2})
		var c = call(context.Background(), app)

		echo(app, &pr.Synced{AcknowledgedCount: 5, PendingCount: 2})
		require.NotNil(t, (<-c.sent).GetAck())

		// Acknowledging only the first of the two doesn't resolve it.
		app.synced.update(&pr.Synced{AcknowledgedCount: 6, PendingCount: 1, CloseRequestSeq: 1})
		c.assertParked(t)

		app.synced.update(&pr.Synced{AcknowledgedCount: 7, CloseRequestSeq: 1})
		require.NotNil(t, (<-c.sent).GetDone())
		require.NoError(t, <-c.done)
	})

	t.Run("a-session-ending-before-the-ack-breaks-the-wait", func(t *testing.T) {
		var app = newApp(&pr.Synced{AcknowledgedCount: 5, PendingCount: 1})
		var c = call(context.Background(), app)

		// Park first: a caller which instead loads the incremented session
		// would await an echo this test never sends.
		c.assertParked(t)
		app.synced.update(nil)

		require.Equal(t, codes.Unavailable, status.Code(<-c.done))
		require.Empty(t, c.sent, "an unforced commit is never acknowledged")
	})

	t.Run("a-session-ending-after-the-ack-breaks-the-wait", func(t *testing.T) {
		var app = newApp(&pr.Synced{AcknowledgedCount: 5, PendingCount: 1})
		var c = call(context.Background(), app)

		echo(app, &pr.Synced{AcknowledgedCount: 5, PendingCount: 1})
		require.NotNil(t, (<-c.sent).GetAck())
		app.synced.update(nil)

		require.Equal(t, codes.Unavailable, status.Code(<-c.done))
	})

	t.Run("a-new-session-does-not-resolve-our-target", func(t *testing.T) {
		// A replacement session's counts restart from zero, so they must not
		// be read against a target recorded under the session which ended.
		var app = newApp(&pr.Synced{AcknowledgedCount: 0, PendingCount: 1})
		var c = call(context.Background(), app)

		echo(app, &pr.Synced{AcknowledgedCount: 0, PendingCount: 1})
		require.NotNil(t, (<-c.sent).GetAck())

		app.synced.update(nil)
		app.synced.update(&pr.Synced{AcknowledgedCount: 3, CloseRequestSeq: 9})

		require.Equal(t, codes.Unavailable, status.Code(<-c.done))
	})

	t.Run("a-caller-hanging-up-is-canceled", func(t *testing.T) {
		var app = newApp(&pr.Synced{AcknowledgedCount: 5, PendingCount: 1})
		var ctx, cancel = context.WithCancel(context.Background())
		var c = call(ctx, app)

		echo(app, &pr.Synced{AcknowledgedCount: 5, PendingCount: 1})
		require.NotNil(t, (<-c.sent).GetAck())
		cancel()

		require.Equal(t, codes.Canceled, status.Code(<-c.done))
	})

	t.Run("concurrent-callers-coalesce-onto-one-close", func(t *testing.T) {
		var app = newApp(&pr.Synced{AcknowledgedCount: 5, PendingCount: 1})
		var first, second = call(context.Background(), app), call(context.Background(), app)

		// Both have claimed a sequence, but the session loop is woken once.
		require.Eventually(t, func() bool { return app.closeSeq.Load() == 2 },
			time.Second, time.Millisecond)

		// One CloseNow bearing the highest sequence satisfies both.
		echo(app, &pr.Synced{AcknowledgedCount: 5, PendingCount: 1})
		require.NotNil(t, (<-first.sent).GetAck())
		require.NotNil(t, (<-second.sent).GetAck())
		require.Empty(t, app.closeNowCh, "the leader is asked exactly once")

		app.synced.update(&pr.Synced{AcknowledgedCount: 6, CloseRequestSeq: 2})
		require.NotNil(t, (<-first.sent).GetDone())
		require.NotNil(t, (<-second.sent).GetDone())
		require.NoError(t, <-first.done)
		require.NoError(t, <-second.done)
	})
}
