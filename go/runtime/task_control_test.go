package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/consumertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Required by the gRPC loopback which consumertest.NewConsumer builds.
func init() { pb.RegisterGRPCDispatcher("local") }

const (
	tcTestTask    = "acmeCo/test/materialization"
	tcTestShardID = "materialize/acmeCo/test/materialization/0011223344556677/00000000-00000000"
	// tcTestScope is the `id:prefix` a Read-level /authorize/user/task
	// token carries: the task's shard template prefix.
	tcTestScope = "materialize/acmeCo/test/materialization/0011223344556677/"

	// A capture, which the front door answers as having nothing to await.
	tcTestCaptureTask    = "acmeCo/test/capture"
	tcTestCaptureShardID = "capture/acmeCo/test/capture/0011223344556677/00000000-00000000"
	tcTestCaptureScope   = "capture/acmeCo/test/capture/0011223344556677/"

	// A materialization whose Store isn't a syncNower: the V1 runtime.
	tcTestV1Task    = "acmeCo/test/materialization-v1"
	tcTestV1ShardID = "materialize/acmeCo/test/materialization-v1/0011223344556677/00000000-00000000"
	tcTestV1Scope   = "materialize/acmeCo/test/materialization-v1/0011223344556677/"
)

func TestTaskControlSyncNow(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var stub = &stubSyncNower{}

	// Two reactor members, so that a front door which is *not* shard zero's
	// primary is exercised: it must proxy to the one which is.
	var members [2]*consumertest.Consumer
	for i := range members {
		var cmr = consumertest.NewConsumer(consumertest.Args{
			C:        t,
			Etcd:     etcd,
			Journals: nil, // Never read: the stub App produces no messages.
			App:      taskControlTestApp{stub: stub},
			Suffix:   fmt.Sprintf("member-%d", i),
		})
		var relay = &taskControl{service: cmr.Service}
		pr.RegisterTaskControlServer(cmr.Server.GRPCServer,
			pr.NewVerifiedTaskControlServer(relay, cmr.Service.Verifier))
		cmr.Server.HTTPMux.Handle(TaskControlSyncNowPath,
			&taskControlHTTP{relay: relay, verifier: cmr.Service.Verifier})
		cmr.Tasks.GoRun()
		members[i] = cmr
	}
	defer func() {
		for _, cmr := range members {
			cmr.Tasks.Cancel()
			require.NoError(t, cmr.Tasks.Wait())
		}
	}()

	consumertest.CreateShards(t, members[0],
		&pc.ShardSpec{
			Id:             tcTestShardID,
			MaxTxnDuration: time.Minute,
			LabelSet: pb.MustLabelSet(
				labels.TaskName, tcTestTask,
				labels.TaskType, ops.TaskType_materialization.String()),
		},
		&pc.ShardSpec{
			Id:             tcTestCaptureShardID,
			MaxTxnDuration: time.Minute,
			LabelSet: pb.MustLabelSet(
				labels.TaskName, tcTestCaptureTask,
				labels.TaskType, ops.TaskType_capture.String()),
		},
		&pc.ShardSpec{
			Id:             tcTestV1ShardID,
			MaxTxnDuration: time.Minute,
			LabelSet: pb.MustLabelSet(
				labels.TaskName, tcTestV1Task,
				labels.TaskType, ops.TaskType_materialization.String()),
		},
	)

	// Determine the shard's primary member, and thereby which front door is
	// co-located with the sidecar and which must reach across.
	var route pb.Route
	require.NoError(t, members[0].WaitForPrimary(ctx, tcTestShardID, &route))
	require.NoError(t, members[0].WaitForPrimary(ctx, tcTestCaptureShardID, nil))
	require.NoError(t, members[0].WaitForPrimary(ctx, tcTestV1ShardID, nil))
	var primary, remote = members[0], members[1]
	if route.Members[route.Primary].Suffix == "member-1" {
		primary, remote = members[1], members[0]
	}

	var token = mintTaskToken(t, primary, pb.MustLabelSet("id:prefix", tcTestScope))

	t.Run("flushes-per-message", func(t *testing.T) {
		// Gate the stub between the heartbeat and Done, proving each NDJSON
		// line is flushed and readable before the stream completes.
		var gate = make(chan struct{})
		stub.script(scriptedOkStream(gate))

		var resp = postSyncNow(t, primary, token, `{"taskName": "`+tcTestTask+`"}`)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

		var lines = bufio.NewScanner(resp.Body)
		require.Contains(t, readResultLine(t, lines), "ack")
		require.Contains(t, readResultLine(t, lines), "heartbeat")

		close(gate) // Only now may the stub complete the stream.
		require.Contains(t, readResultLine(t, lines), "done")
		require.False(t, lines.Scan(), "expected EOF after done")

		require.Equal(t, 1, stub.calls())
	})

	t.Run("proxies-from-a-non-primary-front-door", func(t *testing.T) {
		stub.script(scriptedOkStream(nil))
		var resp = postSyncNow(t, remote, token, `{"taskName": "`+tcTestTask+`"}`)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var lines = bufio.NewScanner(resp.Body)
		require.Contains(t, readResultLine(t, lines), "ack")
		require.Contains(t, readResultLine(t, lines), "heartbeat")
		require.Contains(t, readResultLine(t, lines), "done")

		// Reaching the primary's Store at all proves the token was forwarded
		// on the proxy hop: the peer front door verifies it independently.
		require.Equal(t, 1, stub.calls())
	})

	t.Run("not-found-passthrough", func(t *testing.T) {
		stub.script(func(func(*pr.SyncNowResponse) error) error {
			return status.Error(codes.NotFound, "no live leader session")
		})
		var resp = postSyncNow(t, primary, token, `{"taskName": "`+tcTestTask+`"}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
		var errBody = readErrorLine(t, resp)
		require.Equal(t, float64(codes.NotFound), errBody["grpcCode"])
		require.Equal(t, float64(http.StatusNotFound), errBody["httpCode"])
		require.Contains(t, errBody["message"], "no live leader session")
	})

	t.Run("rejects-missing-token", func(t *testing.T) {
		var resp = postSyncNow(t, primary, "", `{"taskName": "`+tcTestTask+`"}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		require.Equal(t, float64(codes.Unauthenticated), readErrorLine(t, resp)["grpcCode"])
	})

	t.Run("rejects-missing-token-before-reading-the-body", func(t *testing.T) {
		// An unauthenticated caller must not get us to decode its body: the
		// endpoint is publicly reachable and the body is otherwise unbounded.
		// A body which would itself be rejected proves the ordering, since it
		// answers Unauthenticated rather than InvalidArgument.
		var resp = postSyncNow(t, primary, "", `{"taskName": 42}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		require.Equal(t, float64(codes.Unauthenticated), readErrorLine(t, resp)["grpcCode"])
	})

	t.Run("rejects-an-oversized-body", func(t *testing.T) {
		var resp = postSyncNow(t, primary, token,
			`{"taskName": "`+strings.Repeat("x", maxSyncNowRequestBytes)+`"}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("rejects-mis-scoped-token", func(t *testing.T) {
		var misScoped = mintTaskToken(t, primary,
			pb.MustLabelSet("id:prefix", "materialize/acmeCo/other/0011223344556677/"))
		var resp = postSyncNow(t, primary, misScoped, `{"taskName": "`+tcTestTask+`"}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("rejects-bad-request", func(t *testing.T) {
		var resp = postSyncNow(t, primary, token, `{"taskName": 42}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("unknown-task", func(t *testing.T) {
		stub.script(scriptedOkStream(nil))
		var resp = postSyncNow(t, primary, token, `{"taskName": "acmeCo/test/not-a-task"}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)

		// No Store is consulted for a task we can't resolve.
		require.Zero(t, stub.calls())
	})

	t.Run("capture-has-nothing-to-await", func(t *testing.T) {
		stub.script(scriptedOkStream(nil))
		var captureToken = mintTaskToken(t, primary,
			pb.MustLabelSet("id:prefix", tcTestCaptureScope))
		var resp = postSyncNow(t, primary, captureToken, `{"taskName": "`+tcTestCaptureTask+`"}`)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Captures hold no open transaction: the front door answers from the
		// task's shard type, without consulting the shard's Store.
		var lines = bufio.NewScanner(resp.Body)
		require.Contains(t, readResultLine(t, lines), "ack")
		require.Contains(t, readResultLine(t, lines), "done")
		require.False(t, lines.Scan(), "expected EOF after done")

		require.Zero(t, stub.calls())
	})

	t.Run("capture-rejects-mis-scoped-token", func(t *testing.T) {
		// The capture's "nothing to await" answer comes only after the
		// caller's claims are verified to cover its shard.
		var resp = postSyncNow(t, primary, token, `{"taskName": "`+tcTestCaptureTask+`"}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("v1-materialization-is-not-found", func(t *testing.T) {
		stub.script(scriptedOkStream(nil))
		var v1Token = mintTaskToken(t, primary, pb.MustLabelSet("id:prefix", tcTestV1Scope))
		var resp = postSyncNow(t, primary, v1Token, `{"taskName": "`+tcTestV1Task+`"}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
		require.Contains(t, readErrorLine(t, resp)["message"], "not running on the V2 runtime")
		require.Zero(t, stub.calls())
	})
}

// scriptedOkStream returns a stub script sending the canonical happy-path
// sequence: Ack, Heartbeat, then Done. If `gate` is non-nil, Done is withheld
// until it closes.
func scriptedOkStream(gate <-chan struct{}) func(func(*pr.SyncNowResponse) error) error {
	return func(send func(*pr.SyncNowResponse) error) error {
		if err := send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Ack_{Ack: &pr.SyncNowResponse_Ack{}},
		}); err != nil {
			return err
		}
		if err := send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Heartbeat_{Heartbeat: &pr.SyncNowResponse_Heartbeat{}},
		}); err != nil {
			return err
		}
		if gate != nil {
			<-gate
		}
		return send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Done_{Done: &pr.SyncNowResponse_Done{}},
		})
	}
}

func postSyncNow(t *testing.T, cmr *consumertest.Consumer, token, body string) *http.Response {
	var url = cmr.Server.Endpoint().URL().String() + TaskControlSyncNowPath
	var req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// readResultLine scans one NDJSON line and returns its unwrapped "result".
func readResultLine(t *testing.T, lines *bufio.Scanner) map[string]interface{} {
	require.True(t, lines.Scan(), "expected another NDJSON line")
	var parsed map[string]map[string]interface{}
	require.NoError(t, json.Unmarshal(lines.Bytes(), &parsed))
	require.Contains(t, parsed, "result")
	return parsed["result"]
}

// readErrorLine reads the response body as a single NDJSON error line and
// returns its unwrapped "error".
func readErrorLine(t *testing.T, resp *http.Response) map[string]interface{} {
	var parsed map[string]map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&parsed))
	require.Contains(t, parsed, "error")
	return parsed["error"]
}

// mintTaskToken signs READ-capability Claims as the control plane's
// /authorize/user/task would, and returns the raw bearer token.
func mintTaskToken(t *testing.T, cmr *consumertest.Consumer, sel pb.LabelSet) string {
	var ctx, err = cmr.Service.Authorizer.Authorize(context.Background(), pb.Claims{
		Capability: pb.Capability_READ,
		Selector:   pb.LabelSelector{Include: sel},
	}, time.Hour)
	require.NoError(t, err)

	var md, _ = metadata.FromOutgoingContext(ctx)
	return strings.TrimPrefix(md.Get("authorization")[0], "Bearer ")
}

// stubSyncNower scripts the syncNow of a task shard's local Store, standing in
// for materializeAppV2 and counting the calls the relay routes to it.
type stubSyncNower struct {
	mu      sync.Mutex
	serveFn func(func(*pr.SyncNowResponse) error) error
	count   int
}

// script installs the syncNow behavior for the next test case.
func (s *stubSyncNower) script(fn func(func(*pr.SyncNowResponse) error) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.serveFn, s.count = fn, 0
}

// calls returns how many times the relay has reached us since `script`.
func (s *stubSyncNower) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *stubSyncNower) syncNow(_ context.Context, send func(*pr.SyncNowResponse) error) error {
	s.mu.Lock()
	var fn = s.serveFn
	s.count++
	s.mu.Unlock()

	return fn(send)
}

// taskControlTestApp is a minimal consumer.Application whose shards become
// resolvable primaries without a broker, recovery log, or messages: as a
// MessageProducer it simply never produces, so shards idle. Its Stores stand
// in for the V2 and V1 runtime applications — only the former is a syncNower.
type taskControlTestApp struct{ stub *stubSyncNower }

// taskControlTestStore stands in for a V1-runtime application: a Store which
// cannot force or await a commit.
type taskControlTestStore struct{}

// taskControlSyncStore stands in for materializeAppV2, delegating syncNow to
// the test's script.
type taskControlSyncStore struct {
	taskControlTestStore
	*stubSyncNower
}

var _ syncNower = &taskControlSyncStore{}

func (a taskControlTestApp) NewStore(shard consumer.Shard, _ *recoverylog.Recorder) (consumer.Store, error) {
	if shard.Spec().Id == tcTestV1ShardID {
		return taskControlTestStore{}, nil
	}
	return &taskControlSyncStore{stubSyncNower: a.stub}, nil
}
func (taskControlTestApp) NewMessage(*pb.JournalSpec) (message.Message, error) {
	panic("not called")
}
func (taskControlTestApp) ConsumeMessage(consumer.Shard, consumer.Store, message.Envelope, *message.Publisher) error {
	panic("not called")
}
func (taskControlTestApp) FinalizeTxn(consumer.Shard, consumer.Store, *message.Publisher) error {
	panic("not called")
}
func (taskControlTestApp) StartReadingMessages(shard consumer.Shard, _ consumer.Store, _ pc.Checkpoint, intoCh chan<- consumer.EnvelopeOrError) {
	// Produce no messages, but deliver the shard's cancellation: the
	// transaction loop of a MessageProducer application tears down only
	// upon reading an error from this channel.
	go func() {
		<-shard.Context().Done()
		intoCh <- consumer.EnvelopeOrError{Error: shard.Context().Err()}
	}()
}
func (taskControlTestApp) ReplayRange(_ consumer.Shard, _ consumer.Store, _ pb.Journal, _, _ pb.Offset) message.Iterator {
	panic("not called")
}
func (taskControlTestApp) ReadThrough(consumer.Shard, consumer.Store, consumer.ResolveArgs) (pb.Offsets, error) {
	return nil, nil
}

func (taskControlTestStore) StartCommit(consumer.Shard, pc.Checkpoint, consumer.OpFutures) consumer.OpFuture {
	return client.FinishedOperation(nil)
}
func (taskControlTestStore) RestoreCheckpoint(consumer.Shard) (pc.Checkpoint, error) {
	return pc.Checkpoint{}, nil
}
func (taskControlTestStore) Destroy() {}
