package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/estuary/flow/go/labels"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Required by the dispatch balancer of consumertest Consumer loopbacks,
// which the relay uses to proxy to a peer's front door.
func init() { pb.RegisterGRPCDispatcher("local") }

const (
	tcTestTask    = "acmeCo/test/materialization"
	tcTestShardID = "materialize/acmeCo/test/materialization/0011223344556677/00000000-00000000"
	// tcTestScope is the `id:prefix` a Read-level /authorize/user/task
	// token carries: the task's shard template prefix.
	tcTestScope = "materialize/acmeCo/test/materialization/0011223344556677/"

	// A capture, which the front door resolves as NOT_APPLICABLE.
	tcTestCaptureTask    = "acmeCo/test/capture"
	tcTestCaptureShardID = "capture/acmeCo/test/capture/0011223344556677/00000000-00000000"
	tcTestCaptureScope   = "capture/acmeCo/test/capture/0011223344556677/"
)

func TestTaskControlSyncNow(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var stub = newStubSidecar(t)
	defer stub.server.GracefulStop()

	// Two reactor members, so the relay's peer-proxy path is exercised
	// end-to-end: the shard is assigned to one, and requests to the other's
	// front door must forward.
	var members [2]*consumertest.Consumer
	for i := range members {
		var cmr = consumertest.NewConsumer(consumertest.Args{
			C:        t,
			Etcd:     etcd,
			Journals: nil, // Never read: the stub App produces no messages.
			App:      taskControlTestApp{},
			Suffix:   fmt.Sprintf("member-%d", i),
		})
		var relay = &taskControl{
			service:         cmr.Service,
			sidecarEndpoint: func(pb.Endpoint) (string, error) { return stub.endpoint, nil },
		}
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
			LabelSet:       pb.MustLabelSet(labels.TaskName, tcTestTask),
		},
		&pc.ShardSpec{
			Id:             tcTestCaptureShardID,
			MaxTxnDuration: time.Minute,
			LabelSet:       pb.MustLabelSet(labels.TaskName, tcTestCaptureTask),
		},
	)

	// Determine the shard's primary member, and thereby which front door is
	// local to it and which must proxy.
	var route pb.Route
	require.NoError(t, members[0].WaitForPrimary(ctx, tcTestShardID, &route))
	require.NoError(t, members[0].WaitForPrimary(ctx, tcTestCaptureShardID, nil))
	var primary, proxied = members[0], members[1]
	if route.Members[route.Primary].Suffix == "member-1" {
		primary, proxied = members[1], members[0]
	}

	var token = mintTaskToken(t, primary, pb.MustLabelSet("id:prefix", tcTestScope))

	t.Run("grpc-relay", func(t *testing.T) {
		stub.script(scriptedOkStream(nil))
		var stream = openSyncNow(t, ctx, primary, token, tcTestTask)

		requireStreamedOk(t, stream)

		var taskName, auth = stub.observed()
		require.Equal(t, tcTestTask, taskName)
		require.Equal(t, "Bearer "+token, auth)
	})

	t.Run("grpc-relay-via-peer", func(t *testing.T) {
		stub.script(scriptedOkStream(nil))
		var stream = openSyncNow(t, ctx, proxied, token, tcTestTask)

		requireStreamedOk(t, stream)

		// The caller's token is forwarded verbatim across both hops.
		var taskName, auth = stub.observed()
		require.Equal(t, tcTestTask, taskName)
		require.Equal(t, "Bearer "+token, auth)
	})

	t.Run("grpc-not-found-passthrough", func(t *testing.T) {
		stub.script(func(*pr.SyncNowRequest, pr.TaskControl_SyncNowServer) error {
			return status.Error(codes.NotFound, "no live leader session")
		})
		var stream = openSyncNow(t, ctx, primary, token, tcTestTask)

		var _, err = stream.Recv()
		require.Equal(t, codes.NotFound, status.Code(err))
		require.ErrorContains(t, err, "no live leader session")
	})

	t.Run("grpc-rejects-missing-token", func(t *testing.T) {
		var stream, err = pr.NewTaskControlClient(primary.Service.Loopback).
			SyncNow(pb.WithDispatchDefault(ctx), &pr.SyncNowRequest{TaskName: tcTestTask})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("grpc-rejects-mis-scoped-token", func(t *testing.T) {
		var misScoped = mintTaskToken(t, primary,
			pb.MustLabelSet("id:prefix", "materialize/acmeCo/other/0011223344556677/"))
		var stream = openSyncNow(t, ctx, primary, misScoped, tcTestTask)

		var _, err = stream.Recv()
		require.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("grpc-unknown-task", func(t *testing.T) {
		stub.script(scriptedOkStream(nil))
		var stream = openSyncNow(t, ctx, primary, token, "acmeCo/test/not-a-task")
		var _, err = stream.Recv()
		require.Equal(t, codes.NotFound, status.Code(err))

		// The sidecar is never consulted for a task we can't resolve.
		var taskName, _ = stub.observed()
		require.Empty(t, taskName)
	})

	t.Run("grpc-capture-is-not-applicable", func(t *testing.T) {
		stub.script(scriptedOkStream(nil))
		var captureToken = mintTaskToken(t, primary,
			pb.MustLabelSet("id:prefix", tcTestCaptureScope))
		var stream = openSyncNow(t, ctx, primary, captureToken, tcTestCaptureTask)

		// Captures hold no open transaction: the front door resolves the
		// outcome from the task's shard type, without dialing the sidecar.
		var resp, err = stream.Recv()
		require.NoError(t, err)
		require.Equal(t, pr.SyncNowResponse_NOT_APPLICABLE, resp.GetAck().Outcome)
		require.Equal(t, &pr.SyncNowResponse_Status{}, resp.GetAck().Status)

		resp, err = stream.Recv()
		require.NoError(t, err)
		require.Equal(t, &pr.SyncNowResponse_Done{}, resp.GetDone())

		_, err = stream.Recv()
		require.ErrorContains(t, err, "EOF")

		var taskName, _ = stub.observed()
		require.Empty(t, taskName)
	})

	t.Run("grpc-capture-rejects-mis-scoped-token", func(t *testing.T) {
		// NOT_APPLICABLE is resolved only after the caller's claims are
		// verified to cover the capture's shard.
		var stream = openSyncNow(t, ctx, primary, token, tcTestCaptureTask)
		var _, err = stream.Recv()
		require.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("http-relay-flushes-per-message", func(t *testing.T) {
		// Gate the stub between Progress and Done, proving each NDJSON line
		// is flushed and readable before the stream completes.
		var gate = make(chan struct{})
		stub.script(scriptedOkStream(gate))

		var resp = postSyncNow(t, primary, token, `{"taskName": "`+tcTestTask+`"}`)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

		var lines = bufio.NewScanner(resp.Body)
		require.Equal(t, "HELD_COLLAPSED", readResultLine(t, lines)["ack"].(map[string]interface{})["outcome"])
		require.Contains(t, readResultLine(t, lines), "progress")

		close(gate) // Only now may the stub complete the stream.
		require.Contains(t, readResultLine(t, lines), "done")
		require.False(t, lines.Scan(), "expected EOF after done")

		var taskName, auth = stub.observed()
		require.Equal(t, tcTestTask, taskName)
		require.Equal(t, "Bearer "+token, auth)
	})

	t.Run("http-not-found-passthrough", func(t *testing.T) {
		stub.script(func(*pr.SyncNowRequest, pr.TaskControl_SyncNowServer) error {
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

	t.Run("http-rejects-missing-token", func(t *testing.T) {
		var resp = postSyncNow(t, primary, "", `{"taskName": "`+tcTestTask+`"}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		require.Equal(t, float64(codes.Unauthenticated), readErrorLine(t, resp)["grpcCode"])
	})

	t.Run("http-rejects-mis-scoped-token", func(t *testing.T) {
		var misScoped = mintTaskToken(t, primary,
			pb.MustLabelSet("id:prefix", "materialize/acmeCo/other/0011223344556677/"))
		var resp = postSyncNow(t, primary, misScoped, `{"taskName": "`+tcTestTask+`"}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("http-rejects-bad-request", func(t *testing.T) {
		var resp = postSyncNow(t, primary, token, `{"taskName": 42}`)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

// scriptedOkStream returns a stub script sending the canonical happy-path
// sequence: Ack, Progress, then Done. If `gate` is non-nil, Done is withheld
// until it closes.
func scriptedOkStream(gate <-chan struct{}) func(*pr.SyncNowRequest, pr.TaskControl_SyncNowServer) error {
	return func(_ *pr.SyncNowRequest, stream pr.TaskControl_SyncNowServer) error {
		if err := stream.Send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Ack_{Ack: &pr.SyncNowResponse_Ack{
				Outcome: pr.SyncNowResponse_HELD_COLLAPSED,
				Status:  &pr.SyncNowResponse_Status{HeadPhase: "Extend", TailPhase: "Done"},
			}},
		}); err != nil {
			return err
		}
		if err := stream.Send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Progress{Progress: &pr.SyncNowResponse_Status{
				SourcedDocsTotal: 10, SourcedBytesTotal: 100, HeadPhase: "StartCommit", TailPhase: "Acknowledge",
			}},
		}); err != nil {
			return err
		}
		if gate != nil {
			<-gate
		}
		return stream.Send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Done_{Done: &pr.SyncNowResponse_Done{
				CommittedDocsTotal: 10, CommittedBytesTotal: 100, OpenAgeMillis: 5000,
			}},
		})
	}
}

// requireStreamedOk asserts the stream yields Ack, Progress, and Done in
// order, followed by a clean EOF.
func requireStreamedOk(t *testing.T, stream pr.TaskControl_SyncNowClient) {
	var resp, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, pr.SyncNowResponse_HELD_COLLAPSED, resp.GetAck().Outcome)

	resp, err = stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.GetProgress())

	resp, err = stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.GetDone())

	_, err = stream.Recv()
	require.ErrorContains(t, err, "EOF")
}

func openSyncNow(t *testing.T, ctx context.Context, cmr *consumertest.Consumer, token, taskName string) pr.TaskControl_SyncNowClient {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	var stream, err = pr.NewTaskControlClient(cmr.Service.Loopback).
		SyncNow(pb.WithDispatchDefault(ctx), &pr.SyncNowRequest{TaskName: taskName})
	require.NoError(t, err)
	return stream
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

// stubSidecar is a scripted TaskControl gRPC server standing in for the
// runtime-next sidecar, recording what the relay delivers to it.
type stubSidecar struct {
	endpoint string
	server   *grpc.Server

	mu       sync.Mutex
	serveFn  func(*pr.SyncNowRequest, pr.TaskControl_SyncNowServer) error
	taskName string
	auth     string
}

func newStubSidecar(t *testing.T) *stubSidecar {
	var listener, err = net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var stub = &stubSidecar{
		endpoint: "http://" + listener.Addr().String(),
		server:   grpc.NewServer(),
	}
	pr.RegisterTaskControlServer(stub.server, stub)
	go func() { _ = stub.server.Serve(listener) }()
	return stub
}

// script installs the SyncNow behavior for the next test case.
func (s *stubSidecar) script(fn func(*pr.SyncNowRequest, pr.TaskControl_SyncNowServer) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.serveFn, s.taskName, s.auth = fn, "", ""
}

// observed returns the task name and Authorization of the last request.
func (s *stubSidecar) observed() (taskName, auth string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.taskName, s.auth
}

func (s *stubSidecar) SyncNow(req *pr.SyncNowRequest, stream pr.TaskControl_SyncNowServer) error {
	var md, _ = metadata.FromIncomingContext(stream.Context())

	s.mu.Lock()
	var fn = s.serveFn
	s.taskName = req.TaskName
	if auth := md.Get("authorization"); len(auth) != 0 {
		s.auth = auth[0]
	}
	s.mu.Unlock()

	return fn(req, stream)
}

// taskControlTestApp is a minimal consumer.Application whose shards become
// resolvable primaries without a broker, recovery log, or messages: as a
// MessageProducer it simply never produces, so shards idle.
type taskControlTestApp struct{}
type taskControlTestStore struct{}

func (taskControlTestApp) NewStore(consumer.Shard, *recoverylog.Recorder) (consumer.Store, error) {
	return taskControlTestStore{}, nil
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
