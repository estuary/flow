package shuffle

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"testing"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/auth"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAPIIntegrationWithFixtures(t *testing.T) {
	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "2222222222222222",
			BuildDb:    path.Join(t.TempDir(), "build.db"),
			Source:     "file:///ab.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, bindings.BuildCatalog(args))

	var derivation *pf.CollectionSpec
	require.NoError(t, catalog.Extract(args.BuildDb, func(db *sql.DB) (err error) {
		derivation, err = catalog.LoadCollection(db, "a/derivation")
		return err
	}))

	var backgroundCtx = pb.WithDispatchDefault(context.Background())
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var bk = brokertest.NewBroker(t, etcd, "local", "broker")
	var journalSpec = brokertest.Journal(pb.JournalSpec{
		Name:     "a/journal",
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	})
	brokertest.CreateJournals(t, bk, journalSpec)

	// Write a bunch of Document fixtures.
	var app = client.NewAppender(backgroundCtx, bk.Client(), pb.AppendRequest{Journal: "a/journal"})

	for i := 0; i != 100; i++ {
		var record = fmt.Sprintf(`{"_meta":{"uuid":"%s"}, "a": %d, "aa": "%d", "b": "%d"}`+"\n",
			message.BuildUUID(message.ProducerID{8, 6, 7, 5, 3, 0},
				message.Clock(i<<4), message.Flag_OUTSIDE_TXN).String(),
			i%3,
			i%3,
			i%2,
		)
		var _, err = app.Write([]byte(record))
		require.NoError(t, err)
	}
	require.NoError(t, app.Close())

	// Start a shuffled read of the fixtures.
	// Observe only messages having {"a": 1, "aa": "1"}, and not 0 or 2.
	var expectKey = tuple.Tuple{1, "1"}.Pack()
	var range_ = pf.RangeSpec{
		KeyBegin: flow.PackedKeyHash_HH64(expectKey),
		KeyEnd:   flow.PackedKeyHash_HH64(expectKey) + 1,
		// Observe only even Clock values.
		RClockBegin: 0,
		RClockEnd:   1 << 31,
	}
	var req = pr.ShuffleRequest{
		Journal:      "a/journal",
		Replay:       false,
		BuildId:      "a-build-id",
		Offset:       app.Response.Commit.End,
		EndOffset:    0,
		Range:        range_,
		Coordinator:  "the-coordinator",
		Resolution:   &mockHeader,
		ShuffleIndex: 0,
		Derivation:   derivation,
	}

	// Build coordinator and start a gRPC ShuffleServer over loopback.
	// Use a resolve() fixture which returns a mocked store with our |coordinator|.
	var srv = server.MustLoopback()
	var apiCtx, cancelAPICtx = context.WithCancel(backgroundCtx)
	var coordinator = NewCoordinator(apiCtx, localPublisher, bk.Client())

	var auth, err = auth.NewKeyedAuth("c2VjcmV0")
	require.NoError(t, err)

	var api pr.AuthShufflerServer = &API{
		resolve: func(args consumer.ResolveArgs) (consumer.Resolution, error) {
			require.Equal(t, args.ShardID, pc.ShardID("the-coordinator"))

			return consumer.Resolution{
				Store: &testStore{coordinator: coordinator},
				Done:  func() {},
			}, nil
		},
	}
	pr.RegisterShufflerServer(srv.GRPCServer, pr.NewAuthShufflerServer(api, auth))

	var shuffler = pr.NewAuthShufflerClient(pr.NewShufflerClient(srv.GRPCLoopback), auth)
	var tasks = task.NewGroup(apiCtx)
	srv.QueueTasks(tasks)
	tasks.GoRun()

	// Start a blocking read which starts at the current write head.
	tailStream, err := shuffler.Shuffle(backgroundCtx, &req)
	require.NoError(t, err)

	// Expect we read a ShuffleResponse which tells us we're currently tailing.
	out, err := tailStream.Recv()
	require.NoError(t, err)
	require.Equal(t, &pr.ShuffleResponse{
		ReadThrough: app.Response.Commit.End,
		WriteHead:   app.Response.Commit.End,
	}, out)

	// Start a non-blocking, fixed read which "replays" the written fixtures.
	var mockResolveFn = func(args consumer.ResolveArgs) (consumer.Resolution, error) {
		// This a no-op fixture intended only to Validate.
		return consumer.Resolution{Header: mockHeader}, nil
	}

	req.Replay = true
	req.Offset = 0
	req.EndOffset = app.Response.Commit.End

	var replayRead = &read{
		publisher: localPublisher,
		spec:      *journalSpec,
		req:       req,
	}
	replayRead.start(backgroundCtx, 0, mockResolveFn, shuffler, nil)

	// Read from |replayRead| until EOF.
	var replayDocs int
	for {
		var env, err = replayRead.next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		replayDocs++
		var msg = env.Message.(pr.IndexedShuffleResponse)

		// Verify expected record shape.
		var record struct {
			Meta struct {
				message.UUID
			} `json:"_meta"`
			A  int
			AA string
			B  string
		}
		require.NoError(t, json.Unmarshal(msg.Arena.Bytes(msg.Docs[msg.Index]), &record))

		require.Equal(t, 1, record.A)
		require.Equal(t, "1", record.AA)
		require.Equal(t, "0", record.B)
		require.Equal(t, 0, int(msg.UuidParts[msg.Index].Clock)%2)
		require.Equal(t, msg.GetUUID(), record.Meta.UUID)

		// Composite shuffle key components were extracted and packed into response keys.
		require.Equal(t, expectKey, msg.Arena.Bytes(msg.PackedKey[msg.Index]))
	}
	// We see 1/3 of key values, and a further 1/2 of those clocks.
	require.Equal(t, 16, replayDocs)

	// Interlude: Another replay read, this time with an invalid schema.
	derivation.Derivation.Transforms[0].Collection.ReadSchemaJson = []byte(`{"invalid":"keyword"}`)

	var badRead = &read{
		publisher: localPublisher,
		spec:      *journalSpec,
		req:       req,
	}
	badRead.start(backgroundCtx, 0, mockResolveFn, shuffler, nil)

	// Expect we read an error, and that TerminalError is set.
	_, err = badRead.next()
	require.Equal(t, io.EOF, err, err.Error())
	require.Regexp(t, "building document extractor: building bundled JSON schema.*", badRead.resp.TerminalError)

	// Write and commit a single ACK document.
	app = client.NewAppender(backgroundCtx, bk.Client(), pb.AppendRequest{Journal: "a/journal"})
	var record = fmt.Sprintf(`{"_meta":{"uuid":"%s"}}`+"\n",
		message.BuildUUID(message.ProducerID{8, 6, 7, 5, 3, 0},
			message.Clock(100<<4), message.Flag_ACK_TXN).String())
	_, err = app.Write([]byte(record))
	require.NoError(t, err)
	require.NoError(t, app.Close())

	// Expect it's sent to |tailStream|.
	out, err = tailStream.Recv()
	require.NoError(t, err)
	require.Len(t, out.UuidParts, 1)
	require.True(t, message.Flags(out.UuidParts[0].Node)&message.Flag_ACK_TXN != 0)

	// Cancel the server-side API context, then do a GracefulStop() (*not* a BoundedGracefulStop)
	// of the server. This will hang if the API doesn't properly unwind our in-flight tailing RPC.
	cancelAPICtx()
	srv.GRPCServer.GracefulStop()

	// We expect to read an "unavailable" status after being kicked off the server.
	_, err = tailStream.Recv()
	var s, _ = status.FromError(err)
	require.Equal(t, codes.Unavailable, s.Code())

	require.NoError(t, tasks.Wait())
}

var localPublisher = ops.NewLocalPublisher(
	ops.ShardLabeling{
		Build:    "the-build",
		LogLevel: ops.Log_debug,
		Range: pf.RangeSpec{
			KeyBegin:    0x00001111,
			KeyEnd:      0x11110000,
			RClockBegin: 0x00002222,
			RClockEnd:   0x22220000,
		},
		TaskName: "some-tenant/task/name",
		TaskType: ops.TaskType_derivation,
	},
)

var mockHeader = pb.Header{
	Route: pb.Route{Primary: -1},
	Etcd: pb.Header_Etcd{
		ClusterId: 1234,
		MemberId:  1234,
		Revision:  1234,
		RaftTerm:  1234,
	},
}
