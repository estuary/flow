package shuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

func TestAPIIntegrationWithFixtures(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// Start a flow-worker to serve the extraction RPC.
	wh, err := flow.NewWorkerHost("extract")
	require.Nil(t, err)
	defer wh.Stop()

	var bk = brokertest.NewBroker(t, etcd, "local", "broker")

	brokertest.CreateJournals(t, bk, brokertest.Journal(pb.JournalSpec{
		Name:     "a/journal",
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	}))

	// Write a bunch of Document fixtures.
	var backgroundCtx = pb.WithDispatchDefault(context.Background())
	var app = client.NewAppender(backgroundCtx, bk.Client(), pb.AppendRequest{Journal: "a/journal"})

	for i := 0; i != 100; i++ {
		var record = fmt.Sprintf(`{"_meta":{"uuid":"%s"}, "a": %d, "b": "%d"}`+"\n",
			message.BuildUUID(message.ProducerID{8, 6, 7, 5, 3, 0},
				message.Clock(i), message.Flag_OUTSIDE_TXN).String(),
			i%3,
			i%2,
		)
		var _, err = app.Write([]byte(record))
		require.NoError(t, err)
	}
	require.NoError(t, app.Close())

	// Start a shuffled read of the fixtures.
	var cfg = pf.ShuffleConfig{
		Journal: "a/journal",
		Ring: pf.Ring{
			Name:    "a-ring",
			Members: []pf.Ring_Member{{}, {}, {}, {}},
		},
		Coordinator: 1,
		Shuffle: pf.Shuffle{
			Transform:     "a-transform",
			ShuffleKeyPtr: []string{"/a", "/b"},
			BroadcastTo:   2,
		},
	}

	// Build coordinator and start a gRPC ShuffleServer over loopback.
	// Use a resolve() fixture which returns a mocked store with our |coordinator|.
	var srv = server.MustLoopback()
	var apiCtx, cancelAPICtx = context.WithCancel(backgroundCtx)
	var coordinator = newCoordinator(apiCtx, bk.Client(), pf.NewExtractClient(wh.Conn))

	pf.RegisterShufflerServer(srv.GRPCServer, &API{
		resolve: func(args consumer.ResolveArgs) (consumer.Resolution, error) {
			require.Equal(t, args.ShardID, cfg.Ring.ShardID(int(cfg.Coordinator)))

			return consumer.Resolution{
				Store: &testStore{coordinator: coordinator},
				Done:  func() {},
			}, nil
		},
	})

	var tasks = task.NewGroup(apiCtx)
	srv.QueueTasks(tasks)
	tasks.GoRun()

	// Start a blocking read which starts at the current write head.
	tailStream, err := pf.NewShufflerClient(srv.GRPCLoopback).Shuffle(backgroundCtx, &pf.ShuffleRequest{
		Config:    cfg,
		RingIndex: 2,
		Offset:    app.Response.Commit.End,
	})
	require.NoError(t, err)

	// Expect we read a ShuffleResponse which tells us we're currently tailing.
	out, err := tailStream.Recv()
	require.Equal(t, &pf.ShuffleResponse{
		Transform:   "a-transform",
		ReadThrough: app.Response.Commit.End,
		WriteHead:   app.Response.Commit.End,
	}, out)

	// Start a non-blocking, fixed read which "replays" the written fixtures.
	replayStream, err := pf.NewShufflerClient(srv.GRPCLoopback).Shuffle(backgroundCtx, &pf.ShuffleRequest{
		Config:    cfg,
		RingIndex: 2,
		Offset:    0,
		EndOffset: app.Response.Commit.End,
	})
	require.NoError(t, err)

	// Read from |replayStream| until EOF.
	var actual = pf.ShuffleResponse{
		ShuffleKey: make([]pf.Field, len(cfg.Shuffle.ShuffleKeyPtr)),
	}
	for {
		var out, err = replayStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, "", out.TerminalError)

		var content = out.Arena.AllBytes(out.Content...)
		actual.Content = actual.Arena.AddAll(content...)
		actual.Begin = append(actual.Begin, out.Begin...)
		actual.End = append(actual.End, out.End...)
		actual.UuidParts = append(actual.UuidParts, out.UuidParts...)
		actual.ShuffleHashesLow = append(actual.ShuffleHashesLow, out.ShuffleHashesLow...)
		actual.ShuffleHashesHigh = append(actual.ShuffleHashesLow, out.ShuffleHashesHigh...)

		for k, kk := range out.ShuffleKey {
			for _, vv := range kk.Values {
				actual.ShuffleKey[k].AppendValue(&out.Arena, &actual.Arena, vv)
			}
		}
	}

	for doc, parts := range actual.UuidParts {
		var i = int(parts.Clock)

		// Verify expected record shape.
		var record struct {
			Meta struct {
				message.UUID
			} `json:"_meta"`
			A int
			B string
		}
		require.NoError(t, json.Unmarshal(actual.Arena.Bytes(actual.Content[doc]), &record))
		require.Equal(t, i%3, record.A)
		require.Equal(t, strconv.Itoa(i%2), record.B)
		require.Equal(t, parts.Pack(), record.Meta.UUID)

		// Composite shuffle key components were extracted and accompany responses.
		require.Equal(t, uint64(i%3), actual.ShuffleKey[0].Values[doc].Unsigned)
		require.Equal(t, strconv.Itoa(i%2),
			string(actual.Arena.Bytes(actual.ShuffleKey[1].Values[doc].Bytes)))
	}
	// 100 documents, broadcast to 2 of 4 members, means we see ~50.
	require.Equal(t, 49, len(actual.Content))

	// Write and commit a single ACK document.
	app = client.NewAppender(backgroundCtx, bk.Client(), pb.AppendRequest{Journal: "a/journal"})
	var record = fmt.Sprintf(`{"_meta":{"uuid":"%s"}}`+"\n",
		message.BuildUUID(message.ProducerID{8, 6, 7, 5, 3, 0},
			message.Clock(100), message.Flag_ACK_TXN).String(),
	)
	_, err = app.Write([]byte(record))
	require.NoError(t, err)
	require.NoError(t, app.Close())

	// Expect it's sent to |tailStream|.
	out, err = tailStream.Recv()
	require.NoError(t, err)
	require.Len(t, out.UuidParts, 1)
	require.True(t, message.Flags(out.UuidParts[0].ProducerAndFlags)&message.Flag_ACK_TXN != 0)

	// Cancel the server-side API context, then do a GracefulStop() (*not* a BoundedGracefulStop)
	// of the server. This will hang if the API doesn't properly unwind our in-flight tailing RPC.
	cancelAPICtx()
	srv.GRPCServer.GracefulStop()

	// We expect to read a clean stream EOF after being kicked off the server.
	_, err = tailStream.Recv()
	require.Equal(t, io.EOF, err)

	require.NoError(t, tasks.Wait())
}
