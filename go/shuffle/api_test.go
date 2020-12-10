package shuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/jgraettinger/cockroach-encoding/encoding"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
				message.Clock(i<<4), message.Flag_OUTSIDE_TXN).String(),
			i%3,
			i%2,
		)
		var _, err = app.Write([]byte(record))
		require.NoError(t, err)
	}
	require.NoError(t, app.Close())

	// Start a shuffled read of the fixtures.
	var shuffle = pf.JournalShuffle{
		Journal:     "a/journal",
		Coordinator: "the-coordinator",
		Shuffle: pf.Shuffle{
			ShuffleKeyPtr: []string{"/a", "/b"},
			FilterRClocks: true,
		},
	}
	var ranges = pf.RangeSpec{
		// Observe only messages having {"a": 1}.
		KeyBegin: encoding.EncodeUvarintAscending(nil, 1),
		KeyEnd:   encoding.EncodeUvarintAscending(nil, 2),
		// Observe only even Clock values.
		RClockBegin: 0,
		RClockEnd:   1 << 63,
	}

	// Build coordinator and start a gRPC ShuffleServer over loopback.
	// Use a resolve() fixture which returns a mocked store with our |coordinator|.
	var srv = server.MustLoopback()
	var apiCtx, cancelAPICtx = context.WithCancel(backgroundCtx)
	var coordinator = NewCoordinator(apiCtx, bk.Client(), pf.NewExtractClient(wh.Conn))

	pf.RegisterShufflerServer(srv.GRPCServer, &API{
		resolve: func(args consumer.ResolveArgs) (consumer.Resolution, error) {
			require.Equal(t, args.ShardID, pc.ShardID("the-coordinator"))

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
		Shuffle: shuffle,
		Range:   ranges,
		Offset:  app.Response.Commit.End,
	})
	require.NoError(t, err)

	// Expect we read a ShuffleResponse which tells us we're currently tailing.
	out, err := tailStream.Recv()
	require.Equal(t, &pf.ShuffleResponse{
		ReadThrough: app.Response.Commit.End,
		WriteHead:   app.Response.Commit.End,
	}, out)

	// Start a non-blocking, fixed read which "replays" the written fixtures.
	replayStream, err := pf.NewShufflerClient(srv.GRPCLoopback).Shuffle(backgroundCtx, &pf.ShuffleRequest{
		Shuffle:   shuffle,
		Range:     ranges,
		Offset:    0,
		EndOffset: app.Response.Commit.End,
	})
	require.NoError(t, err)

	// Read from |replayStream| until EOF.
	var actual = pf.ShuffleResponse{
		ShuffleKey: make([]pf.Field, len(shuffle.ShuffleKeyPtr)),
	}
	for {
		var out, err = replayStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, "", out.TerminalError)

		var content = out.Arena.AllBytes(out.DocsJson...)
		actual.DocsJson = append(actual.DocsJson, actual.Arena.AddAll(content...)...)
		actual.Begin = append(actual.Begin, out.Begin...)
		actual.End = append(actual.End, out.End...)
		actual.UuidParts = append(actual.UuidParts, out.UuidParts...)
		actual.PackedKey = actual.Arena.AddAll(out.Arena.AllBytes(out.PackedKey...)...)

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
		require.NoError(t, json.Unmarshal(actual.Arena.Bytes(actual.DocsJson[doc]), &record))
		require.Equal(t, 1, record.A)
		require.Equal(t, "0", record.B)
		require.Equal(t, 0, i%2)
		require.Equal(t, parts.Pack(), record.Meta.UUID)

		// Composite shuffle key components were extracted and accompany responses.
		require.Equal(t, uint64(1), actual.ShuffleKey[0].Values[doc].Unsigned)
		require.Equal(t, "0", string(actual.Arena.Bytes(actual.ShuffleKey[1].Values[doc].Bytes)))
	}
	// We see 1/3 of key values, and a further 1/2 of those clocks.
	require.Equal(t, 16, len(actual.DocsJson))

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
	require.True(t, message.Flags(out.UuidParts[0].ProducerAndFlags)&message.Flag_ACK_TXN != 0)

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
