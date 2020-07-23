package shuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/protocol"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

func TestAPIIntegrationWithFixtures(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	ctx = pb.WithDispatchDefault(ctx)
	var bk = brokertest.NewBroker(t, etcd, "local", "broker")

	brokertest.CreateJournals(t, bk, brokertest.Journal(pb.JournalSpec{
		Name:     "a/journal",
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	}))

	// Write a bunch of Document fixtures.
	var app = client.NewAppender(ctx, bk.Client(), pb.AppendRequest{Journal: "a/journal"})

	for i := 0; i != 100; i++ {
		var record = fmt.Sprintf(`{"_meta":{"uuid":"%s"}, "a": %d, "b": "%d"}`+"\n",
			message.BuildUUID(message.ProducerID{8, 6, 7, 5, 3, 0},
				message.Clock(i), message.Flag_CONTINUE_TXN).String(),
			i%3,
			i%2,
		)
		var _, err = app.Write([]byte(record))
		require.NoError(t, err)
	}
	// Write a single ACK Document and commit the append.
	var record = fmt.Sprintf(`{"_meta":{"uuid":"%s"}}`+"\n",
		message.BuildUUID(message.ProducerID{8, 6, 7, 5, 3, 0},
			message.Clock(100), message.Flag_ACK_TXN).String(),
	)
	var _, err = app.Write([]byte(record))
	require.NoError(t, err)
	require.NoError(t, app.Close())

	// Start a flow-worker to serve the extraction RPC.
	wh, err := flow.NewWorkerHost("extract")
	require.Nil(t, err)
	defer wh.Stop()

	// Build coordinator and start gRPC ShuffleServer over loopback.
	var coordinator = newCoordinator(ctx, bk.Client(), pf.NewExtractClient(wh.Conn))
	var srv = server.MustLoopback()
	pf.RegisterShufflerServer(srv.GRPCServer, &API{
		fooCoordinator: coordinator,
	})

	var tasks = task.NewGroup(ctx)
	srv.QueueTasks(tasks)
	tasks.GoRun()

	// Start a read of the fixtures, shuffled on combinations of /a and /b.
	var cfg = pf.ShuffleConfig{
		Journal: "a/journal",
		Ring: pf.Ring{
			Name:    "a/ring",
			Members: []pf.Ring_Member{{}, {}, {}, {}},
		},
		Coordinator: 1,
		Shuffles: []pf.ShuffleConfig_Shuffle{
			{
				TransformId:   32,
				ShuffleKeyPtr: []string{"/a", "/b"},
				BroadcastTo:   2,
			},
			{
				TransformId:   42,
				ShuffleKeyPtr: []string{"/b"},
				BroadcastTo:   2,
			},
		},
	}

	stream, err := pf.NewShufflerClient(srv.GRPCLoopback).Shuffle(ctx, &pf.ShuffleRequest{
		Config:    cfg,
		RingIndex: 2,
	})
	require.NoError(t, err)

	var docs []pf.Document
	for done := false; !done; {
		var out, err = stream.Recv()
		require.NoError(t, err)
		require.Equal(t, "", out.TerminalError)

		for _, doc := range out.Documents {
			if doc.UuidParts.ProducerAndFlags&uint64(message.Flag_ACK_TXN) != 0 {
				done = true
				break
			}

			var i = int(doc.UuidParts.Clock)

			// Verify expected record shape.
			var record struct {
				Meta struct {
					message.UUID
				} `json:"_meta"`
				A int
				B string
			}
			require.NoError(t, json.Unmarshal(doc.Content, &record))
			require.Equal(t, i%3, record.A)
			require.Equal(t, strconv.Itoa(i%2), record.B)
			require.Equal(t, doc.UuidParts.Pack(), record.Meta.UUID)

			docs = append(docs, pf.Document{
				UuidParts: pf.UUIDParts{Clock: doc.UuidParts.Clock},
				Begin:     doc.Begin,
				End:       doc.End,
				Shuffles:  doc.Shuffles,
			})
		}
		docs = append(docs, out.Documents...)
	}

	// Verify that shuffle outcomes match expectations.
	require.Equal(t, []protocol.Document{
		{Begin: 76, End: 152, UuidParts: pf.UUIDParts{Clock: 1}, Shuffles: []protocol.Document_Shuffle{
			{RingIndex: 0x1, TransformId: 32, Hrw: 0xed5ac51a}, // (a: 1 % 3 = 1, b: 1 % 2 = 1)
			{RingIndex: 0x3, TransformId: 32, Hrw: 0xa20737bc}, // (1, 1)
			{RingIndex: 0x2, TransformId: 42, Hrw: 0x9933cbbe}, // (1)
			{RingIndex: 0x3, TransformId: 42, Hrw: 0x6208dba9}, // (1)
		}},
		{Begin: 228, End: 304, UuidParts: pf.UUIDParts{Clock: 3}, Shuffles: []protocol.Document_Shuffle{
			{RingIndex: 0x3, TransformId: 32, Hrw: 0xbaf8e0c4}, // (0, 1)
			{RingIndex: 0x0, TransformId: 32, Hrw: 0x981ddc74}, // (0, 1)
			{RingIndex: 0x2, TransformId: 42, Hrw: 0x9933cbbe}, // (1)
			{RingIndex: 0x3, TransformId: 42, Hrw: 0x6208dba9}, // (1)
		}},
		{Begin: 304, End: 380, UuidParts: pf.UUIDParts{Clock: 4}, Shuffles: []protocol.Document_Shuffle{
			{RingIndex: 0x1, TransformId: 32, Hrw: 0xfb91ad38}, // (1, 0)
			{RingIndex: 0x2, TransformId: 32, Hrw: 0x9dcfe389}, // (1, 0)
			{RingIndex: 0x1, TransformId: 42, Hrw: 0xee652e7b}, // (0)
			{RingIndex: 0x0, TransformId: 42, Hrw: 0xca57152d}, // (0)
		}},
		{Begin: 380, End: 456, UuidParts: pf.UUIDParts{Clock: 5}, Shuffles: []protocol.Document_Shuffle{
			{RingIndex: 0x1, TransformId: 32, Hrw: 0xf51b1572}, // (2, 1)
			{RingIndex: 0x2, TransformId: 32, Hrw: 0x92faca9d}, // (2, 1)
			{RingIndex: 0x2, TransformId: 42, Hrw: 0x9933cbbe}, // (1)
			{RingIndex: 0x3, TransformId: 42, Hrw: 0x6208dba9}, // (1)
		}},
		{Begin: 532, End: 608, UuidParts: pf.UUIDParts{Clock: 7}, Shuffles: []protocol.Document_Shuffle{
			{RingIndex: 0x1, TransformId: 32, Hrw: 0xed5ac51a}, // (1, 1)
			{RingIndex: 0x3, TransformId: 32, Hrw: 0xa20737bc}, // (1, 1)
			{RingIndex: 0x2, TransformId: 42, Hrw: 0x9933cbbe}, // (1)
			{RingIndex: 0x3, TransformId: 42, Hrw: 0x6208dba9}, // (1)
		}},
		{Begin: 684, End: 760, UuidParts: pf.UUIDParts{Clock: 9}, Shuffles: []protocol.Document_Shuffle{
			{RingIndex: 0x3, TransformId: 32, Hrw: 0xbaf8e0c4}, // (0, 1)
			{RingIndex: 0x0, TransformId: 32, Hrw: 0x981ddc74}, // (0, 1)
			{RingIndex: 0x2, TransformId: 42, Hrw: 0x9933cbbe}, // (1)
			{RingIndex: 0x3, TransformId: 42, Hrw: 0x6208dba9}, // (1)
		}},
	}, docs[:6])
}
