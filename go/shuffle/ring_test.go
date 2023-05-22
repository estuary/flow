package shuffle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/labels"
)

func TestReadingDocuments(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()
	var ctx, cancel = context.WithCancel(pb.WithDispatchDefault(context.Background()))
	defer cancel()

	var bk = brokertest.NewBroker(t, etcd, "local", "broker")
	brokertest.CreateJournals(t, bk, brokertest.Journal(pb.JournalSpec{
		Name:     "a/journal",
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	}))

	// Write a bunch of Document fixtures.
	var count = 100
	var record = []byte(`{"value":"` + strings.Repeat("0123456789", 100) + "\"}\n")

	var app = client.NewAppender(ctx, bk.Client(), pb.AppendRequest{Journal: "a/journal"})
	for i := 0; i != count; i++ {
		var _, err = app.Write(record)
		require.NoError(t, err)
	}
	require.NoError(t, app.Close())

	var ch = make(chan *pr.ShuffleResponse, 1)

	// Place a fixture in |ch| which has a non-empty arena with no remaining capacity.
	// This exercises |readDocument|'s back-pressure handling.
	ch <- &pr.ShuffleResponse{
		WriteHead: 1, // Not tailing.
		Arena:     make(pf.Arena, 1),
	}

	var coordinator = NewCoordinator(ctx, localPublisher, bk.Client())
	var ring = newRing(coordinator, ringKey{
		journal: "a/journal",
		replay:  false,
		buildID: "a-build",
	})

	go ring.readDocuments(ch, pb.ReadRequest{
		Journal:   "a/journal",
		EndOffset: app.Response.Commit.End,
	})

	// Sleep a moment to allow the request to start & tickle back-pressure
	// handling. This may not always be enough time. That's okay, the behavior
	// of this test is stable regardless of whether we win or lose the race.
	time.Sleep(time.Millisecond)

	// Expect to read our unmodified back-pressure fixture.
	require.Equal(t, &pr.ShuffleResponse{
		WriteHead: 1,
		Arena:     make(pf.Arena, 1),
	}, <-ch)

	// Expect to read all fixtures, followed by a channel close.
	for out := range ch {
		require.Equal(t, "", out.TerminalError)

		if l := len(out.Docs); l > 0 {
			require.Equal(t, record, out.Arena.Bytes(out.Docs[0]), record)
			count -= l
		}
		// The final ShuffleResponse (only) should have the Tailing bit set.
		require.Equal(t, count == 0, out.Tailing())
	}
	require.Equal(t, count, 0)

	// Case: Start a read that's at the current write head.
	ch = make(chan *pr.ShuffleResponse, 1)

	go ring.readDocuments(ch, pb.ReadRequest{
		Journal: "a/journal",
		Offset:  app.Response.Commit.End,
	})

	// Expect an initial ShuffleResponse which informs us that we're tailing the live log.
	require.Equal(t, &pr.ShuffleResponse{
		ReadThrough: app.Response.Commit.End,
		WriteHead:   app.Response.Commit.End,
	}, <-ch)

	// Write a single record, and expect to receive a tailing read.
	app = client.NewAppender(ctx, bk.Client(), pb.AppendRequest{Journal: "a/journal"})
	_, _ = app.Write(record)
	require.NoError(t, app.Close())

	var out = <-ch
	require.Equal(t, "", out.TerminalError)
	require.Equal(t, [][]byte{record}, out.Arena.AllBytes(out.Docs...))
	require.Equal(t, []pb.Offset{app.Response.Commit.Begin, app.Response.Commit.End}, out.Offsets)
	require.Equal(t, app.Response.Commit.End, out.ReadThrough)
	require.Equal(t, app.Response.Commit.End, out.WriteHead)

	// Case: Start a read which errors. Expect it's passed through, then the channel is closed.
	ch = make(chan *pr.ShuffleResponse, 1)

	go ring.readDocuments(ch, pb.ReadRequest{
		Journal:   "a/journal",
		Offset:    0,
		EndOffset: 20, // EOF unexpectedly, in the middle of a message.
	})

	out = <-ch
	require.Equal(t, "unexpected EOF", out.TerminalError)
	require.Equal(t, [][]byte{record[:20]}, out.Arena.AllBytes(out.Docs...))
	require.Equal(t, []pb.Offset{0, 20}, out.Offsets)
	require.Equal(t, int64(20), out.ReadThrough)
	require.Equal(t, app.Response.Commit.End, out.WriteHead)

	// Expect channel is closed after sending TerminalError.
	var _, ok = <-ch
	require.False(t, ok)
}

func TestDocumentExtraction(t *testing.T) {
	var coordinator = NewCoordinator(context.Background(), localPublisher, nil)
	var r = newRing(coordinator, ringKey{
		journal: "a/journal",
		replay:  false,
		buildID: "a-build",
	})

	var staged pr.ShuffleResponse
	staged.Docs = staged.Arena.AddAll([]byte("doc-1\n"), []byte("doc-2\n"))

	// Case: extraction fails.
	r.onExtract(&staged, nil, nil, fmt.Errorf("an error"))
	require.Equal(t, pr.ShuffleResponse{
		Arena:         pf.Arena([]byte("doc-1\ndoc-2\n")),
		Docs:          []pf.Slice{{Begin: 0, End: 6}, {Begin: 6, End: 12}},
		TerminalError: "an error",
	}, staged)
	staged.TerminalError = "" // Reset.

	// Case: extraction succeeds, with two documents having a single field.
	// Expect shuffling decisions are made & attached to documents.
	// The response Arena was extended with field bytes.
	var uuids = []pf.UUIDParts{{Clock: 123}, {Clock: 456}}
	var fields = [][]byte{
		tuple.Tuple{uint64(42)}.Pack(),
		tuple.Tuple{"some-string"}.Pack(),
	}
	r.onExtract(&staged, uuids, fields, nil)

	require.Equal(t, pr.ShuffleResponse{
		Arena:     pf.Arena([]byte("doc-1\ndoc-2\n\025*\002some-string\000")),
		Docs:      []pf.Slice{{Begin: 0, End: 6}, {Begin: 6, End: 12}},
		UuidParts: []pf.UUIDParts{{Clock: 123}, {Clock: 456}},
		PackedKey: []pf.Slice{{Begin: 12, End: 14}, {Begin: 14, End: 27}},
	}, staged)
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
