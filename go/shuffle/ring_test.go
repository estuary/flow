package shuffle

import (
	"context"
	"strings"
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocol"
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

	var ctx = pb.WithDispatchDefault(context.Background())
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

	var ch = make(chan pf.ShuffleResponse, 1)

	// Place a fixture in |ch| which simulates a very large ShuffleResponse.
	// This excercises |readDocument|'s back-pressure handling.
	ch <- pf.ShuffleResponse{
		Documents: []pf.Document{{End: responseSizeThreshold}},
	}

	go readDocuments(ctx, bk.Client(), pb.ReadRequest{
		Journal:   "a/journal",
		EndOffset: app.Response.Commit.End,
	}, ch)

	// Sleep a moment to allow the request to start & tickle back-pressure
	// handling. This may not always be enough time. That's okay, the behavior
	// of this test is stable regardless of whether we win or lose the race.
	time.Sleep(time.Millisecond)

	// Expect to read our unmodified back-pressure fixture.
	require.Equal(t, pf.ShuffleResponse{
		Documents: []pf.Document{{End: responseSizeThreshold}},
	}, <-ch)

	// Expect to read all fixtures, followed by a channel close.
	for out := range ch {
		require.Equal(t, "", out.TerminalError)

		if l := len(out.Documents); l > 0 {
			require.Equal(t, out.Documents[0].Content, record)
			require.Equal(t, out.Documents[0].ContentType, pf.Document_JSON)
			count -= l
		}
	}
	require.Equal(t, count, 0)

	// Start a read which errors. Expect it's passed through, then the channel is closed.
	ch = make(chan pf.ShuffleResponse, 1)

	go readDocuments(ctx, bk.Client(), pb.ReadRequest{
		Journal: "does/not/exist",
	}, ch)

	var out = <-ch
	require.Equal(t, "fetching journal spec: named journal does not exist (does/not/exist)", out.TerminalError)
	var _, ok = <-ch
	require.False(t, ok)
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
