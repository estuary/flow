package shuffle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
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

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	ctx = pb.WithDispatchDefault(ctx)
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
	// This exercises |readDocument|'s back-pressure handling.
	ch <- pf.ShuffleResponse{
		WriteHead: 1, // Not tailing.
		Begin:     []pb.Offset{0},
		End:       []pb.Offset{responseSizeThreshold},
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
		WriteHead: 1,
		Begin:     []pb.Offset{0},
		End:       []pb.Offset{responseSizeThreshold},
	}, <-ch)

	// Expect to read all fixtures, followed by a channel close.
	for out := range ch {
		require.Equal(t, "", out.TerminalError)

		if l := len(out.DocsJson); l > 0 {
			require.Equal(t, record, out.Arena.Bytes(out.DocsJson[0]), record)
			count -= l
		}
		// The final ShuffleResponse (only) should have the Tailing bit set.
		require.Equal(t, count == 0, out.Tailing())
	}
	require.Equal(t, count, 0)

	// Case: Start a read that's at the current write head.
	ch = make(chan pf.ShuffleResponse, 1)

	go readDocuments(ctx, bk.Client(), pb.ReadRequest{
		Journal: "a/journal",
		Offset:  app.Response.Commit.End,
	}, ch)

	// Expect an initial ShuffleResponse which informs us that we're tailing the live log.
	require.Equal(t, pf.ShuffleResponse{
		ReadThrough: app.Response.Commit.End,
		WriteHead:   app.Response.Commit.End,
	}, <-ch)

	// Write a single record, and expect to receive a tailing read.
	app = client.NewAppender(ctx, bk.Client(), pb.AppendRequest{Journal: "a/journal"})
	_, _ = app.Write(record)
	require.NoError(t, app.Close())

	var out = <-ch
	require.Equal(t, [][]byte{record}, out.Arena.AllBytes(out.DocsJson...))
	require.Equal(t, []pb.Offset{app.Response.Commit.Begin}, out.Begin)
	require.Equal(t, []pb.Offset{app.Response.Commit.End}, out.End)
	require.Equal(t, app.Response.Commit.End, out.ReadThrough)
	require.Equal(t, app.Response.Commit.End, out.WriteHead)

	// Case: Start a read which errors. Expect it's passed through, then the channel is closed.
	ch = make(chan pf.ShuffleResponse, 1)

	go readDocuments(ctx, bk.Client(), pb.ReadRequest{
		Journal:   "a/journal",
		Offset:    0,
		EndOffset: 20, // EOF unexpectedly, in the middle of a message.
	}, ch)

	out = <-ch
	require.Equal(t, "unexpected EOF", out.TerminalError)
	var _, ok = <-ch
	require.False(t, ok)
}

func TestDocumentExtraction(t *testing.T) {
	var r = ring{
		shuffle: pf.JournalShuffle{
			Shuffle: pf.Shuffle{
				ShuffleKeyPtr: []string{"/foo", "/bar"},
			},
		},
	}

	var staged pf.ShuffleResponse
	staged.DocsJson = staged.Arena.AddAll([]byte("doc-1\n"), []byte("doc-2\n"))

	// Case: extraction fails.
	r.onExtract(&staged, nil, nil, fmt.Errorf("an error"))
	require.Equal(t, pf.ShuffleResponse{
		Arena:         pf.Arena([]byte("doc-1\ndoc-2\n")),
		DocsJson:      []pf.Slice{{Begin: 0, End: 6}, {Begin: 6, End: 12}},
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

	require.Equal(t, pf.ShuffleResponse{
		Arena:     pf.Arena([]byte("doc-1\ndoc-2\n\025*\002some-string\000")),
		DocsJson:  []pf.Slice{{Begin: 0, End: 6}, {Begin: 6, End: 12}},
		UuidParts: []pf.UUIDParts{{Clock: 123}, {Clock: 456}},
		PackedKey: []pf.Slice{{Begin: 12, End: 14}, {Begin: 14, End: 27}},
	}, staged)

	// Case: extraction succeeds with a single document having multiple fields,
	// which also cover all possible field types.
	staged = pf.ShuffleResponse{}
	uuids = []pf.UUIDParts{{Clock: 123}}

	fields = [][]byte{tuple.Tuple{
		nil,
		true,
		false,
		42,
		-35,
		3.141,
		"str",
		[]byte(`{"k":"v"}`),
		[]byte("[null]"),
	}.Pack()}
	r.onExtract(&staged, uuids, fields, nil)

	var expect = []byte("\x00'&\x15*\x13\xdc!\xc0\t ě\xa5\xe3T\x02str\x00\x01{\"k\":\"v\"}\x00\x01[null]\x00")
	require.Equal(t, expect, staged.Arena.Bytes(staged.PackedKey[0]))

	// Case: again, but this time expect an MD5 is returned.
	r.shuffle.Hash = pf.Shuffle_MD5
	staged = pf.ShuffleResponse{}
	r.onExtract(&staged, uuids, fields, nil)

	expect = []byte("\x01)@\xb8g蝑D\x13\xe8\r֓b\x1d\xe9\x00")
	require.Equal(t, expect, staged.Arena.Bytes(staged.PackedKey[0]))
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
