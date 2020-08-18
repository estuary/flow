package shuffle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/jgraettinger/cockroach-encoding/encoding"
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

		if l := len(out.Content); l > 0 {
			require.Equal(t, record, out.Arena.Bytes(out.Content[0]), record)
			require.Equal(t, pf.ContentType_JSON, out.ContentType)
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
	require.Equal(t, pf.ContentType_JSON, out.ContentType)
	require.Equal(t, [][]byte{record}, out.Arena.AllBytes(out.Content...))
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
				Transform:     "a-transform",
				ShuffleKeyPtr: []string{"/foo", "/bar"},
			},
		},
	}

	var staged = pf.ShuffleResponse{
		ContentType: pf.ContentType_JSON,
	}
	staged.Content = staged.Arena.AddAll([]byte("doc-1\n"), []byte("doc-2\n"))

	require.Equal(t, &pf.ExtractRequest{
		Arena:       pf.Arena([]byte("doc-1\ndoc-2\n")),
		ContentType: pf.ContentType_JSON,
		Content:     []pf.Slice{{Begin: 0, End: 6}, {Begin: 6, End: 12}},
		UuidPtr:     pf.DocumentUUIDPointer,
		FieldPtrs:   []string{"/foo", "/bar"},
	}, r.buildExtractRequest(&staged))

	// Case: extraction fails.
	r.onExtract(&staged, nil, fmt.Errorf("an error"))
	require.Equal(t, pf.ShuffleResponse{
		Arena:         pf.Arena([]byte("doc-1\ndoc-2\n")),
		ContentType:   pf.ContentType_JSON,
		Content:       []pf.Slice{{Begin: 0, End: 6}, {Begin: 6, End: 12}},
		TerminalError: "an error",
	}, staged)
	staged.TerminalError = "" // Reset.

	// Case: extraction succeeds, with two documents having a single field.
	// Expect shuffling decisions are made & attached to documents.
	// The response Arena was extended with field bytes.
	var fixture = pf.ExtractResponse{
		UuidParts: []pf.UUIDParts{{Clock: 123}, {Clock: 456}},
	}
	fixture.Fields = []pf.Field{{Values: []pf.Field_Value{
		{Kind: pf.Field_Value_UNSIGNED, Unsigned: 42},
		{Kind: pf.Field_Value_STRING, Bytes: fixture.Arena.Add([]byte("some-string"))},
	}}}
	r.onExtract(&staged, &fixture, nil)

	require.Equal(t, pf.ShuffleResponse{
		Arena:       pf.Arena([]byte("doc-1\ndoc-2\n\262some-string\022some-string\000\001")),
		ContentType: pf.ContentType_JSON,
		Content:     []pf.Slice{{Begin: 0, End: 6}, {Begin: 6, End: 12}},
		UuidParts:   []pf.UUIDParts{{Clock: 123}, {Clock: 456}},
		PackedKey:   []pf.Slice{{Begin: 12, End: 13}, {Begin: 24, End: 38}},
		ShuffleKey: []pf.Field{
			{Values: []pf.Field_Value{
				{Kind: pf.Field_Value_UNSIGNED, Unsigned: 42},
				{Kind: pf.Field_Value_STRING, Bytes: pf.Slice{Begin: 13, End: 24}},
			}}},
	}, staged)

	// Case: extraction succeeds with a single document having multiple fields,
	// which also cover all possible field types.
	staged = pf.ShuffleResponse{}
	fixture = pf.ExtractResponse{
		UuidParts: []pf.UUIDParts{{Clock: 123}},
	}
	fixture.Fields = []pf.Field{
		{Values: []pf.Field_Value{{Kind: pf.Field_Value_NULL}}},
		{Values: []pf.Field_Value{{Kind: pf.Field_Value_TRUE}}},
		{Values: []pf.Field_Value{{Kind: pf.Field_Value_FALSE}}},
		{Values: []pf.Field_Value{{Kind: pf.Field_Value_UNSIGNED, Unsigned: 42}}},
		{Values: []pf.Field_Value{{Kind: pf.Field_Value_SIGNED, Signed: -35}}},
		{Values: []pf.Field_Value{{Kind: pf.Field_Value_DOUBLE, Double: 3.141}}},
		{Values: []pf.Field_Value{{Kind: pf.Field_Value_STRING, Bytes: fixture.Arena.Add([]byte("str"))}}},
		{Values: []pf.Field_Value{{Kind: pf.Field_Value_OBJECT, Bytes: fixture.Arena.Add([]byte("obj"))}}},
		{Values: []pf.Field_Value{{Kind: pf.Field_Value_ARRAY, Bytes: fixture.Arena.Add([]byte("arr"))}}},
	}
	r.onExtract(&staged, &fixture, nil)

	var b []byte
	b = encoding.EncodeNullAscending(b)
	b = encoding.EncodeTrueAscending(b)
	b = encoding.EncodeFalseAscending(b)
	b = encoding.EncodeUvarintAscending(b, 42)
	b = encoding.EncodeVarintAscending(b, -35)
	b = encoding.EncodeFloatAscending(b, 3.141)
	b = encoding.EncodeBytesAscending(b, []byte("str"))
	b = encoding.EncodeBytesAscending(b, []byte("obj"))
	b = encoding.EncodeBytesAscending(b, []byte("arr"))

	require.Equal(t, b, staged.Arena.Bytes(staged.PackedKey[0]))

	// Case: again, but this time expect an MD5 is returned.
	r.shuffle.Hash = pf.Shuffle_MD5
	staged = pf.ShuffleResponse{}
	r.onExtract(&staged, &fixture, nil)

	b = encoding.EncodeBytesAscending(nil,
		[]byte{0x5d, 0x4b, 0xc1, 0x53, 0x5, 0xa5, 0x60, 0x15, 0x6c, 0xa3, 0x96, 0x50, 0xde, 0xd4, 0x4b, 0x2d})
	require.Equal(t, b, staged.Arena.Bytes(staged.PackedKey[0]))
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
