package flow

import (
	"context"
	"fmt"
	"io"
	"testing"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/message"
)

func TestTransactionLifeCycle(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = brokertest.NewBroker(t, etcd, "local", "broker")
	var ajc = client.NewAppendService(context.Background(), broker.Client())
	var pub = message.NewPublisher(ajc, nil)

	var journals, err = NewJournalsKeySpace(ctx, etcd, "/broker.test")
	require.NoError(t, err)
	journals.WatchApplyDelay = 0
	go journals.Watch(ctx, etcd)

	// Create an output Mapper.
	var collectionSpec, shuffleResponse, combineResponse = buildFixtures()
	var mapper = &Mapper{
		Ctx:           ctx,
		JournalClient: broker.Client(),
		Journals:      journals,
	}

	// Start a Transaction, with a mocked RPC stream.
	var (
		tx  mockTx
		rx  = make(chan deriveResponseOrError, 10)
		txn = &Transaction{
			derivation: &collectionSpec,
			mapFn:      mapper.Map,
			tx:         &tx,
			rx:         rx,
		}

		// Utility to facilitate building message.Envelopes.
		makeEnv = func(index int) message.Envelope {
			return message.Envelope{
				Message: pf.IndexedShuffleResponse{
					ShuffleResponse: &shuffleResponse,
					Index:           index,
					Transform:       &pf.TransformSpec{CatalogDbId: int32(index)},
				},
			}
		}
	)

	// Case: Open fails, then succeeds.
	tx.err = fmt.Errorf("whoops")
	require.EqualError(t, txn.Open(), "sending DeriveRequest_Open: whoops")
	tx.take()

	tx.err = nil
	require.NoError(t, txn.Open())
	require.Equal(t, tx.take(), &pf.DeriveRequest{
		Kind: &pf.DeriveRequest_Open_{
			Open: &pf.DeriveRequest_Open{Collection: "a/collection"}},
	})

	// Consumes of "Continue" messages queue but don't poll the RPC to send.
	require.NoError(t, txn.ConsumeMessage(nil, makeEnv(msgCont1), nil))
	require.NoError(t, txn.ConsumeMessage(nil, makeEnv(msgCont2), nil))

	// Expect documents were queued into |txn.next|.
	require.Equal(t, [][]byte{[]byte("continue"), []byte("2nd-cont")},
		txn.next.Arena.AllBytes(txn.next.DocsJson...))
	require.Equal(t, []pf.UUIDParts{
		{ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
		{ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
	}, txn.next.UuidParts)
	require.Equal(t, [][]byte{[]byte("key-cont"), []byte("key-2cont")},
		txn.next.Arena.AllBytes(txn.next.PackedKey...))
	require.Equal(t, []int32{0, 1}, txn.next.TransformId)

	// Consume of "ACK" _isn't_ queued, but does poll the RPC.
	require.NoError(t, txn.ConsumeMessage(nil, makeEnv(msgAck), nil))

	// |txn.next| was cleared.
	require.Equal(t, pf.DeriveRequest_Continue{
		Arena:       pf.Arena{}, // Non-nil but empty (for re-use).
		DocsJson:    []pf.Slice{},
		UuidParts:   []pf.UUIDParts{},
		PackedKey:   []pf.Slice{},
		TransformId: []int32{},
	}, txn.next)

	// Documents were sent, and |flighted| incremented.
	var sent = tx.take()
	require.Len(t, sent.Kind.(*pf.DeriveRequest_Continue_).Continue.DocsJson, 2)
	require.Equal(t, 1, txn.flighted)

	// Now, suppose we've flighted as many Continues as we're allowed.
	txn.flighted = txnMaxFlighted

	// Consume a Continue followed by an ACK.
	require.NoError(t, txn.ConsumeMessage(nil, makeEnv(msgCont1), nil))
	require.NoError(t, txn.ConsumeMessage(nil, makeEnv(msgAck), nil))

	// Expect documents were not sent.
	require.Equal(t, txnMaxFlighted, txn.flighted)
	require.Len(t, txn.next.DocsJson, 1)
	require.Nil(t, tx.take())

	// Consume another ACK, but this time there's a ready Continue response.
	rx <- deriveResponseOrError{DeriveResponse: pf.DeriveResponse{
		Kind: &pf.DeriveResponse_Continue_{Continue: nil}}}
	require.NoError(t, txn.ConsumeMessage(nil, makeEnv(2), nil))

	// This time, |next| was flighted.
	require.Len(t, tx.take().Kind.(*pf.DeriveRequest_Continue_).Continue.DocsJson, 1)
	require.Equal(t, txnMaxFlighted, txn.flighted)
	require.Len(t, txn.next.DocsJson, 0) // Cleared for re-use.

	// Consume a Continue, then another Continue which tips us over the arena threshold.
	// Because we're at |txnMaxFlighted|, we must block until a response Continue is
	// received, opening up the window.
	require.NoError(t, txn.ConsumeMessage(nil, makeEnv(msgCont1), nil))

	var realThreshold = txnArenaThreshold
	txnArenaThreshold = 1

	go func() {
		rx <- deriveResponseOrError{DeriveResponse: pf.DeriveResponse{
			Kind: &pf.DeriveResponse_Continue_{Continue: nil}}}
	}()

	require.NoError(t, txn.ConsumeMessage(nil, makeEnv(msgCont2), nil))
	txnArenaThreshold = realThreshold

	require.Len(t, tx.take().Kind.(*pf.DeriveRequest_Continue_).Continue.DocsJson, 2)
	require.Equal(t, txnMaxFlighted, txn.flighted)
	require.Len(t, txn.next.DocsJson, 0) // Cleared for re-use.

	// Consume an "Outside" message, which is queued.
	require.NoError(t, txn.ConsumeMessage(nil, makeEnv(msgOutside), nil))
	require.Len(t, txn.next.DocsJson, 1)

	// We're reading to finalize the transaction. As precondition,
	// we have a Continue remainder which must be flighted, and we're
	// awaiting two other Continue response.
	txn.flighted = 2

	// Queue Continue responses.
	for i := 0; i != txn.flighted+1; i++ {
		rx <- deriveResponseOrError{DeriveResponse: pf.DeriveResponse{
			Kind: &pf.DeriveResponse_Continue_{Continue: nil}}}
	}
	// Queue our |combineResponse| fixture.
	rx <- deriveResponseOrError{DeriveResponse: pf.DeriveResponse{
		Kind: &pf.DeriveResponse_Flush{Flush: &combineResponse}}}
	// Empty CombineResponse signals end-of-flush.
	rx <- deriveResponseOrError{DeriveResponse: pf.DeriveResponse{
		Kind: &pf.DeriveResponse_Flush{Flush: new(pf.CombineResponse)}}}

	require.NoError(t, txn.FinalizeTxn(nil, pub))

	// Expect we sent a Continue with our pending document, followed by a Flush.
	require.Len(t, tx.take().Kind.(*pf.DeriveRequest_Continue_).Continue.DocsJson, 1)
	require.Equal(t, &pf.DeriveRequest_Flush{
		UuidPlaceholderPtr: "/uuid",
		FieldPtrs:          []string{"/bar/ptr", "/foo/ptr", "/key"},
	}, tx.take().Kind.(*pf.DeriveRequest_Flush_).Flush)

	// Mapper was used to publish our message fixtures, and created partitions on-demand.
	journals.Mu.RLock()
	defer journals.Mu.RUnlock()

	require.Len(t, journals.KeyValues, 2)
	for i, n := range []string{
		"a/collection/bar=32/foo=A/pivot=00",
		"a/collection/bar=42/foo=B/pivot=00",
	} {
		require.Equal(t, n, journals.KeyValues[i].Decoded.(*pb.JournalSpec).Name.String())
	}

	// Build a non-trivial CheckPoint fixture we'll pass through.
	intents, err := pub.BuildAckIntents()
	require.NoError(t, err)

	var checkpoint = pc.BuildCheckpoint(pc.BuildCheckpointArgs{
		ReadThrough: pb.Offsets{"foo": 123},
		ProducerStates: []message.ProducerState{
			{JournalProducer: message.JournalProducer{Journal: "foo"}}},
		AckIntents: intents,
	})

	// Start to commit.
	var appendOps = ajc.PendingExcept("")
	var op = txn.StartCommit(nil, checkpoint, appendOps)
	rx <- deriveResponseOrError{err: io.EOF}

	// Expect the returned future resolves without an error,
	// but not before all of |appendOps| completed.
	require.NoError(t, op.Err())
	for o := range appendOps {
		select {
		case <-o.Done(): // Pass.
		default:
			require.Fail(t, "should already be Done")
		}
	}

	// We sent a Prepare, then cleanly closed.
	require.Equal(t, checkpoint, tx.take().Kind.(*pf.DeriveRequest_Prepare_).Prepare.Checkpoint)
	require.True(t, tx.closed)

	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}

type mockTx struct {
	requests []pf.DeriveRequest
	closed   bool
	err      error
}

func (tx *mockTx) Send(req *pf.DeriveRequest) error {
	// Marshal, then unmarshal to deep-copy.
	var b, _ = req.Marshal()
	var clone pf.DeriveRequest
	_ = clone.Unmarshal(b)
	tx.requests = append(tx.requests, clone)
	return tx.err
}

func (tx *mockTx) CloseSend() error {
	tx.closed = true
	return tx.err
}

func (tx *mockTx) take() (out *pf.DeriveRequest) {
	if len(tx.requests) == 0 {
		return nil
	}
	out = &tx.requests[0]
	tx.requests = tx.requests[1:]
	return
}

func buildFixtures() (pf.CollectionSpec, pf.ShuffleResponse, pf.CombineResponse) {
	var spec = pf.CollectionSpec{
		Name:    "a/collection",
		KeyPtrs: []string{"/key"},
		UuidPtr: "/uuid",
		Partitions: []pf.Projection{
			{Field: "bar", Ptr: "/bar/ptr"},
			{Field: "foo", Ptr: "/foo/ptr"},
		},
		JournalSpec: *brokertest.Journal(pb.JournalSpec{}),
	}

	var cr pf.CombineResponse
	cr.DocsJson = cr.Arena.AddAll(
		[]byte(`{"one":1,"_uuid":"`+string(pf.DocumentUUIDPlaceholder)+`"}`+"\n"),
		[]byte(`{"two":2,"_uuid":"`+string(pf.DocumentUUIDPlaceholder)+`"}`+"\n"),
	)
	cr.Fields = []pf.Field{
		// Logical partition portion of fields.
		{
			Values: []pf.Field_Value{
				{Kind: pf.Field_Value_UNSIGNED, Unsigned: 32},
				{Kind: pf.Field_Value_UNSIGNED, Unsigned: 42},
			},
		},
		{
			Values: []pf.Field_Value{
				{Kind: pf.Field_Value_STRING, Bytes: cr.Arena.Add([]byte("A"))},
				{Kind: pf.Field_Value_STRING, Bytes: cr.Arena.Add([]byte("B"))},
			},
		},
		// Collection key portion of fields.
		{
			Values: []pf.Field_Value{
				{Kind: pf.Field_Value_TRUE},
				{Kind: pf.Field_Value_FALSE},
			},
		},
	}

	var sr pf.ShuffleResponse
	sr.DocsJson = sr.Arena.AddAll(
		[]byte("continue"),
		[]byte("2nd-cont"),
		[]byte("ack"),
		[]byte("outside"),
	)
	sr.PackedKey = sr.Arena.AddAll(
		[]byte("key-cont"),
		[]byte("key-2cont"),
		[]byte("key-ack"),
		[]byte("key-out"),
	)
	sr.UuidParts = []pf.UUIDParts{
		{ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
		{ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
		{ProducerAndFlags: uint64(message.Flag_ACK_TXN)},
		{ProducerAndFlags: uint64(message.Flag_OUTSIDE_TXN)},
	}

	return spec, sr, cr
}

const (
	// Indexes which correspond to the ShuffleResponse fixture.
	msgCont1   = 0
	msgCont2   = 1
	msgAck     = 2
	msgOutside = 3
)
