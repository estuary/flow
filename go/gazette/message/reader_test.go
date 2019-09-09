package message

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/teststub"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
)

func TestReadIterators(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var (
		clock Clock
		spec  = newTestMsgSpec("a/journal")
		bk    = brokertest.NewBroker(t, etcd, "local", "broker")
		ajc   = client.NewAppendService(context.Background(), bk.Client())
		seq   = NewSequencer(nil, 0) // Very limited ring look-back.
		A, B  = NewProducerID(), NewProducerID()
	)
	brokertest.CreateJournals(t, bk, spec)

	// Build message fixtures, where |A| writes un-acknowledged messages and |B|
	// writes acknowledged ones, which are interspersed.
	var allMessages = []testMsg{
		{UUID: BuildUUID(A, clock.Tick(), Flag_CONTINUE_TXN), Str: "A1"},
		{UUID: BuildUUID(B, clock.Tick(), Flag_CONTINUE_TXN), Str: "B1"},
		{UUID: BuildUUID(A, clock.Tick(), Flag_CONTINUE_TXN), Str: "A2"},
		{UUID: BuildUUID(B, clock.Tick(), Flag_CONTINUE_TXN), Str: "B2"},
		{UUID: BuildUUID(B, clock.Tick(), Flag_ACK_TXN)},
	}
	// Publish fixtures to the journal.
	var aa = ajc.StartAppend(pb.AppendRequest{Journal: spec.Name}, nil)
	for _, msg := range allMessages {
		aa.Require(JSONFraming.Marshal(msg, aa.Writer()))
	}
	assert.NoError(t, aa.Release())

	var verify = func(msgs []testMsg, it Iterator) {
		for _, msg := range msgs {
			var env, err = it.Next()
			assert.NoError(t, err)
			assert.Equal(t, &msg, env.Message)
		}
		var _, err = it.Next()
		assert.Equal(t, io.EOF, err)
	}

	<-aa.Done()
	var req = pb.ReadRequest{Journal: spec.Name, EndOffset: aa.Response().Commit.End}

	// Expect a ReadUncommittedIter reads all message fixtures, in order.
	verify(allMessages, NewReadUncommittedIter(
		client.NewRetryReader(context.Background(), bk.Client(), req), newTestMsg))

	// Expect a ReadCommittedIter reads only |B|'s messages.
	verify([]testMsg{allMessages[1], allMessages[3], allMessages[4]}, NewReadCommittedIter(
		client.NewRetryReader(context.Background(), bk.Client(), req), newTestMsg, seq))

	bk.Tasks.Cancel()
	assert.NoError(t, bk.Tasks.Wait())
}

func TestReadIteratorInitErrors(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var bk = brokertest.NewBroker(t, etcd, "local", "broker")
	brokertest.CreateJournals(t, bk,
		brokertest.Journal(pb.JournalSpec{Name: "missing/content-type"}))

	var _, err = NewReadUncommittedIter(
		client.NewRetryReader(context.Background(), bk.Client(), pb.ReadRequest{
			Journal: "does/not/exist",
		}), newTestMsg).Next()
	assert.EqualError(t, err, "fetching journal spec: named journal does not exist (does/not/exist)")

	_, err = NewReadUncommittedIter(
		client.NewRetryReader(context.Background(), bk.Client(), pb.ReadRequest{
			Journal: "missing/content-type",
		}), newTestMsg).Next()
	assert.EqualError(t, err, "determining framing: unrecognized content-type ()")

	bk.Tasks.Cancel()
	assert.NoError(t, bk.Tasks.Wait())
}

func TestReadUncommittedIterReadErrorCases(t *testing.T) {
	var broker = teststub.NewBroker(t)
	defer broker.Cleanup()

	var ctx = context.Background()
	var spec = &pb.JournalSpec{Name: "a/journal"}

	// Case: a multi-message sequence with interleaved garbage frames,
	// and a graceful EOF at requested EndOffset on a message boundary.
	go func() {
		_ = <-broker.ReadReqCh // Read request.

		// Send a message (14 bytes) and garbage frame (8 bytes).
		broker.ReadRespCh <- pb.ReadResponse{
			Status:    pb.Status_OK,
			Offset:    1000,
			WriteHead: 9999,
			Fragment: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            1000,
				End:              1000 + 14,
				CompressionCodec: pb.CompressionCodec_NONE,
			},
		}
		broker.ReadRespCh <- pb.ReadResponse{
			Offset:  1000,
			Content: []byte(`{"Str":"one"}` + "\ngarbage\n"),
		}

		// Send the next fragment many times, which results in lots of
		// zero-byte Reads being returned to the bufio.Reader, and an
		// eventual io.ErrNoProgress. Expect it's handled.
		for i := 0; i != 100; i++ {
			broker.ReadRespCh <- pb.ReadResponse{
				Offset:    1000 + 14 + 8,
				WriteHead: 9999,
				Fragment: &pb.Fragment{
					Journal:          "a/journal",
					Begin:            1000 + 14 + 8,
					End:              1000 + 14 + 8 + 14 + 8,
					CompressionCodec: pb.CompressionCodec_NONE,
				},
			}
		}
		// Send another message (14 bytes) and garbage frame (8 bytes).
		broker.ReadRespCh <- pb.ReadResponse{
			Offset:  1000 + 14 + 8,
			Content: []byte(`{"Str":"two"}` + "\ngarbage\n"),
		}
		broker.WriteLoopErrCh <- nil // EOF.
	}()

	var rr = client.NewRetryReader(ctx, broker.Client(), pb.ReadRequest{
		Journal:   "a/journal",
		Offset:    1,
		EndOffset: 1000 + 2*14 + 2*8,
	})
	var r = NewReadUncommittedIter(rr, newTestMsg)
	r.spec, r.framing = spec, JSONFraming // Set fixtures without running init().

	var env, err = r.Next()
	assert.NoError(t, err)
	assert.Equal(t, Envelope{
		Journal: spec,
		Begin:   1000, // Jumps from offset of 1.
		End:     1000 + 14,
		Message: &testMsg{Str: "one"},
	}, env)

	env, err = r.Next()
	assert.NoError(t, err)
	assert.Equal(t, Envelope{
		Journal: spec,
		Begin:   1000 + 14 + 8,
		End:     1000 + 14 + 8 + 14,
		Message: &testMsg{Str: "two"},
	}, env)

	_, err = r.Next()
	assert.Equal(t, io.EOF, err)

	// Case: EndOffset is met but is not on frame boundary.
	go func() {
		_ = <-broker.ReadReqCh // Read request.

		// Send a message (14 bytes) and garbage frame (8 bytes).
		broker.ReadRespCh <- pb.ReadResponse{
			Status:    pb.Status_OK,
			Offset:    1000,
			WriteHead: 9999,
			Fragment: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            1000,
				End:              2000,
				CompressionCodec: pb.CompressionCodec_NONE,
			},
		}
		broker.ReadRespCh <- pb.ReadResponse{
			Offset:  1000,
			Content: []byte("no newline"), // 10 bytes.
		}
		broker.WriteLoopErrCh <- nil // EOF.
	}()

	rr = client.NewRetryReader(ctx, broker.Client(), pb.ReadRequest{
		Journal:   "a/journal",
		Offset:    1000,
		EndOffset: 1000 + 10,
	})

	r = NewReadUncommittedIter(rr, newTestMsg)
	r.spec, r.framing = spec, JSONFraming
	_, err = r.Next()
	assert.EqualError(t, err, "framing.Unpack(offset 1000): unexpected EOF")
}
