package message

import (
	"context"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/labels"
)

func TestPublishCommitted(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var (
		clock Clock
		spec  = newTestMsgSpec("a/journal")
		bk    = brokertest.NewBroker(t, etcd, "local", "broker")
		ajc   = client.NewAppendService(context.Background(), bk.Client())
		pub   = NewPublisher(ajc, &clock)
	)
	brokertest.CreateJournals(t, bk, spec)

	// Publish some messages that map to |spec| with JSONFraming.
	var mapping = func(Mappable) (pb.Journal, Framing, error) {
		return spec.Name, JSONFraming, nil
	}
	for _, s := range []string{"hello", "world", "beer!"} {
		var aa, err = pub.PublishCommitted(mapping, &testMsg{Str: s})
		assert.NoError(t, err)
		<-aa.Done()
	}

	assert.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 1, Flag_OUTSIDE_TXN), Str: "hello"},
		{UUID: BuildUUID(pub.producer, 2, Flag_OUTSIDE_TXN), Str: "world"},
		{UUID: BuildUUID(pub.producer, 3, Flag_OUTSIDE_TXN), Str: "beer!"},
	}, readAllMsgs(t, bk, spec))

	// Expect validation errors are returned, before the mapping is invoked.
	var _, err = pub.PublishCommitted(nil, &testMsg{err: errors.New("whoops!")})
	assert.EqualError(t, err, "whoops!")

	bk.Tasks.Cancel()
	assert.NoError(t, bk.Tasks.Wait())
}

func TestPublishUncommitted(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var (
		clock Clock
		spec1 = newTestMsgSpec("journal/one")
		spec2 = newTestMsgSpec("journal/two")
		spec3 = newTestMsgSpec("journal/three")

		bk  = brokertest.NewBroker(t, etcd, "local", "broker")
		ajc = client.NewAppendService(context.Background(), bk.Client())
		pub = NewPublisher(ajc, &clock)
	)
	brokertest.CreateJournals(t, bk, spec1, spec2, spec3)

	var publish = func(journal pb.Journal, value string) {
		assert.NoError(t, pub.PublishUncommitted(func(Mappable) (pb.Journal, Framing, error) {
			return journal, JSONFraming, nil
		}, &testMsg{Str: value}))
	}

	// Publish two pending messages.
	publish(spec1.Name, "value one")
	publish(spec2.Name, "value two")

	// Build intents. Expect ACK intents for each published journal.
	var intents, err = pub.BuildAckIntents()
	assert.NoError(t, err)
	assert.Len(t, intents, 2)
	writeIntents(t, ajc, intents)

	// Publish again, then build & write intents.
	publish(spec3.Name, "value three")
	publish(spec1.Name, "value four")

	intents, err = pub.BuildAckIntents()
	assert.NoError(t, err)
	assert.Len(t, intents, 2)
	writeIntents(t, ajc, intents)

	// Verify expected messages and ACKs were written to journals.
	assert.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 1, Flag_CONTINUE_TXN), Str: "value one"},
		{UUID: BuildUUID(pub.producer, 3, Flag_ACK_TXN)},
		{UUID: BuildUUID(pub.producer, 6, Flag_CONTINUE_TXN), Str: "value four"},
		{UUID: BuildUUID(pub.producer, 8, Flag_ACK_TXN)},
	}, readAllMsgs(t, bk, spec1))

	assert.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 2, Flag_CONTINUE_TXN), Str: "value two"},
		{UUID: BuildUUID(pub.producer, 4, Flag_ACK_TXN)},
	}, readAllMsgs(t, bk, spec2))

	assert.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 5, Flag_CONTINUE_TXN), Str: "value three"},
		{UUID: BuildUUID(pub.producer, 7, Flag_ACK_TXN)},
	}, readAllMsgs(t, bk, spec3))

	bk.Tasks.Cancel()
	assert.NoError(t, bk.Tasks.Wait())
}

func TestIntegrationOfPublisherWithSequencerAndReader(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var (
		clock Clock
		ctx   = context.Background()
		spec  = newTestMsgSpec("a/journal")
		bk    = brokertest.NewBroker(t, etcd, "local", "broker")
		ajc   = client.NewAppendService(ctx, bk.Client())
	)
	brokertest.CreateJournals(t, bk, spec)

	// Start a long-lived RetryReader of |spec|.
	var rr = client.NewRetryReader(ctx, bk.Client(), pb.ReadRequest{
		Journal: spec.Name,
		Block:   true,
	})
	var r = NewReadUncommittedIter(rr, newTestMsg)

	// Sequencer |seq| has a very limited look-back window.
	var seq = NewSequencer(nil, 3)

	var seqPump = func() (out []testMsg) {
		var env, err = r.Next()
		assert.NoError(t, err)
		seq.QueueUncommitted(env)

		for {
			var env, err = seq.DequeCommitted()
			if err == io.EOF {
				return
			}
			if err == ErrMustStartReplay {
				var from, to = seq.ReplayRange()
				var rr = client.NewRetryReader(ctx, bk.Client(), pb.ReadRequest{
					Journal:   env.Journal.Name,
					Offset:    from,
					EndOffset: to,
				})
				seq.StartReplay(NewReadUncommittedIter(rr, newTestMsg))
				continue
			}
			require.NoError(t, err)
			out = append(out, *env.Message.(*testMsg))
		}
	}

	var mapping = func(Mappable) (pb.Journal, Framing, error) {
		return spec.Name, JSONFraming, nil
	}
	var pub = NewPublisher(ajc, &clock)
	var txnPub = NewPublisher(ajc, &clock)

	// |txnPub| writes first, followed by |pub|, followed by |txnPub| intents.
	// All messages are served from the Sequencer ring.
	_ = txnPub.PublishUncommitted(mapping, &testMsg{Str: "one"})
	_, _ = pub.PublishCommitted(mapping, &testMsg{Str: "two"})

	var intents, _ = txnPub.BuildAckIntents()
	writeIntents(t, ajc, intents)

	assert.Equal(t, []testMsg(nil), seqPump())
	assert.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 2, Flag_OUTSIDE_TXN), Str: "two"},
	}, seqPump())
	assert.Equal(t, []testMsg{
		{UUID: BuildUUID(txnPub.producer, 1, Flag_CONTINUE_TXN), Str: "one"},
		{UUID: BuildUUID(txnPub.producer, 3, Flag_ACK_TXN)},
	}, seqPump())

	// Write more interleaved messages, enough to overflow the Sequencer ring.
	_ = txnPub.PublishUncommitted(mapping, &testMsg{Str: "three"})
	_, _ = pub.PublishCommitted(mapping, &testMsg{Str: "four"})
	_ = txnPub.PublishUncommitted(mapping, &testMsg{Str: "five"})
	_, _ = pub.PublishCommitted(mapping, &testMsg{Str: "six"})
	_ = txnPub.PublishUncommitted(mapping, &testMsg{Str: "seven"})

	intents, _ = txnPub.BuildAckIntents()
	writeIntents(t, ajc, intents)

	assert.Equal(t, []testMsg(nil), seqPump())
	assert.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 5, Flag_OUTSIDE_TXN), Str: "four"},
	}, seqPump())
	assert.Equal(t, []testMsg(nil), seqPump())
	assert.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 7, Flag_OUTSIDE_TXN), Str: "six"},
	}, seqPump())
	assert.Equal(t, []testMsg(nil), seqPump())
	assert.Equal(t, []testMsg{
		{UUID: BuildUUID(txnPub.producer, 4, Flag_CONTINUE_TXN), Str: "three"},
		{UUID: BuildUUID(txnPub.producer, 6, Flag_CONTINUE_TXN), Str: "five"},
		{UUID: BuildUUID(txnPub.producer, 8, Flag_CONTINUE_TXN), Str: "seven"},
		{UUID: BuildUUID(txnPub.producer, 9, Flag_ACK_TXN)},
	}, seqPump())

	bk.Tasks.Cancel()
	assert.NoError(t, bk.Tasks.Wait())
}

func readAllMsgs(t assert.TestingT, bk *brokertest.Broker, spec *pb.JournalSpec) (out []testMsg) {
	var rr = client.NewRetryReader(context.Background(), bk.Client(), pb.ReadRequest{Journal: spec.Name})
	var r = NewReadUncommittedIter(rr, newTestMsg)
	for {
		var env, err = r.Next()
		if errors.Cause(err) == client.ErrOffsetNotYetAvailable {
			return out
		}
		assert.NoError(t, err)
		out = append(out, *env.Message.(*testMsg))
	}
}

func newTestMsgSpec(name pb.Journal) *pb.JournalSpec {
	return brokertest.Journal(pb.JournalSpec{
		Name:     name,
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	})
}

func writeIntents(t assert.TestingT, ajc client.AsyncJournalClient, intents []AckIntent) {
	for _, i := range intents {
		var aa = ajc.StartAppend(pb.AppendRequest{Journal: i.Journal}, nil)
		_, _ = aa.Writer().Write(i.Intent)
		assert.NoError(t, aa.Release())
		<-aa.Done()
	}
}
