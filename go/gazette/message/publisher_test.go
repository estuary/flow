package message

import (
	"context"
	"io"
	"testing"

	"github.com/pkg/errors"
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

	// Publish some messages that map to |spec|.
	var mapping = func(Mappable) (pb.Journal, string, error) {
		return spec.Name, labels.ContentType_JSONLines, nil
	}
	for _, s := range []string{"hello", "world", "beer!"} {
		var aa, err = pub.PublishCommitted(mapping, &testMsg{Str: s})
		require.NoError(t, err)
		require.NoError(t, aa.Err())
	}

	require.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 1, Flag_OUTSIDE_TXN), Str: "hello"},
		{UUID: BuildUUID(pub.producer, 2, Flag_OUTSIDE_TXN), Str: "world"},
		{UUID: BuildUUID(pub.producer, 3, Flag_OUTSIDE_TXN), Str: "beer!"},
	}, readAllMsgs(t, bk, spec))

	// Expect validation errors are returned, before the mapping is invoked.
	var _, err = pub.PublishCommitted(nil, &testMsg{err: errors.New("whoops!")})
	require.EqualError(t, err, "whoops!")

	bk.Tasks.Cancel()
	require.NoError(t, bk.Tasks.Wait())
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
		var mapping = func(Mappable) (pb.Journal, string, error) {
			return journal, labels.ContentType_JSONLines, nil
		}
		var aa, err = pub.PublishUncommitted(mapping, &testMsg{Str: value})
		require.NoError(t, err)
		require.NoError(t, aa.Err())
	}

	// Publish two pending messages.
	publish(spec1.Name, "value one")
	publish(spec2.Name, "value two")

	// Build intents. Expect ACK intents for each published journal.
	var intents, err = pub.BuildAckIntents()
	require.NoError(t, err)
	require.Len(t, intents, 2)
	writeIntents(t, ajc, intents)

	// Publish again, then build & write intents.
	publish(spec3.Name, "value three")
	publish(spec1.Name, "value four")

	intents, err = pub.BuildAckIntents()
	require.NoError(t, err)
	require.Len(t, intents, 2)
	writeIntents(t, ajc, intents)

	// Verify expected messages and ACKs were written to journals.
	require.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 1, Flag_CONTINUE_TXN), Str: "value one"},
		{UUID: BuildUUID(pub.producer, 3, Flag_ACK_TXN)},
		{UUID: BuildUUID(pub.producer, 6, Flag_CONTINUE_TXN), Str: "value four"},
		{UUID: BuildUUID(pub.producer, 8, Flag_ACK_TXN)},
	}, readAllMsgs(t, bk, spec1))

	require.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 2, Flag_CONTINUE_TXN), Str: "value two"},
		{UUID: BuildUUID(pub.producer, 4, Flag_ACK_TXN)},
	}, readAllMsgs(t, bk, spec2))

	require.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 5, Flag_CONTINUE_TXN), Str: "value three"},
		{UUID: BuildUUID(pub.producer, 7, Flag_ACK_TXN)},
	}, readAllMsgs(t, bk, spec3))

	bk.Tasks.Cancel()
	require.NoError(t, bk.Tasks.Wait())
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
	var seq = NewSequencer(nil, nil, 3)

	var seqPump = func() (out []testMsg) {
		var env, err = r.Next()
		require.NoError(t, err)

		if seq.QueueUncommitted(env) == QueueAckCommitReplay {
			var journal, from, to = seq.ReplayRange()
			var rr = client.NewRetryReader(ctx, bk.Client(), pb.ReadRequest{
				Journal:   journal,
				Offset:    from,
				EndOffset: to,
			})
			seq.StartReplay(NewReadUncommittedIter(rr, newTestMsg))
		}
		for {
			if err := seq.Step(); err == io.EOF {
				return
			}
			require.NoError(t, err)
			out = append(out, *seq.Dequeued.Message.(*testMsg))
		}
	}

	var mapping = func(Mappable) (pb.Journal, string, error) {
		return spec.Name, labels.ContentType_JSONLines, nil
	}
	var pub = NewPublisher(ajc, &clock)
	var txnPub = NewPublisher(ajc, &clock)

	// |txnPub| writes first, followed by |pub|, followed by |txnPub| intents.
	// All messages are served from the Sequencer ring.
	_, _ = txnPub.PublishUncommitted(mapping, &testMsg{Str: "one"})
	_, _ = pub.PublishCommitted(mapping, &testMsg{Str: "two"})

	var intents, _ = txnPub.BuildAckIntents()
	writeIntents(t, ajc, intents)

	require.Equal(t, []testMsg(nil), seqPump())
	require.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 2, Flag_OUTSIDE_TXN), Str: "two"},
	}, seqPump())
	require.Equal(t, []testMsg{
		{UUID: BuildUUID(txnPub.producer, 1, Flag_CONTINUE_TXN), Str: "one"},
		{UUID: BuildUUID(txnPub.producer, 3, Flag_ACK_TXN)},
	}, seqPump())

	// Write more interleaved messages, enough to overflow the Sequencer ring.
	_, _ = txnPub.PublishUncommitted(mapping, &testMsg{Str: "three"})
	_, _ = pub.PublishCommitted(mapping, &testMsg{Str: "four"})
	_, _ = txnPub.PublishUncommitted(mapping, &testMsg{Str: "five"})
	_, _ = pub.PublishCommitted(mapping, &testMsg{Str: "six"})
	_, _ = txnPub.PublishUncommitted(mapping, &testMsg{Str: "seven"})

	intents, _ = txnPub.BuildAckIntents()
	writeIntents(t, ajc, intents)

	require.Equal(t, []testMsg(nil), seqPump())
	require.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 5, Flag_OUTSIDE_TXN), Str: "four"},
	}, seqPump())
	require.Equal(t, []testMsg(nil), seqPump())
	require.Equal(t, []testMsg{
		{UUID: BuildUUID(pub.producer, 7, Flag_OUTSIDE_TXN), Str: "six"},
	}, seqPump())
	require.Equal(t, []testMsg(nil), seqPump())
	require.Equal(t, []testMsg{
		{UUID: BuildUUID(txnPub.producer, 4, Flag_CONTINUE_TXN), Str: "three"},
		{UUID: BuildUUID(txnPub.producer, 6, Flag_CONTINUE_TXN), Str: "five"},
		{UUID: BuildUUID(txnPub.producer, 8, Flag_CONTINUE_TXN), Str: "seven"},
		{UUID: BuildUUID(txnPub.producer, 9, Flag_ACK_TXN)},
	}, seqPump())

	bk.Tasks.Cancel()
	require.NoError(t, bk.Tasks.Wait())
}

func TestDeferPublishUncommitted(t *testing.T) {
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

	var seq = NewSequencer(nil, nil, 5)

	var seqPump = func() (out []testMsg) {
		var env, err = r.Next()
		require.NoError(t, err)

		if seq.QueueUncommitted(env) == QueueAckCommitReplay {
			// The sequencer buffer is large enough that we should never need to replay for this
			// test.
			panic("unexpected need to replay")
		}
		for {
			if err := seq.Step(); err == io.EOF {
				return
			}
			require.NoError(t, err)
			out = append(out, *seq.Dequeued.Message.(*testMsg))
		}
	}

	var mapping = func(Mappable) (pb.Journal, string, error) {
		return spec.Name, labels.ContentType_JSONLines, nil
	}
	var pub = NewPublisher(ajc, &clock)

	// Happy path: An uncommitted message can be written before a deferred one, and should get
	// sequenced normally with respect to the deferred message, since the deferred publish is
	// started after.
	var _, err = pub.PublishUncommitted(mapping, &testMsg{Str: "one"})
	require.NoError(t, err)
	require.Equal(t, []testMsg(nil), seqPump())

	fut, err := pub.DeferPublishUncommitted(spec.Name, labels.ContentType_JSONLines, new(testMsg))
	require.NoError(t, err)

	intents, err := pub.BuildAckIntents()
	require.NoError(t, err)

	require.NoError(t, fut.Resolve(&testMsg{Str: "two"}))
	require.Equal(t, []testMsg(nil), seqPump())

	writeIntents(t, ajc, intents)

	var actual = seqPump()
	require.Equal(t, 3, len(actual))
	require.Equal(t, "one", actual[0].Str)
	require.Equal(t, "two", actual[1].Str)
	require.Equal(t, "", actual[2].Str)

	// Sad path cases:
	// The deferred publish message will not be seen because it sequences before "three"
	fut, err = pub.DeferPublishUncommitted(spec.Name, labels.ContentType_JSONLines, new(testMsg))
	require.NoError(t, err)

	_, err = pub.PublishUncommitted(mapping, &testMsg{Str: "three"})
	require.NoError(t, err)
	require.Equal(t, []testMsg(nil), seqPump())
	intents, err = pub.BuildAckIntents()
	require.NoError(t, err)
	require.NoError(t, fut.Resolve(&testMsg{Str: "wont see four"}))
	require.Equal(t, []testMsg(nil), seqPump())

	writeIntents(t, ajc, intents)
	actual = seqPump()
	require.Equal(t, 2, len(actual))
	require.Equal(t, "three", actual[0].Str)
	require.Equal(t, "", actual[1].Str)

	// The deferred publish isn't resolved until after the acks were written, so will not be seen.
	_, err = pub.PublishUncommitted(mapping, &testMsg{Str: "five"})
	require.NoError(t, err)
	require.Equal(t, []testMsg(nil), seqPump())

	fut, err = pub.DeferPublishUncommitted(spec.Name, labels.ContentType_JSONLines, new(testMsg))
	require.NoError(t, err)

	intents, err = pub.BuildAckIntents()
	require.NoError(t, err)
	writeIntents(t, ajc, intents)

	actual = seqPump()
	require.Equal(t, 2, len(actual))
	require.Equal(t, "five", actual[0].Str)
	require.Equal(t, "", actual[1].Str)

	require.NoError(t, fut.Resolve(&testMsg{Str: "wont see six"}))
	require.Equal(t, []testMsg(nil), seqPump())

	_, err = pub.PublishCommitted(mapping, &testMsg{Str: "seven"})
	require.NoError(t, err)
	actual = seqPump()
	require.Equal(t, 1, len(actual))
	require.Equal(t, "seven", actual[0].Str)
}

func readAllMsgs(t require.TestingT, bk *brokertest.Broker, spec *pb.JournalSpec) (out []testMsg) {
	var rr = client.NewRetryReader(context.Background(), bk.Client(), pb.ReadRequest{Journal: spec.Name})
	var r = NewReadUncommittedIter(rr, newTestMsg)
	for {
		var env, err = r.Next()
		if errors.Cause(err) == client.ErrOffsetNotYetAvailable {
			return out
		}
		require.NoError(t, err)
		out = append(out, *env.Message.(*testMsg))
	}
}

func newTestMsgSpec(name pb.Journal) *pb.JournalSpec {
	return brokertest.Journal(pb.JournalSpec{
		Name:     name,
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	})
}

func writeIntents(t require.TestingT, ajc client.AsyncJournalClient, intents []AckIntent) {
	for _, i := range intents {
		var aa = ajc.StartAppend(pb.AppendRequest{Journal: i.Journal}, nil)
		_, _ = aa.Writer().Write(i.Intent)
		require.NoError(t, aa.Release())
		<-aa.Done()
	}
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
