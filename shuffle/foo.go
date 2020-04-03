package shuffle

import (
	"context"

	"go.gazette.dev/core/broker/client"
	grpc "google.golang.org/grpc"
)

// Foo is bar.
type Foo struct {
	client client.ReaderClient
}

type groupRead struct {
	cfg      Config
	rr       *client.RetryReader
	cancelFn context.CancelFunc
	subs     map[int]chan<- EnvelopeOrError
}

func newGroupRead(req ReadRequest, client client.ReaderClient) groupRead {
	var ctx, cancel = context.WithCancel(context.Background())
	var rr = client.NewRetryReader(ctx, client, req.ReadRequest)

	return groupRead{
		cfg:    req.Config,
		rr:     rr,
		cancel: cancel,
	}
}

func (s *Foo) Read(req *ReadRequest, stream Shuffle_ReadServer) error {

}

func (s *Foo) read(req *ReadRequest, stream grpc.ServerStream) error {
	// Start a read.

}

/*
import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// EnvelopeOrError is a message Envelope, or a terminal error encountered while
// trying to read a message Envelope.
type EnvelopeOrError struct {
	message.Envelope
	Err error
}

// Subscriber coordinates shuffled subscriptions of a Shard
// to the dynamic journal set matching the selector.
type Subscriber struct {
	Selector   pb.LabelSelector
	NewMessage message.NewMessageFunc
}

func (sub Subscriber) startReadingMessages(shard consumer.Shard, cp pc.Checkpoint, ch chan<- EnvelopeOrError) {
	var list, err = client.NewPolledList(shard.Context(), shard.JournalClient(), time.Minute, pb.ListRequest{
		Selector: sub.Selector,
	})
	if err != nil {
		ch <- EnvelopeOrError{Err: errors.WithMessage(err, "listing journals")}
		return
	}

	var ss = &shardSubscriber{
		Shard:      shard,
		newMsg:     sub.NewMessage,
		checkpoint: cp,
		ch:         ch,
		list:       list,
	}
	go ss.serviceLoop()
}

type shardSubscriber struct {
	consumer.Shard
	newMsg     message.NewMessageFunc
	checkpoint pc.Checkpoint
	ch         chan<- EnvelopeOrError

	list    *client.PolledList
	readers map[pb.Journal]context.CancelFunc
}

func (ss *shardSubscriber) serviceLoop() {
	// Monitor |list| until context cancel, keeping it in sync with |readers|.
	// Determine offset for started journals.
}

func (c *Subscriber) convergeSubscriptions() {
	// Join live local shards with journals, ensuring there's an active
	// subscription for each. Prune local shards which have been cancelled.
	for i := 0; i != len(c.shards); {

		if c.shards[i].Context().Err() != nil {
			c.shards = append(c.shards[:i], c.shards[i+1:]...) // Prune.
			continue
		}
		for _, j := range c.list.List().Journals {
			var sub = sub{shard: c.shards[i].Spec().Id, journal: j.Spec.Name}

			if _, ok := c.subs[sub]; ok {
				continue // Already started.
			}
			c.subs[sub] = struct{}{}
			go c.startReading(c.shards[i], &j.Spec)
		}
		i++
	}
}

func readOffsetForJournal(s shard, spec *pb.JournalSpec) (int64, error) {
	// Does the shard checkpoint already have an offset?
	if o, ok := s.checkpoint.Sources[spec.Name]; ok {
		return o.ReadThrough, nil
	}

	var bound, err = shardClockBound(s.Spec())
	if err != nil {
		return 0, errors.WithMessagef(err, "parsing clock bound of '%s'", s.Spec().Id)
	}

	list, err := client.ListAllFragments(s.Context(), s.JournalClient(), pb.FragmentsRequest{
		Journal:    spec.Name,
		EndModTime: bound.Unix(),
	})
	if err != nil {
		return 0, errors.WithMessagef(err, "listing fragments of '%s'", spec.Name)
	} else if l := len(list.Fragments); l != 0 {
		return list.Fragments[l-1].Spec.Begin, nil
	} else {
		return 0, nil
	}
}

func (c *Subscriber) startReading(s shard, j *pb.JournalSpec) {
	defer func() { c.subStopCh <- sub{shard: s.Spec().Id, journal: j.Name} }()

	var offset int64
	var v EnvelopeOrError

	offset, v.Err = readOffsetForJournal(s, j)

	var it = message.NewReadUncommittedIter(
		client.NewRetryReader(s.Context(), s.JournalClient(), pb.ReadRequest{
			Journal:    j.Name,
			Offset:     offset,
			Block:      true,
			DoNotProxy: !s.JournalClient().IsNoopRouter(),
		}), c.newMessage)

	for v.Err == nil {
		v.Envelope, v.Err = it.Next()

		// Attempt to place |v| even if context is cancelled,
		// but don't hang if we're cancelled and buffer is full.
		select {
		case s.ch <- v:
		default:
			select {
			case s.ch <- v:
			case <-s.Context().Done():
				return
			}
		}
	}
}

func (c *Subscriber) serviceLoop() {
	for {
		select {
		case s := <-c.shardStartCh:
			c.shards = append(c.shards, s)
		case _ = <-c.list.UpdateCh():
			// Pass.
		case s := <-c.subStopCh:
			delete(c.subs, s)
		}
		c.convergeSubscriptions()
	}
}

func (c *Subscriber) startReadingMessages(s consumer.Shard, cp pc.Checkpoint, ch chan<- EnvelopeOrError) {
	c.shardStartCh <- shard{
		Shard:      s,
		checkpoint: cp,
		ch:         ch,
	}
}

// NewCoordinator constructs a new Coordinator.
func NewCoordinator(state *allocator.State) *Coordinator {
	var c = &Coordinator{
		state: state,
	}

	c.state.KS.Mu.Lock()
	c.state.KS.Observers = append(c.state.KS.Observers, c.updateIndex)
	c.state.KS.Mu.Unlock()

	return c
}

func (c *Coordinator) updateIndex() {

	// For each source journal:
	//  For each disjoint UUID clock space:
	//    Keep a set of shard specs.

	// Given a journal message, which shard should receive it? (Is it me?)
	// Given a journal, which shard should be reading it?
}

*/
