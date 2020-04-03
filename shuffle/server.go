package shuffle

/*
import (
	"context"
	"sync"

	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// Reader foo
type Reader struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// StartReader foo
func readJournal(journal pb.Journal, offset int64, rjc pb.RoutedJournalClient, newMsg message.NewMessageFunc) {
	var ctx, cancel = context.WithCancel(context.Background())

	var it = message.NewReadUncommittedIter(
		client.NewRetryReader(ctx, rjc, pb.ReadRequest{
			Journal:    journal,
			Offset:     offset,
			Block:      true,
			DoNotProxy: !rjc.IsNoopRouter(),
		}), newMsg)

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

// Config configures the behavior of message shuffling.
type Config struct {
	ShuffleKey              message.MappingKeyFunc
	NewMessage              message.NewMessageFunc
	EffectiveShardsForClock func(message.Clock) int
	IndexOfShard            func(protocol.ShardID) int
}

// Server foo
type Server struct {
	cfg Config

	reads map[pb.Journal]*coordinatedJournal
	mu    sync.Mutex
}

type coordinatedJournal struct {
	cfg        Config
	nextOffset int64
	subs       map[int]chan<- EnvelopeOrError
	mu         sync.Mutex
}

// Subscribe implements the Shuffle service.
func (srv *Server) Subscribe(req *ShuffleRequest, stream Shuffle_SubscribeServer) error {
	var cj *coordinatedJournal
	var ok bool
	var ch = make(chan EnvelopeOrError, 32)

	srv.mu.Lock()
	if cj, ok = srv.reads[req.Journal]; !ok {
		cj = startCoordinatedJournalRead()
		srv.reads[req.Journal] = cj
	}

	cj.mu.Lock()
	cj.subs[srv.cfg.IndexOfShard(req.Shard)] = ch
	cj.mu.Unlock()
	srv.mu.Unlock()

	defer func() {
		srv.mu.Lock()
		cj.mu.Lock()

		subs.
			cj.mu.Unlock()
		srv.mu.Unlock()
	}()

	for eoe := range ch {
		if eoe.Err != nil {
			return eoe.Err
		}

	}

}

func (sub *Server) consumeIterator(cfg Config, it message.Iterator) {
	for {
		var env, err = it.Next()

		if err != nil {
			subs.Broadcast(err)
			return
		}

		var uuid = env.Message.GetUUID()
		var clock = message.GetClock(uuid)
		var N = cfg.EffectiveShardsForClock(clock)
		var idx = messageIndex(env.Message, cfg.ShuffleKey, N)
		subs.Send(idx, env)
	}
}
*/
