package shuffle

import (
	"context"
	"io"

	pf "github.com/estuary/flow/go/protocol"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

type coordinator struct {
	rjc pb.RoutedJournalClient
}

type ring struct {
	*coordinator
	ctx    context.Context
	cancel context.CancelFunc

	subscribers
	readChans []chan pf.ShuffleResponse
}

func (r *ring) onSubscribe(sub subscriber) {
	var rr = r.subscribers.add(sub)
	if rr == nil {
		return // This subscriber doesn't require starting a new read.
	}

	var readCh = make(chan pf.ShuffleResponse, 1)
	r.readChans = append(r.readChans, readCh)
	go readDocuments(r.ctx, r.coordinator.rjc, *rr, readCh)
}

func (r *ring) onRead(resp pf.ShuffleResponse, ok bool) {
	if !ok {
		// Reader at the top of the read stack has reached EOF.
		if len(r.readChans) <= 1 {
			panic("unexpected EOF from shuffle reader at stack bottom")
		}
		r.readChans = r.readChans[:len(r.readChans)-1]
		return
	} else if resp.TerminalError != "" {
		r.subscribers.stageResponses(resp)

	}
}

// readDocuments is a function variable for easy mocking in tests.
var readDocuments = func(
	ctx context.Context,
	rjc pb.RoutedJournalClient,
	req pb.ReadRequest,
	ch chan pf.ShuffleResponse,
) {
	defer close(ch)

	var rr = client.NewRetryReader(ctx, rjc, req)
	var it = message.NewReadUncommittedIter(rr, func(*pb.JournalSpec) (message.Message, error) {
		return new(pf.Document), nil
	})

	for {
		var env, err = it.Next()

		// Attempt to pop a pending ShuffleResponse that we can extend.
		// Or, start a new one if none is buffered.
		var response pf.ShuffleResponse
		select {
		case response = <-ch:
		default:
		}
		var delta int64

		if err == nil {
			var doc = *env.Message.(*pf.Document)
			doc.Begin, doc.End = env.Begin, env.End
			response.Documents = append(response.Documents, doc)
			delta = response.Documents[len(response.Documents)-1].End - response.Documents[0].Begin
		} else if err != io.EOF {
			response.TerminalError = err.Error()
		}

		// Place back onto channel. This cannot block since buffer N=1,
		// we dequeued above, and we're the only writer.
		ch <- response

		if err != nil {
			return
		} else if delta < responseSizeThreshold {
			continue
		}

		// We cannot queue further documents into the |response| that we just placed
		// into the channel. Send a new & empty ShuffleResponse, which will block
		// until the prior |response| is recieved (making room in the N=1 buffer)
		// or until the context is cancelled.
		select {
		case ch <- pf.ShuffleResponse{}:
		case <-ctx.Done():
			return
		}
	}
}

const responseSizeThreshold int64 = 1 << 16 // 65KB.
