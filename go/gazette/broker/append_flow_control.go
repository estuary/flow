package broker

import (
	"context"
	"errors"
	"time"

	pb "go.gazette.dev/core/broker/protocol"
)

var (
	// MaxAppendRate is the maximum rate at which any journal may be appended to,
	// in bytes per second. It upper-bounds the MaxAppendRate of any particular
	// JournalSpec. If zero, there is no maximum rate.
	MaxAppendRate int64 = 0 // No limit.
	// MinAppendRate is the minimum rate at which any Append RPC client may
	// stream content chunks, in bytes per second. Client RPCs that are unable
	// to sustain this flow rate in any given second are aborted, allowing other
	// blocked RPCs to proceed. MinAppendRate provides baseline protection to
	// limit the impact of slow or faulted clients over the pipeline, which is
	// an exclusively owned and highly contended resource.
	MinAppendRate int64 = 1 << 16 // 64K per second.
	// ErrFlowControlUnderflow is returned if an Append RPC was terminated due to
	// flow control policing. Specifically, the client failed to sustain the
	// MinAppendRate when sending content chunks of the stream.
	ErrFlowControlUnderflow = errors.New(
		"client stream didn't sustain the minimum append data rate")
	// flowControlBurstFactor is the initial "credit" given to the client with
	// respect to Max/MinAppendRate and in units of seconds.
	// One second is selected as a reasonable upper bound of round-trip time
	// between broker and client -- within this timeout, the broker is able to open
	// the flow control window to the client, and the client can start filling that
	// window with new data. Very long lived append streams are still permitted
	// (though not recommended), so long as the client sustains this rate.
	flowControlBurstFactor = time.Second
	// flowControlQuantum is the time quantum with which flow control is evaluated.
	flowControlQuantum = time.Millisecond * 50
)

type appendFlowControl struct {
	maxRate, minRate int64 // Max/min flow rate (in bytes per second).
	balance          int64 // Bytes available to be sent. If this reaches zero, we must throttle.
	spent            int64 // Bytes we have already sent (spent). If this reaches zero, we underflow.
	charge           int64 // Bytes to debit from |balance| before sending the next chunk.
	lastMillis       int64 // Time of last tick which updated balances.
	totalChunks      int64 // Total number of chunks in the session.
	delayedChunks    int64 // Number of delayed chunks in the session.

	ticker       *time.Ticker     // Ticker of flowControlQuantums.
	invalidateCh <-chan struct{}  // Signaled with the resolution is invalidated.
	chunkCh      chan appendChunk // Internally transits chunks read from the RPC stream.
}

// start a flow control session over a single Append RPC stream.
func (fc *appendFlowControl) start(ctx context.Context, res *resolution,
	recv func() (*pb.AppendRequest, error)) func() (*pb.AppendRequest, error) {

	fc.reset(res, timeNow().UnixNano()/1e6)
	fc.ticker = time.NewTicker(flowControlQuantum)

	// Pump calls to |recv| in a goroutine, as they may block indefinitely.
	// We expect that |recv| is tied to |ctx|, which is in turn cancelled upon
	// the return of the Append RPC handler, so these don't leak.
	go func(ch chan<- appendChunk) {
		for {
			var req, err = recv()
			select {
			case ch <- appendChunk{req: req, err: err}:
				// Pass.
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}(fc.chunkCh)

	return fc.recv
}

// reset the appendFlowControl for a new Append RPC with the given resolution.
func (fc *appendFlowControl) reset(res *resolution, nowMillis int64) {
	*fc = appendFlowControl{
		maxRate:    res.journalSpec.MaxAppendRate,
		balance:    fc.balance,    // Keep prior balance.
		lastMillis: fc.lastMillis, // Keep prior lastMillis.

		invalidateCh: res.invalidateCh,
		chunkCh:      make(chan appendChunk, 8),
	}

	// Constrain |maxRate| by the global flag setting.
	if MaxAppendRate != 0 && MaxAppendRate < fc.maxRate {
		fc.maxRate = MaxAppendRate
	}
	// Initialization case? Allow an initial burst of |maxRate| bytes
	if fc.lastMillis == 0 {
		fc.balance = fc.maxRate * int64(flowControlBurstFactor) / int64(time.Second)
		fc.lastMillis = nowMillis
	} else {
		_ = fc.onTick(nowMillis)
	}
	// Allow an initial delay of |MinAppendRate| bytes.
	fc.minRate = MinAppendRate
	fc.spent = MinAppendRate * int64(flowControlBurstFactor) / int64(time.Second)
}

// recv returns the next flow-controlled AppendRequest chunk.
func (fc *appendFlowControl) recv() (*pb.AppendRequest, error) {
	var ch = fc.chunkCh // Nil-able local copy.
	var req *pb.AppendRequest

	for {
		select {
		case chunk := <-ch:
			if chunk.err != nil {
				fc.ticker.Stop()
				return nil, chunk.err
			}

			req, ch = chunk.req, nil // Don't select again if we loop.
			fc.onChunk(int64(len(req.Content)))

		case now := <-fc.ticker.C:
			var millis = now.UnixNano() / 1e6

			if err := fc.onTick(millis); err != nil {
				fc.ticker.Stop()
				return nil, err
			}

		case <-fc.invalidateCh:
			fc.onInvalidate()
		}

		if req != nil && fc.charge == 0 {
			return req, nil
		}
	}
}

// onTick is called with each ticker tick.
func (fc *appendFlowControl) onTick(millis int64) error {
	if millis < fc.lastMillis {
		panic("millis < fc.lastMillis")
	}

	// Add |d| interval bytes to |balance|, capping at |maxRate|.
	var d = fc.maxRate * (millis - fc.lastMillis) / 1e3 // Millis => seconds.
	fc.balance = min64(fc.balance+d, fc.maxRate)

	// Deduct |d| interval bytes from |spent|.
	d = fc.minRate * (millis - fc.lastMillis) / 1e3
	fc.spent -= min64(d, fc.spent)

	fc.lastMillis = millis
	fc.debit()

	if fc.spent <= 0 {
		return ErrFlowControlUnderflow
	}
	return nil
}

// onChunk is called with the length of chunks read from the RPC stream.
func (fc *appendFlowControl) onChunk(length int64) {
	if fc.charge != 0 {
		panic("charge != 0")
	}
	fc.charge = length
	fc.debit()

	if fc.charge != 0 {
		fc.delayedChunks++
	}
	fc.totalChunks++
}

// onInvalidate is called when invalidateCh signals.
func (fc *appendFlowControl) onInvalidate() {
	// Disable |maxRate| flow control for the remainder of this RPC.
	// The rationale is that appends are the means by which route consistency
	// is achieved, and until this append completes the new topology cannot
	// become consistent. We want fast route convergence, and can tolerate
	// spikes above |maxRate| to encourage it.
	fc.maxRate, fc.invalidateCh = 0, nil // Don't select again.
	fc.debit()
}

// debit some or all of |charge| from the available |balance|,
// and add the debited portion to |spent|.
func (fc *appendFlowControl) debit() {
	var d = min64(fc.balance, fc.charge)
	fc.balance -= d
	fc.charge -= d
	fc.spent = min64(fc.spent+d, fc.minRate) // Add |d| bytes to |spent|, capping at |minRate|.

	if fc.maxRate == 0 {
		// |balance| is effectively infinite.
		fc.spent = min64(fc.spent+fc.charge, fc.minRate)
		fc.charge = 0
	}
}

type appendChunk struct {
	req *pb.AppendRequest
	err error
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
