package broker

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestAppendFlowCallbackCases(t *testing.T) {
	defer func(v int64) { MinAppendRate = v }(MinAppendRate)
	MinAppendRate = 1e3 // 1 byte per milli.

	var spec = pb.JournalSpec{MaxAppendRate: 1e4} // 10 bytes per milli.

	var expect = func(a, b int64) {
		assert.Equal(t, a, b)
	}

	t.Run("over-then-underflow", func(t *testing.T) {
		var fc appendFlowControl
		fc.reset(&resolution{journalSpec: &spec}, 12345)

		// |fc| is re-initialized.
		expect(1e4, fc.maxRate)
		expect(1e4, fc.balance)
		expect(1e3, fc.minRate)
		expect(1e3, fc.spent)
		expect(0, fc.charge)

		// First chunk proceeds.
		fc.onChunk(1e4 - 20)
		expect(20, fc.balance)
		expect(1e3, fc.spent)
		expect(0, fc.charge)

		// Second chunk must wait for more budget to accrue.
		fc.onChunk(40)
		expect(0, fc.balance)
		expect(1e3, fc.spent)
		expect(20, fc.charge)

		// A tick is applied, but still not enough budget.
		assert.NoError(t, fc.onTick(fc.lastMillis+1))
		expect(0, fc.balance)
		expect(1e3, fc.spent)
		expect(10, fc.charge)

		// A very very large tick is applied, perhaps because the ticker was
		// delayed. Expect that remaining charge is applied _after_ accruing
		// budget updates. Specifically, we don't want to underflow in this case
		// because we didn't have an opportunity to read another chunk (we were
		// waiting on the delayed ticker to discharge the last one).
		assert.NoError(t, fc.onTick(fc.lastMillis+100000))
		expect(1e4-10, fc.balance)
		expect(10, fc.spent)
		expect(0, fc.charge)

		// A small chunk proceeds immediately.
		fc.onChunk(40)
		expect(1e4-50, fc.balance)
		expect(50, fc.spent)
		expect(0, fc.charge)

		// Another modest tick. This time, we do underflow.
		assert.Equal(t, ErrFlowControlUnderflow, fc.onTick(fc.lastMillis+51))
		expect(1e4, fc.balance)
		expect(0, fc.spent)
		expect(0, fc.charge)

		expect(3, fc.totalChunks)
		expect(1, fc.delayedChunks)
	})

	t.Run("one-chunk-many-ticks", func(t *testing.T) {
		var fc appendFlowControl
		fc.reset(&resolution{
			journalSpec: &pb.JournalSpec{MaxAppendRate: MinAppendRate},
		}, 12345)

		fc.onChunk(MinAppendRate * 11)

		for i := int64(0); i != 10; i++ {
			expect(0, fc.balance)
			expect(MinAppendRate, fc.spent)
			expect(MinAppendRate*(10-i), fc.charge)
			assert.NoError(t, fc.onTick(fc.lastMillis+1000))
		}
		expect(0, fc.balance)
		expect(MinAppendRate, fc.spent)
		expect(0, fc.charge) // Chunk is permitted to proceed.

		expect(1, fc.totalChunks)
		expect(1, fc.delayedChunks)
	})

	t.Run("balance-carries-forward", func(t *testing.T) {
		var fc appendFlowControl
		fc.reset(&resolution{journalSpec: &spec}, 12345)
		expect(1e4, fc.balance)

		// Chunks, followed by tick, then chunk.
		fc.onChunk(5000)
		expect(5000, fc.balance)
		expect(1e3, fc.spent)
		expect(0, fc.charge)

		assert.NoError(t, fc.onTick(fc.lastMillis+100))
		expect(6000, fc.balance)
		expect(1e3-100, fc.spent)

		fc.onChunk(5000)
		expect(1000, fc.balance)
		expect(1e3, fc.spent)
		expect(0, fc.charge)

		expect(2, fc.totalChunks)
		expect(0, fc.delayedChunks)

		// RPC completes, and next one starts a bit later.
		fc.reset(&resolution{journalSpec: &spec}, fc.lastMillis+150)
		expect(2500, fc.balance) // Balance was carried forward & updated.
		expect(0, fc.totalChunks)
	})

	t.Run("invalidate-then-chunk", func(t *testing.T) {
		var fc appendFlowControl
		fc.reset(&resolution{journalSpec: &spec}, 12345)
		expect(1e4, fc.balance)

		fc.onInvalidate()
		expect(0, fc.maxRate)

		// Chunk which is larger than balance is allowed immediately.
		fc.onChunk(1e5)
		expect(0, fc.balance)
		expect(1e3, fc.spent)
		expect(0, fc.charge)
	})

	t.Run("chunk-then-invalidate", func(t *testing.T) {
		var fc appendFlowControl
		fc.reset(&resolution{journalSpec: &spec}, 12345)
		expect(1e4, fc.balance)

		fc.onChunk(1e5)
		expect(0, fc.balance)
		expect(1e3, fc.spent)
		expect(1e5-1e4, fc.charge)

		fc.onInvalidate()
		expect(0, fc.maxRate)
		expect(0, fc.balance)
		expect(0, fc.charge)
	})

	t.Run("zero-max-rate", func(t *testing.T) {
		var fc appendFlowControl
		fc.reset(&resolution{journalSpec: new(pb.JournalSpec)}, 12345)

		expect(0, fc.maxRate)
		expect(0, fc.balance)
		expect(1e3, fc.spent)

		// Ticks still decrement |spent|.
		assert.NoError(t, fc.onTick(fc.lastMillis+500))
		expect(0, fc.balance)
		expect(500, fc.spent)

		// Chunks always proceed.
		fc.onChunk(1e5)
		expect(0, fc.balance)
		expect(1e3, fc.spent)
		expect(0, fc.charge)

		// We can still underflow.
		assert.Equal(t, ErrFlowControlUnderflow, fc.onTick(fc.lastMillis+1e3))
		expect(0, fc.spent)
	})

	t.Run("flagged-max-rate-bounds-journal-spec", func(t *testing.T) {
		defer func(v int64) { MaxAppendRate = v }(MaxAppendRate)
		MaxAppendRate = 1e10

		var fc appendFlowControl
		fc.reset(&resolution{journalSpec: &pb.JournalSpec{MaxAppendRate: 1e15}}, 12345)
		expect(1e10, fc.maxRate)
	})
}

func TestAppendFlowRecvCases(t *testing.T) {
	defer func(v int64) { MinAppendRate = v }(MinAppendRate)
	MinAppendRate = 1e2 // 0.1 byte per milli.

	var millis int64 = 12345
	var tickCh = make(chan time.Time, 8)
	var invalidateCh = make(chan struct{})
	var fc appendFlowControl

	fc.reset(&resolution{
		journalSpec:  &pb.JournalSpec{MaxAppendRate: 1e3}, // 1 byte per milli.
		invalidateCh: invalidateCh,
	}, millis)
	fc.ticker = &time.Ticker{C: tickCh}

	var sendChunk = func(size int) {
		fc.chunkCh <- appendChunk{
			req: &pb.AppendRequest{Content: bytes.Repeat([]byte("x"), size)},
		}
	}
	var sendTick = func(delta int64) {
		millis += delta
		tickCh <- time.Unix(0, millis*1e6)
	}
	var expect = func(a, b int64) {
		assert.Equal(t, a, b)
	}

	// Case: chunk proceeds immediately.
	sendChunk(10)

	var req, err = fc.recv()
	assert.NoError(t, err)
	assert.Len(t, req.Content, 10)
	expect(1e3-10, fc.balance)

	// Case: chunk must wait for ticks.
	sendChunk(1e3)
	sendTick(2)
	sendTick(8)

	req, err = fc.recv()
	assert.NoError(t, err)
	assert.Len(t, req.Content, 1e3)
	expect(0, fc.balance)
	expect(millis, fc.lastMillis)

	// Case: error is returned immediately.
	fc.chunkCh <- appendChunk{err: errors.New("foobar")}
	req, err = fc.recv()
	assert.EqualError(t, err, "foobar")

	// Case: underflow.
	sendTick(1e3)
	req, err = fc.recv()
	assert.Equal(t, ErrFlowControlUnderflow, err)

	// Case: large chunk is returned immediately after route invalidation.
	sendChunk(1e3 + 1)
	close(invalidateCh)
	req, err = fc.recv()
	assert.Len(t, req.Content, 1e3+1)
}

func installAppendTimeoutFixture() (uninstall func()) {
	var a, b = flowControlBurstFactor, flowControlQuantum
	flowControlBurstFactor = time.Microsecond
	flowControlQuantum = time.Microsecond

	return func() {
		flowControlBurstFactor = a
		flowControlQuantum = b
	}
}
