package journal

import (
	"bytes"
	"errors"
	"testing/iotest"

	gc "github.com/go-check/check"
)

type BrokerSuite struct {
	broker *Broker

	appendResults chan AppendResult

	replicateOps chan ReplicateOp
	committed    chan struct{}
	replicator   [3]*testReplicator
}

func (s *BrokerSuite) SetUpTest(c *gc.C) {
	s.broker = NewBroker("a/journal")

	// Initial append operation fixtures.
	s.appendResults = make(chan AppendResult)
	s.broker.Append(AppendOp{
		AppendArgs: AppendArgs{
			Content: bytes.NewBufferString("write one "),
		},
		Result: s.appendResults,
	})
	s.broker.Append(AppendOp{
		AppendArgs: AppendArgs{
			Content: bytes.NewBufferString("write two "),
		},
		Result: s.appendResults,
	})

	// Set up replicas which capture operations and signal on Commit() call.
	var config = BrokerConfig{
		RouteToken: "a-route-token",
		WriteHead:  12345,
	}
	s.replicateOps = make(chan ReplicateOp)
	s.committed = make(chan struct{})
	for i := range s.replicator {
		s.replicator[i] = &testReplicator{
			ops:       s.replicateOps,
			committed: s.committed,
		}
		config.Replicas = append(config.Replicas, s.replicator[i])
	}
	s.broker.UpdateConfig(config)
}

func (s *BrokerSuite) TearDownTest(c *gc.C) {
	s.broker.Stop()
}

func (s *BrokerSuite) serveReplicaWriters(c *gc.C) {
	var ops = [...]ReplicateOp{
		<-s.replicateOps, <-s.replicateOps, <-s.replicateOps}
	for i, op := range ops {
		c.Check(op.Journal, gc.Equals, Name("a/journal"))
		c.Check(op.RouteToken, gc.Equals, RouteToken("a-route-token"))
		c.Check(op.NewSpool, gc.Equals, s.broker.config.writtenSinceRoll == 0)
		c.Check(op.WriteHead, gc.Equals, s.broker.config.WriteHead)

		op.Result <- ReplicateResult{Writer: s.replicator[i]}
	}
	for _ = range s.replicator {
		<-s.committed
	}
}

func (s *BrokerSuite) TestBasicFixture(c *gc.C) {
	s.broker.StartServingOps(12345)
	s.serveReplicaWriters(c)

	for _, r := range s.replicator {
		c.Check(r.commitDelta, gc.Equals, int64(20))
		c.Check(r.buffer.String(), gc.Equals, "write one write two ")
	}
	// Success was returned to both append ops.
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{WriteHead: int64(12365)})
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{WriteHead: int64(12365)})

	c.Check(s.broker.config.WriteHead, gc.Equals, int64(12365))
	c.Check(s.broker.config.writtenSinceRoll, gc.Equals, int64(20))
}

func (s *BrokerSuite) TestSomeCommitErrorsHandling(c *gc.C) {
	s.replicator[1].commitErr = errors.New("error!")
	s.replicator[2].commitErr = errors.New("error!")

	s.broker.StartServingOps(12345)
	s.serveReplicaWriters(c)

	// Both appends were written and attempted to be committed.
	for _, r := range s.replicator {
		c.Check(r.commitDelta, gc.Equals, int64(20))
		c.Check(r.buffer.String(), gc.Equals, "write one write two ")
	}
	// Failure was returned to both append ops.
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{Error: ErrReplicationFailed})
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{Error: ErrReplicationFailed})

	// Write-head still moves forward, since at least one replica committed.
	c.Check(s.broker.config.WriteHead, gc.Equals, int64(12365))
	c.Check(s.broker.config.writtenSinceRoll, gc.Equals, int64(20))
}

func (s *BrokerSuite) TestAllCommitErrorsHandling(c *gc.C) {
	s.replicator[0].commitErr = errors.New("error!")
	s.replicator[1].commitErr = errors.New("error!")
	s.replicator[2].commitErr = errors.New("error!")

	s.broker.StartServingOps(12345)
	s.serveReplicaWriters(c)

	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{Error: ErrReplicationFailed})
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{Error: ErrReplicationFailed})

	// Write-head does not move.
	c.Check(s.broker.config.WriteHead, gc.Equals, int64(12345))
	c.Check(s.broker.config.writtenSinceRoll, gc.Equals, int64(0))
}

func (s *BrokerSuite) TestWriteErrorHandling(c *gc.C) {
	s.replicator[2].writeErr = errors.New("error!")

	s.broker.StartServingOps(12345)
	s.serveReplicaWriters(c)

	// Expect that (just) the first append was written, but aborted.
	for _, r := range s.replicator {
		c.Check(r.commitDelta, gc.Equals, int64(0))
		c.Check(r.buffer.String(), gc.Equals, "write one ")
	}
	// Failure was returned to the first append op.
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{Error: ErrReplicationFailed})

	c.Check(s.broker.config.WriteHead, gc.Equals, int64(12345))
	c.Check(s.broker.config.writtenSinceRoll, gc.Equals, int64(0))

	// Replica recovers. Second append op now succeeds.
	s.replicator[2].writeErr = nil
	s.serveReplicaWriters(c)

	for _, r := range s.replicator {
		c.Check(r.commitDelta, gc.Equals, int64(10)) // Length of second write.
		c.Check(r.buffer.String(), gc.Equals, "write one write two ")
	}
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{WriteHead: int64(12355)})

	c.Check(s.broker.config.WriteHead, gc.Equals, int64(12355))
	c.Check(s.broker.config.writtenSinceRoll, gc.Equals, int64(10))
}

func (s *BrokerSuite) TestBrokenReadHandling(c *gc.C) {
	// Model an append op which reads one byte of data, but then times-out.
	s.broker.Append(AppendOp{
		AppendArgs: AppendArgs{
			Content: iotest.OneByteReader(
				iotest.TimeoutReader(bytes.NewBufferString("!!!!"))),
		},
		Result: s.appendResults,
	})
	// A valid read, which is handled as a separate transaction.
	s.broker.Append(AppendOp{
		AppendArgs: AppendArgs{
			Content: bytes.NewBufferString(" separate"),
		},
		Result: s.appendResults,
	})
	s.broker.StartServingOps(12345)

	var ops = [...]ReplicateOp{
		<-s.replicateOps, <-s.replicateOps, <-s.replicateOps}
	for i, op := range ops {
		op.Result <- ReplicateResult{Writer: s.replicator[i]}
	}
	// Read error was detected and returned to caller.
	c.Check((<-s.appendResults).Error, gc.ErrorMatches, "timeout")

	// Expect that 21 bytes were written, but only 20 were committed.
	for _ = range s.replicator {
		<-s.committed
	}
	for _, r := range s.replicator {
		c.Check(r.commitDelta, gc.Equals, int64(20))
		c.Check(r.buffer.String(), gc.Equals, "write one write two !")
	}
	// Success was returned to the initial append ops.
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{WriteHead: int64(12365)})
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{WriteHead: int64(12365)})

	c.Check(s.broker.config.WriteHead, gc.Equals, int64(12365))
	c.Check(s.broker.config.writtenSinceRoll, gc.Equals, int64(20))

	// Final append is a new transaction.
	s.serveReplicaWriters(c)
	for _, r := range s.replicator {
		c.Check(r.commitDelta, gc.Equals, int64(9))
		c.Check(r.buffer.String(), gc.Equals, "write one write two ! separate")
	}
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{WriteHead: int64(12374)})

	c.Check(s.broker.config.WriteHead, gc.Equals, int64(12374))
	c.Check(s.broker.config.writtenSinceRoll, gc.Equals, int64(29))
}

func (s *BrokerSuite) TestWrongWriteHeadErrorHandling(c *gc.C) {
	s.broker.StartServingOps(12345)

	// One replica returns a write-head error.
	var ops = [...]ReplicateOp{
		<-s.replicateOps, <-s.replicateOps, <-s.replicateOps}
	for i, op := range ops {
		if i == 0 {
			op.Result <- ReplicateResult{
				Error:          ErrWrongWriteHead,
				ErrorWriteHead: 234567,
			}
		} else {
			op.Result <- ReplicateResult{Writer: s.replicator[i]}
		}
	}
	// Expect other replicas recieved an abort-commit.
	for i := range s.replicator {
		if i == 0 {
			continue
		}
		<-s.committed
	}
	for _, r := range s.replicator {
		c.Check(r.commitDelta, gc.Equals, int64(0))
		c.Check(r.buffer.String(), gc.Equals, "")
	}
	// First append op was notified of failure.
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{Error: ErrReplicationFailed})
	// Write head was stepped forward.
	c.Check(s.broker.config.WriteHead, gc.Equals, int64(234567))

	// Second append succeeds, from new write-head.
	s.serveReplicaWriters(c)
	for _, r := range s.replicator {
		c.Check(r.commitDelta, gc.Equals, int64(10))
		c.Check(r.buffer.String(), gc.Equals, "write two ")
	}
	// Second append op is notified of success.
	c.Check(<-s.appendResults, gc.DeepEquals, AppendResult{WriteHead: int64(234577)})

	c.Check(s.broker.config.WriteHead, gc.Equals, int64(234577))
	c.Check(s.broker.config.writtenSinceRoll, gc.Equals, int64(10))
}

type testReplicator struct {
	ops chan ReplicateOp

	writeErr, commitErr error
	buffer              bytes.Buffer
	commitDelta         int64
	committed           chan struct{}
}

func (c *testReplicator) Replicate(op ReplicateOp) {
	c.ops <- op
}

func (c *testReplicator) Write(b []byte) (int, error) {
	c.buffer.Write(b)
	return len(b), c.writeErr
}

func (c *testReplicator) Commit(delta int64) error {
	c.commitDelta = delta
	c.committed <- struct{}{}
	return c.commitErr
}

var _ = gc.Suite(&BrokerSuite{})
