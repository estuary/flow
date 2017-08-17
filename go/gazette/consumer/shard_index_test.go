package consumer

import (
	gc "github.com/go-check/check"
)

type ShardIndexSuite struct{}

func (s *ShardIndexSuite) TestRegistration(c *gc.C) {
	var ind ShardIndex
	var runner Runner
	ind.RegisterWithRunner(&runner)

	c.Check(runner.ShardPreInitHook, gc.NotNil)  // equals ind.addShard.
	c.Check(runner.ShardPostStopHook, gc.NotNil) // equals ind.removeShardAndWait.
}

func (s *ShardIndexSuite) TestAcquireReleaseFlow(c *gc.C) {
	var shard = new(MockShard)
	shard.On("ID").Return(ShardID("shard-xyz-000"))

	var ind ShardIndex
	ind.IndexShard(shard)

	// Expect shard can be acquired and released.
	for i := 0; i != 2; i++ {
		var t, ok = ind.AcquireShard(shard.ID())
		c.Check(ok, gc.Equals, true)
		c.Check(t, gc.Equals, shard)
		ind.ReleaseShard(t)
	}

	// No references remain. Shard can be removed without blocking.
	ind.DeindexShard(shard)

	var t, ok = ind.AcquireShard(shard.ID())
	c.Check(ok, gc.Equals, false)
	c.Check(t, gc.IsNil)

	// Re-index and acquire a reference.
	ind.IndexShard(shard)
	t, _ = ind.AcquireShard(shard.ID())
	c.Check(t, gc.Equals, shard)

	// Trigger a removal. Expect the shard can no longer be queried.
	var entry = ind.deindexShardNonblocking(shard)

	t, ok = ind.AcquireShard(shard.ID())
	c.Check(ok, gc.Equals, false)
	c.Check(t, gc.IsNil)

	// Expect that releasing the shard unblocks its WaitGroup.
	go ind.ReleaseShard(shard)
	entry.WaitGroup.Wait()
}

var _ = gc.Suite(&ShardIndexSuite{})
