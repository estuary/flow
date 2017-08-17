package consumer

import (
	"sync"

	log "github.com/Sirupsen/logrus"
)

// ShardIndex tracks Shard instances by ShardID. It provides for acquisition
// of a Shard instance by ID, and management of Shard tear-down by maintaining
// reference counts of currently-acquired Shards. This is useful for building
// APIs which query against consumer databases.
type ShardIndex struct {
	shards   map[ShardID]*shardIndexEntry
	shardsMu sync.Mutex
}

type shardIndexEntry struct {
	Shard
	sync.WaitGroup
}

// RegisterWithRunner registers the ShardIndex to watch the Runner,
// indexing the live set of mastered Shards.
func (i *ShardIndex) RegisterWithRunner(r *Runner) {
	r.ShardPreInitHook = i.IndexShard
	r.ShardPostStopHook = i.DeindexShard
}

// AcquireShard queries for a live, mastered Shard of |id|. If found, the
// returned |shard| is locked from tear-down (eg, due to membership change)
// and must be released via ReleaseShard.
func (i *ShardIndex) AcquireShard(id ShardID) (shard Shard, ok bool) {
	i.shardsMu.Lock()
	defer i.shardsMu.Unlock()

	var e *shardIndexEntry
	if e, ok = i.shards[id]; !ok || e.Shard == nil {
		return nil, false
	}

	e.WaitGroup.Add(1)
	return e.Shard, true
}

// ReleaseShard releases a previously obtained Shard, allowing tear-down to
// occur if the Shard membership status has changed and all references have
// been released.
func (i *ShardIndex) ReleaseShard(shard Shard) {
	i.shardsMu.Lock()
	defer i.shardsMu.Unlock()

	var e, ok = i.shards[shard.ID()]
	if !ok {
		log.WithField("shard", shard.ID()).Panic("unknown shard")
	}

	e.WaitGroup.Done()
}

// IndexShard adds |shard| to the index. shard.ID() must not already be indexed.
// IndexShard is exported to facilitate testing, but clients should generally
// use RegisterWithRunner and not call IndexShard directly.
func (i *ShardIndex) IndexShard(shard Shard) {
	i.shardsMu.Lock()
	defer i.shardsMu.Unlock()

	if i.shards == nil {
		i.shards = make(map[ShardID]*shardIndexEntry)
	}
	if e, ok := i.shards[shard.ID()]; ok && e.Shard != nil {
		log.WithField("shard", shard.ID()).Panic("duplicate shard")
	}
	i.shards[shard.ID()] = &shardIndexEntry{Shard: shard}
}

// DeindexShard drops |shard| from the index, blocking until all obtained
// references have been released. shard.ID() must be indexed. DeindexShard is
// exported to facilitate testing, but clients should generally use
// RegisterWithRunner and not call DeindexShard directly.
func (i *ShardIndex) DeindexShard(shard Shard) {
	i.deindexShardNonblocking(shard).WaitGroup.Wait()
}

// TODO(johnny): Remove these. They're deprecated names which are being
// supported for the moment to avoid unrelated churning in the patch set.
func (i *ShardIndex) AddShard(shard Shard)    { i.IndexShard(shard) }
func (i *ShardIndex) RemoveShard(shard Shard) { i.DeindexShard(shard) }

func (i *ShardIndex) deindexShardNonblocking(shard Shard) *shardIndexEntry {
	i.shardsMu.Lock()
	defer i.shardsMu.Unlock()

	var e, ok = i.shards[shard.ID()]
	if !ok || e.Shard == nil {
		log.WithField("shard", shard.ID()).Panic("unknown shard")
	}

	// nil to prevent further references being taken.
	e.Shard = nil
	return e
}
