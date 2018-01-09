// summer is a consumer plugin (eg, should be built with
// `go build --buildmode=plugin`).
package main

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/examples/stream-sum"
	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/topic"
)

type summer struct{}

func (summer) Topics() []*topic.Description {
	return []*topic.Description{stream_sum.Chunks}
}

// shardCache holds partial stream Sums to persist on the next flush,
// and AsyncAppends of published sums which are waiting to finish.
type shardCache struct {
	pending map[stream_sum.StreamID]stream_sum.Sum
	appends []*journal.AsyncAppend
}

func (summer) InitShard(s consumer.Shard) error {
	s.SetCache(&shardCache{pending: make(map[stream_sum.StreamID]stream_sum.Sum)})
	return nil
}

func (summer) Consume(env topic.Envelope, s consumer.Shard, pub *topic.Publisher) error {
	var cache = s.Cache().(*shardCache)
	var chunk = env.Message.(*stream_sum.Chunk)

	var sum, ok = cache.pending[chunk.ID]
	if !ok {
		// Fill from database.
		if b, err := s.Database().GetBytes(s.ReadOptions(), chunk.ID[:]); err != nil {
			log.WithFields(log.Fields{"err": err, "id": chunk.ID}).Fatal("reading db")
		} else if len(b) == 0 {
			// Miss. Initialize a new stream.
			sum.ID = chunk.ID
		} else if err = json.Unmarshal(b, &sum); err != nil {
			log.WithFields(log.Fields{"err": err, "id": chunk.ID}).Fatal("unmarshalling record")
		}
	}

	if done, err := sum.Update(*chunk); err != nil {
		log.WithFields(log.Fields{"err": err, "id": chunk.ID}).Fatal("updating record")
	} else if done {
		// Publish completed sum.
		if aa, err := pub.Publish(&sum, stream_sum.Sums); err != nil {
			log.WithFields(log.Fields{"err": err, "id": sum.ID}).Fatal("publishing sum")
		} else {
			cache.appends = append(cache.appends, aa)
		}
	}

	// Retain the partial sum for the next chunk or flush.
	cache.pending[sum.ID] = sum

	return nil
}

func (summer) Flush(s consumer.Shard, pub *topic.Publisher) error {
	var cache = s.Cache().(*shardCache)
	var writeBatch = s.Transaction()

	// Block until all sums published during this transaction have completed.
	for _, aa := range cache.appends {
		<-aa.Ready
	}
	cache.appends = cache.appends[:0]

	for id, sum := range cache.pending {
		delete(cache.pending, id) // Clear for next transaction.

		if b, err := json.Marshal(sum); err != nil {
			log.WithFields(log.Fields{"err": err}).Fatal("marshalling record")
		} else {
			writeBatch.Put(id[:], b)
		}
	}
	return nil
}

func main() {} // Not called.
var Consumer consumer.Consumer = summer{}
