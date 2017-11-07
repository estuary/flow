// counter is a consumer plugin (eg, should be built with
// `go build --buildmode=plugin`).
package main

import (
	"strconv"

	"github.com/LiveRamp/gazette/consumer"
	"github.com/LiveRamp/gazette/examples/word-count"
	"github.com/LiveRamp/gazette/topic"
)

type counter struct{}

func (counter) Topics() []*topic.Description {
	return []*topic.Description{word_count.Deltas}
}

type shardCache struct {
	pendingCounts map[string]int
}

func (counter) InitShard(s consumer.Shard) error {
	s.SetCache(&shardCache{
		pendingCounts: make(map[string]int),
	})
	return nil
}

func (counter) Consume(env topic.Envelope, s consumer.Shard, pub *topic.Publisher) error {
	var cache = s.Cache().(*shardCache)
	var record = env.Message.(*word_count.Record)

	var count, ok = cache.pendingCounts[record.Word]
	if !ok {
		// Fill from database.
		if b, err := s.Database().GetBytes(s.ReadOptions(), []byte(record.Word)); err != nil {
			return err
		} else if len(b) == 0 {
			// Miss. |count| is already zero.
		} else if count, err = strconv.Atoi(string(b)); err != nil {
			return err
		}
	}

	cache.pendingCounts[record.Word] = count + record.Count
	return nil
}

func (counter) Flush(s consumer.Shard, pub *topic.Publisher) error {
	var cache = s.Cache().(*shardCache)
	var writeBatch = s.Transaction()

	for word, count := range cache.pendingCounts {
		writeBatch.Put([]byte(word), []byte(strconv.AppendInt(nil, int64(count), 10)))

		// Publish the updated word count to the output topic.
		if err := pub.Publish(&word_count.Record{Word: word, Count: count}, word_count.Counts); err != nil {
			return err
		}
		delete(cache.pendingCounts, word) // Reset for next transaction.
	}
	return nil
}

func main() {} // Not called.
var Consumer consumer.Consumer = counter{}
