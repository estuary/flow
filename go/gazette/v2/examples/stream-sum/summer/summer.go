// +build !norocksdb

package main

import (
	"encoding/json"

	"github.com/LiveRamp/gazette/v2/cmd/run-consumer/consumermodule"
	"github.com/LiveRamp/gazette/v2/examples/stream-sum"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	consumermodule.BaseConfig
}

type app struct{}

// shardCache is a cache of partial stream Sums.
type shardCache map[stream_sum.StreamID]stream_sum.Sum

func (app) NewStore(shard consumer.Shard, dir string, rec *recoverylog.Recorder) (consumer.Store, error) {
	var rdb = consumer.NewRocksDBStore(rec, dir)
	rdb.Cache = make(shardCache)
	return rdb, rdb.Open()
}

func (app) NewMessage(*pb.JournalSpec) (message.Message, error) { return new(stream_sum.Chunk), nil }

func (app) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope) error {
	var rdb = store.(*consumer.RocksDBStore)
	var cache = rdb.Cache.(shardCache)
	var chunk = env.Message.(*stream_sum.Chunk)

	var sum, ok = cache[chunk.ID]
	if !ok {
		// Fill from database.
		if b, err := rdb.DB.GetBytes(rdb.ReadOptions, chunk.ID[:]); err != nil {
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
		if _, err := message.Publish(shard.JournalClient(), stream_sum.SumsMapping, &sum); err != nil {
			log.WithFields(log.Fields{"err": err, "id": sum.ID}).Fatal("publishing sum")
		}
	}

	// Retain the partial sum for the next chunk or flush.
	cache[sum.ID] = sum

	return nil
}

func (*app) FinalizeTxn(shard consumer.Shard, store consumer.Store) error {
	var rdb = store.(*consumer.RocksDBStore)
	var cache = rdb.Cache.(shardCache)

	for id, sum := range cache {
		delete(cache, id) // Clear for next transaction.

		if b, err := json.Marshal(sum); err != nil {
			log.WithFields(log.Fields{"err": err}).Fatal("marshalling record")
		} else {
			rdb.WriteBatch.Put(id[:], b)
		}
	}
	return nil
}

type module struct{}

func (module) NewConfig() consumermodule.Config                          { return new(Config) }
func (module) NewApplication(consumermodule.Config) consumer.Application { return new(app) }
func (module) Register(cfg consumermodule.Config, app consumer.Application, sc mainboilerplate.ServerContext, svc *consumer.Service) {
	log.WithFields(log.Fields{
		"cfg": cfg,
		"app": app,
		"sc":  sc,
		"svc": svc,
	}).Info("Register called")
}

func main() {} // Not called.
var Module consumermodule.Module = module{}
