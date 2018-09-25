// Package counter is a consumer plugin (eg, should be built with
// `go build --buildmode=plugin`).
package main

import (
	"context"
	"encoding/binary"

	"github.com/LiveRamp/gazette/v2/cmd/run-consumer/consumermodule"
	"github.com/LiveRamp/gazette/v2/examples/word-count"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	WordCount struct {
		N int `long:"N" description:"Number of grams per N-gram"`
	} `group:"WordCount" namespace:"wordcount"`

	consumermodule.BaseConfig
}

type ngramApp struct {
	relocMapping message.MappingFunc
	ajc          client.AsyncJournalClient
}

// shardCache is a cache of NGram counts
type shardCache map[word_count.NGram]uint64

func (a *ngramApp) NewStore(shard consumer.Shard, dir string, rec *recoverylog.Recorder) (consumer.Store, error) {
	var rdb = consumer.NewRocksDBStore(rec, dir)

	// Set a filter which re-locates NGrams no longer belonging to this Shard.
	rdb.Options.SetCompactionFilter(&compactionFilter{
		sources:      shard.Spec().Sources,
		relocMapping: a.relocMapping,
		ajc:          a.ajc,
	})
	rdb.Cache = make(shardCache)
	return rdb, rdb.Open()
}

func (a *ngramApp) NewMessage(*protocol.JournalSpec) (message.Message, error) {
	return new(word_count.NGramCount), nil
}

func (a *ngramApp) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope) error {
	var rdb = store.(*consumer.RocksDBStore)
	var cache = rdb.Cache.(shardCache)

	var m = env.Message.(*word_count.NGramCount)

	var prior, ok = cache[m.NGram]
	if !ok {
		// Fill from database.
		if b, err := rdb.DB.GetBytes(rdb.ReadOptions, []byte(m.NGram)); err != nil {
			return err
		} else if len(b) == 0 {
			// Miss; leave |prior| as zero.
		} else if p, i := binary.Uvarint(b); i <= 0 {
			log.WithField("i", i).Panic("error parsing varint")
		} else {
			prior = p
		}
	}
	cache[m.NGram] = prior + m.Count
	return nil
}

func (ngramApp) FinalizeTxn(shard consumer.Shard, store consumer.Store) error {
	var rdb = store.(*consumer.RocksDBStore)
	var cache = rdb.Cache.(shardCache)
	var b []byte

	for ngram, cnt := range cache {
		delete(cache, ngram)

		var n = len(ngram)
		if cap(b) < n+binary.MaxVarintLen64 {
			b = make([]byte, n+binary.MaxVarintLen64)
		}
		rdb.WriteBatch.Put(
			append(b[:0], ngram...),              // Key.
			b[n:n+binary.PutUvarint(b[n:], cnt)], // Value
		)
	}
	return nil
}

type compactionFilter struct {
	sources      []consumer.ShardSpec_Source
	relocMapping message.MappingFunc
	ajc          client.AsyncJournalClient
}

func (cf *compactionFilter) Name() string {
	return "word-count-compactor\x00"
}

func (cf *compactionFilter) Filter(level int, key, val []byte) (remove bool, newVal []byte) {
	if key[0] == 0x00 {
		// This is a meta-key of the store (eg, journal offset). Ignore.
		return false, nil
	}

	var cnt, i = binary.Uvarint(val)
	if i <= 0 {
		panic(i)
	}
	var ngram = &word_count.NGramCount{
		NGram: word_count.NGram(key),
		Count: cnt,
	}
	var journal, framing, err = cf.relocMapping(ngram)

	if err != nil {
		panic(err.Error())
	}
	for _, src := range cf.sources {
		if src.Journal == journal {
			return false, nil // Correctly located; keep this record.
		}
	}

	log.WithFields(log.Fields{
		"ngram": ngram.NGram,
		"count": cnt,
		"from":  cf.sources[0].Journal,
		"to":    journal,
	}).Info("relocating ngram")

	var aa = cf.ajc.StartAppend(journal)
	aa.Require(framing.Marshal(ngram, aa.Writer()))

	if err = aa.Release(); err != nil {
		panic(err.Error())
	}
	// The record is being relocated; indicate that it should be removed locally.
	// This is safe to do because RocksDB must sync new SSTs being written as
	// part of this compaction. That sync is recorded as a strong write barrier,
	// and cannot complete until these relocation writes have fully committed.
	return true, nil
}

type module struct{}

func (module) NewConfig() consumermodule.Config                          { return new(Config) }
func (module) NewApplication(consumermodule.Config) consumer.Application { return new(ngramApp) }
func (module) Register(cfg consumermodule.Config, app consumer.Application, sc mainboilerplate.ServerContext, svc *consumer.Service) {
	if cfg.(*Config).WordCount.N == 0 {
		log.Fatal("--wordcount.N must be specified")
	}

	deltaMapping, err := word_count.NewNGramDeltaMapping(context.Background(), svc.Journals)
	mainboilerplate.Must(err, "failed to build NGramDeltaMapping")
	relocMapping, err := word_count.NewNGramRelocationsMapping(context.Background(), svc.Journals)
	mainboilerplate.Must(err, "failed to build NGramRelocationsMapping")

	var as = client.NewAppendService(context.Background(), svc.Journals)

	word_count.RegisterNGramServer(sc.GRPCServer, &server{
		Service: svc,
		n:       cfg.(*Config).WordCount.N,
		mapping: deltaMapping,
		ajc:     as,
	})
	app.(*ngramApp).relocMapping = relocMapping
	app.(*ngramApp).ajc = as
}

func main() {} // Not called.
var Module consumermodule.Module = module{}
