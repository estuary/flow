// go:generate protoc -I . -I ../../../vendor -I ../../../../../.. --gogo_out=plugins=grpc:. word_count.proto
// +build !norocksdb

// Package word_count is an example application which provides a gRPC API for
// publishing texts and querying running counts of NGrams extracted from
// previously published texts.
package word_count

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/LiveRamp/gazette/v2/cmd/run-consumer/consumermodule"
	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	"github.com/pkg/errors"
)

// NGram is a string of N space-delimited tokens, where N is fixed.
type NGram string

// Counter consumes NGramCount messages and aggregates total counts of each NGram.
// It also provides gRPC APIs for publishing text and querying NGram counts. It
// implements the following interfaces:
// - consumermodule.Module
// - consumer.Application
// - NGramServer (generated gRPC service stub).
type Counter struct {
	svc     *consumer.Service
	n       int
	mapping message.MappingFunc
	ajc     client.AsyncJournalClient
}

// counterConfig is the configuration used by Counter.
type counterConfig struct {
	WordCount struct {
		N int `long:"N" description:"Number of grams per N-gram"`
	} `group:"WordCount" namespace:"wordcount"`

	consumermodule.BaseConfig
}

// NewConfig returns a new configuration instance.
// Implements consumermodule.Module.
func (Counter) NewConfig() consumermodule.Config { return new(counterConfig) }

// NewApplication returns a new instance of itself (as Counter is its own
// consumer.Application).
// Implements consumermodule.Module.
func (Counter) NewApplication() consumer.Application { return new(Counter) }

// InitModule initializes the application to serve the NGram gRPC service.
// Implements consumermodule.Module.
func (Counter) InitModule(args consumermodule.InitArgs) error {
	var N = args.Config.(*counterConfig).WordCount.N
	if N == 0 {
		return errors.New("--wordcount.N must be specified")
	}

	// Build a "deltas" MappingFunc over "topic=examples/word-count/deltas" partitions.
	var parts, err = client.NewPolledList(args.Context, args.Service.Journals, time.Minute,
		pb.ListRequest{
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("topic", deltasTopicLabel),
			},
		})
	if err != nil {
		return errors.Wrap(err, "building NGramDeltaMapping")
	}

	// Fill out the Counter instance with fields used by the gRPC API.
	var app = args.Application.(*Counter)

	app.svc = args.Service
	app.ajc = client.NewAppendService(args.Context, args.Service.Journals)
	app.mapping = message.ModuloMapping(
		func(m message.Message, b []byte) []byte {
			return append(b, m.(*NGramCount).NGram[:]...)
		},
		parts.List,
	)
	app.n = N

	RegisterNGramServer(args.Server.GRPCServer, app)
	return nil
}

// NewStore builds a RocksDB store for the Shard.
// Implements consumer.Application.
func (Counter) NewStore(shard consumer.Shard, dir string, rec *recoverylog.Recorder) (consumer.Store, error) {
	var rdb = consumer.NewRocksDBStore(rec, dir)
	rdb.Cache = make(map[NGram]uint64)
	return rdb, rdb.Open()
}

// NewMessage returns an NGramCount message.
// Implements consumer.Application.
func (Counter) NewMessage(*pb.JournalSpec) (message.Message, error) { return new(NGramCount), nil }

// ConsumeMessage folds an NGramCount into its respective running NGram count.
// Implements consumer.Application.
func (Counter) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope) error {
	var rdb = store.(*consumer.RocksDBStore)
	var cache = rdb.Cache.(map[NGram]uint64)

	var m = env.Message.(*NGramCount)

	var prior, ok = cache[m.NGram]
	if !ok {
		// Fill from database.
		if b, err := rdb.DB.GetBytes(rdb.ReadOptions, []byte(m.NGram)); err != nil {
			return err
		} else if len(b) == 0 {
			// Miss; leave |prior| as zero.
		} else if p, i := binary.Uvarint(b); i <= 0 {
			return errors.Wrapf(err, "failed to parse encoded varint count")
		} else {
			prior = p
		}
	}
	cache[m.NGram] = prior + m.Count
	return nil
}

// FinalizeTxn marshals in-memory NGram counts to the |store|, ensuring persistence
// across consumer transactions.
// Implements consumer.Application.
func (Counter) FinalizeTxn(shard consumer.Shard, store consumer.Store) error {
	var rdb = store.(*consumer.RocksDBStore)
	var cache = rdb.Cache.(map[NGram]uint64)
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

// Publish extracts NGrams of the configured length from the PublishRequest,
// and publishes an NGramCount message for each. It returns after all published
// messages have committed to their respective journals.
func (api *Counter) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	var (
		resp         = new(PublishResponse)
		N            = api.n                    // Number of tokens per-NGram.
		grams        = make([]string, N)        // NGram being extracted.
		delta        = &NGramCount{Count: 1}    // NGramCount delta to publish.
		buf          bytes.Buffer               // Buffer for the composed NGram.
		asyncAppends []*client.AsyncAppend      // AsyncAppends of published NGrams.
		appendsIndex = make(map[pb.Journal]int) // Indexes |asyncAppends| by journal.

		// Use a very simplistic token strategy: lowercase, letter character sequences.
		words = strings.FieldsFunc(strings.ToLower(req.Text),
			func(r rune) bool { return !unicode.IsLetter(r) })
	)
	for i := range grams {
		grams[i] = "START"
	}
	for i := 0; i != len(words)+api.n; i++ {
		copy(grams, grams[1:])

		if i < len(words) {
			grams[N-1] = words[i]
		} else {
			grams[N-1] = "END"
		}

		buf.Reset()
		for j, g := range grams {
			buf.WriteString(g)
			if j != N-1 {
				buf.WriteByte(' ')
			}
		}

		delta.NGram = NGram(buf.String())
		if aa, err := message.Publish(api.ajc, api.mapping, delta); err != nil {
			return resp, err
		} else if ind, ok := appendsIndex[aa.Request().Journal]; ok {
			asyncAppends[ind] = aa // Keep only the last AsyncAppend of each journal.
		} else {
			appendsIndex[aa.Request().Journal] = len(asyncAppends)
			asyncAppends = append(asyncAppends, aa)
		}
	}

	client.WaitForPendingAppends(asyncAppends)
	return resp, nil
}

// Query a count for an NGram count (or counts for a prefix thereof). If the
// requested or imputed Shard does not resolve locally, Query will proxy the
// request to the responsible process.
func (api *Counter) Query(ctx context.Context, req *QueryRequest) (resp *QueryResponse, err error) {
	resp = new(QueryResponse)

	if req.Shard == "" {
		if req.Shard, err = api.mapPrefixToShard(req.Prefix); err != nil {
			return
		}
	}

	var res consumer.Resolution
	if res, err = api.svc.Resolver.Resolve(consumer.ResolveArgs{
		Context:     ctx,
		ShardID:     req.Shard,
		MayProxy:    req.Header == nil, // MayProxy if request hasn't already been proxied.
		ProxyHeader: req.Header,
	}); err != nil {
		return
	} else if res.Status != consumer.Status_OK {
		err = fmt.Errorf(res.Status.String())
		return
	} else if res.Store == nil {
		req.Header = &res.Header // Proxy to the resolved primary peer.

		return NewNGramClient(api.svc.Loopback).Query(
			pb.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId), req)
	}
	defer res.Done()

	var rdb = res.Store.(*consumer.RocksDBStore)
	var it = rdb.DB.NewIterator(rdb.ReadOptions)
	defer it.Close()

	var prefix = []byte(req.Prefix)

	if req.Prefix != "" {
		it.Seek(prefix)
	} else {
		// Meta-keys such as journal offsets are encoded with a preceding 0x00.
		// Start iterating over application-defined keys from 0x01.
		it.Seek([]byte{0x01})
	}
	for ; it.ValidForPrefix(prefix); it.Next() {
		var cnt, i = binary.Uvarint(it.Value().Data())
		if i <= 0 {
			err = fmt.Errorf("internal error parsing varint (%d)", i)
			return
		}
		resp.Grams = append(resp.Grams, NGramCount{
			NGram: NGram(it.Key().Data()),
			Count: cnt,
		})
	}
	return
}

func (api *Counter) mapPrefixToShard(prefix NGram) (shard consumer.ShardID, err error) {
	// Determine the Journal which Prefix maps to, and then the ID of the
	// ShardSpec which consumes that |journal|.
	var journal pb.Journal
	if journal, _, err = api.mapping(&NGramCount{NGram: prefix}); err != nil {
		return
	}

	// Walk ShardSpecs to find one which has |journal| as a source.
	api.svc.State.KS.Mu.RLock()
	defer api.svc.State.KS.Mu.RUnlock()

	for _, kv := range api.svc.State.Items {
		var spec = kv.Decoded.(allocator.Item).ItemValue.(*consumer.ShardSpec)

		for _, src := range spec.Sources {
			if src.Journal == journal {
				shard = spec.Id
				return
			}
		}
	}
	err = fmt.Errorf("no ShardSpec is consuming mapped journal %s", journal)
	return
}

// deltasTopicLabel identifies journals which are partitions of NGramCount delta messages.
const deltasTopicLabel = "examples/word-count/deltas"
