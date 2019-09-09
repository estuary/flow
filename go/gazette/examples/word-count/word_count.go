// Package word_count is an example application which provides a gRPC API for
// publishing texts and querying running counts of NGrams extracted from
// previously published texts.
package word_count

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"

	"github.com/pkg/errors"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/consumer/store-rocksdb"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"
)

// NGram is a string of N space-delimited tokens, where N is fixed.
type NGram string

func (c *NGramCount) GetUUID() (uuid message.UUID)                  { copy(uuid[:], c.Uuid); return }
func (c *NGramCount) SetUUID(uuid message.UUID)                     { c.Uuid = uuid[:] }
func (c *NGramCount) NewAcknowledgement(pb.Journal) message.Message { return new(NGramCount) }

// Counter consumes NGramCount messages and aggregates total counts of each NGram.
// It also provides gRPC APIs for publishing text and querying NGram counts. It
// implements the following interfaces:
// - runconsumer.Application
// - NGramServer (generated gRPC service stub).
type Counter struct {
	svc     *consumer.Service
	n       int
	mapping message.MappingFunc
	pub     *message.Publisher
}

// counterConfig is the configuration used by Counter.
type counterConfig struct {
	WordCount struct {
		N int `long:"N" description:"Number of grams per N-gram"`
	} `group:"WordCount" namespace:"wordcount"`

	runconsumer.BaseConfig
}

// NewConfig returns a new configuration instance.
func (Counter) NewConfig() runconsumer.Config { return new(counterConfig) }

// InitApplication initializes the application to serve the NGram gRPC service.
func (counter *Counter) InitApplication(args runconsumer.InitArgs) error {
	var N = args.Config.(*counterConfig).WordCount.N
	if N == 0 {
		return errors.New("--wordcount.N must be specified")
	}

	// Build a "deltas" MappingFunc over "app.gazette.dev/message-type=NGramCount" partitions.
	var parts, err = client.NewPolledList(args.Context, args.Service.Journals, time.Minute,
		pb.ListRequest{
			Selector: pb.LabelSelector{Include: pb.MustLabelSet(labels.MessageType, "NGramCount")},
		})
	if err != nil {
		return errors.Wrap(err, "building NGramDeltaMapping")
	}

	// Fill out the Counter instance with fields used by the gRPC API.
	counter.svc = args.Service
	counter.pub = message.NewPublisher(client.NewAppendService(args.Context, args.Service.Journals), nil)
	counter.mapping = message.ModuloMapping(
		func(m message.Mappable, w io.Writer) { _, _ = w.Write([]byte(m.(*NGramCount).NGram)) },
		parts.List,
	)
	counter.n = N

	RegisterNGramServer(args.Server.GRPCServer, counter)
	return nil
}

// NewStore builds a RocksDB store for the Shard.
// Implements consumer.Application.
func (Counter) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	var rdb = store_rocksdb.NewStore(rec)
	rdb.Cache = make(map[NGram]uint64)
	return rdb, rdb.Open()
}

// NewMessage returns an NGramCount message.
// Implements consumer.Application.
func (Counter) NewMessage(*pb.JournalSpec) (message.Message, error) { return new(NGramCount), nil }

// ConsumeMessage folds an NGramCount into its respective running NGram count.
// Implements consumer.Application.
func (Counter) ConsumeMessage(_ consumer.Shard, store consumer.Store, env message.Envelope, _ *message.Publisher) error {
	var rdb = store.(*store_rocksdb.Store)
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
func (Counter) FinalizeTxn(_ consumer.Shard, store consumer.Store, _ *message.Publisher) error {
	var rdb = store.(*store_rocksdb.Store)
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
func (counter *Counter) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	var (
		resp  = new(PublishResponse)
		N     = counter.n                            // Number of tokens per-NGram.
		grams = make([]string, N)                    // NGram being extracted.
		delta = &NGramCount{Count: 1}                // NGramCount delta to publish.
		buf   bytes.Buffer                           // Buffer for the composed NGram.
		ops   = make(map[pb.Journal]client.OpFuture) // AsyncAppends of published NGrams.

		// Use a very simplistic token strategy: lowercase, letter character sequences.
		words = strings.FieldsFunc(strings.ToLower(req.Text),
			func(r rune) bool { return !unicode.IsLetter(r) })
	)
	for i := range grams {
		grams[i] = "START"
	}
	for i := 0; i != len(words)+counter.n; i++ {
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
		if aa, err := counter.pub.PublishCommitted(counter.mapping, delta); err != nil {
			return resp, err
		} else {
			ops[aa.Request().Journal] = aa
		}
	}

	for _, op := range ops {
		if op.Err() != nil {
			return resp, op.Err()
		}
	}
	return resp, nil
}

// Query a count for an NGram count (or counts for a prefix thereof). If the
// requested or imputed Shard does not resolve locally, Query will proxy the
// request to the responsible process.
func (counter *Counter) Query(ctx context.Context, req *QueryRequest) (resp *QueryResponse, err error) {
	resp = new(QueryResponse)

	if req.Shard == "" {
		if req.Shard, err = counter.mapPrefixToShard(req.Prefix); err != nil {
			return
		}
	}

	var res consumer.Resolution
	if res, err = counter.svc.Resolver.Resolve(consumer.ResolveArgs{
		Context:     ctx,
		ShardID:     req.Shard,
		MayProxy:    req.Header == nil, // MayProxy if request hasn't already been proxied.
		ProxyHeader: req.Header,
	}); err != nil {
		return
	} else if res.Status != pc.Status_OK {
		err = fmt.Errorf(res.Status.String())
		return
	} else if res.Store == nil {
		req.Header = &res.Header // Proxy to the resolved primary peer.

		return NewNGramClient(counter.svc.Loopback).Query(
			pb.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId), req)
	}
	defer res.Done()

	var rdb = res.Store.(*store_rocksdb.Store)
	var it = store_rocksdb.AsArenaIterator(rdb.DB.NewIterator(rdb.ReadOptions), make([]byte, 32*1024))
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
		var cnt, i = binary.Uvarint(it.Value())
		if i <= 0 {
			err = fmt.Errorf("internal error parsing varint (%d)", i)
			return
		}
		resp.Grams = append(resp.Grams, NGramCount{
			NGram: NGram(it.Key()),
			Count: cnt,
		})
	}
	return
}

func (counter *Counter) mapPrefixToShard(prefix NGram) (shard pc.ShardID, err error) {
	// Determine the Journal which Prefix maps to, and then the ID of the
	// ShardSpec which consumes that |journal|.
	var journal pb.Journal
	if journal, _, err = counter.mapping(&NGramCount{NGram: prefix}); err != nil {
		return
	}

	// Walk ShardSpecs to find one which has |journal| as a source.
	counter.svc.State.KS.Mu.RLock()
	defer counter.svc.State.KS.Mu.RUnlock()

	for _, kv := range counter.svc.State.Items {
		var spec = kv.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)

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
