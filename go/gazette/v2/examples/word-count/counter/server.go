package main

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/LiveRamp/gazette/v2/examples/word-count"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
)

type server struct {
	*consumer.Service

	n       int
	mapping message.MappingFunc
	ajc     client.AsyncJournalClient
}

func (srv *server) Publish(ctx context.Context, req *word_count.PublishRequest) (resp *word_count.PublishResponse, err error) {
	resp = new(word_count.PublishResponse)
	err = word_count.PublishNGrams(srv.mapping, srv.ajc, req.Text, srv.n)
	return
}

func (srv *server) Query(ctx context.Context, req *word_count.QueryRequest) (resp *word_count.QueryResponse, err error) {
	resp = new(word_count.QueryResponse)

	if req.Shard == "" {
		if req.Shard, err = srv.prefixToShard(req.Prefix); err != nil {
			return
		}
	}

	var res consumer.Resolution
	if res, err = srv.Service.Resolver.Resolve(consumer.ResolveArgs{
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
		return word_count.NewNGramClient(srv.Loopback).Query(
			protocol.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId), req)
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
		resp.Grams = append(resp.Grams, word_count.NGramCount{
			NGram: word_count.NGram(it.Key().Data()),
			Count: cnt,
		})
	}
	return
}

func (srv *server) prefixToShard(prefix word_count.NGram) (shard consumer.ShardID, err error) {
	// Determine the Journal which Prefix maps to, and then the ID of the
	// ShardSpec which consumes that |journal|.
	var journal protocol.Journal
	if journal, _, err = srv.mapping(&word_count.NGramCount{NGram: prefix}); err != nil {
		return
	}
	for _, spec := range srv.Service.Specs() {
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
