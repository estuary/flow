package ingest

import (
	"bytes"
	"context"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocol"
	"go.gazette.dev/core/broker/protocol/ext"
	"go.gazette.dev/core/keyspace"
)

// GRPCAPI is a gRPC API for ingestion.
type GRPCAPI struct {
	Ingester *flow.Ingester
	Journals *keyspace.KeySpace
}

// Ingest implements IngesterServer.
func (a *GRPCAPI) Ingest(ctx context.Context, req *pf.IngestRequest) (*pf.IngestResponse, error) {
	var ingestion = a.Ingester.Start()
	defer ingestion.Done()

	for _, c := range req.Collections {
		var docs = c.DocsJsonLines

		for len(docs) != 0 {
			var pivot = bytes.IndexByte(docs, '\n')
			if pivot == -1 {
				pivot = len(docs)
			}

			if err := ingestion.Add(c.Name, docs[:pivot]); err != nil {
				return new(pf.IngestResponse), err
			}
			docs = docs[pivot+1:]
		}
	}

	var offsets, err = ingestion.PrepareAndAwait()
	if err != nil {
		return new(pf.IngestResponse), err
	}

	a.Journals.Mu.RLock()
	var etcd = ext.FromEtcdResponseHeader(a.Journals.Header)
	a.Journals.Mu.RUnlock()

	return &pf.IngestResponse{
		JournalWriteHeads: offsets,
		JournalEtcd:       etcd,
	}, nil
}

var _ pf.IngesterServer = (*GRPCAPI)(nil)
