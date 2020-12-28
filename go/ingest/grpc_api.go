package ingest

import (
	"bytes"
	"context"

	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/broker/protocol/ext"
)

// grpcAPI is a gRPC API for ingestion.
type grpcAPI struct {
	args
}

// Ingest implements IngesterServer.
func (a *grpcAPI) Ingest(ctx context.Context, req *pf.IngestRequest) (*pf.IngestResponse, error) {
	var ingestion = a.ingester.Start()

	for _, c := range req.Collections {
		var docs = c.DocsJsonLines
		for {
			var pivot = bytes.IndexByte(docs, '\n')
			if pivot == -1 {
				pivot = len(docs)
			}

			if pivot != 0 {
				if err := ingestion.Add(c.Name, docs[:pivot]); err != nil {
					return new(pf.IngestResponse), err
				}
			}

			if pivot == len(docs) {
				break
			}
			docs = docs[pivot+1:]
		}
	}

	var offsets, err = ingestion.PrepareAndAwait()
	if err != nil {
		return new(pf.IngestResponse), err
	}

	a.journals.Mu.RLock()
	var etcd = ext.FromEtcdResponseHeader(a.journals.Header)
	a.journals.Mu.RUnlock()

	return &pf.IngestResponse{
		JournalWriteHeads: offsets,
		JournalEtcd:       etcd,
	}, nil
}

var _ pf.IngesterServer = (*grpcAPI)(nil)
