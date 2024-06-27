package flow

import (
	"context"
	"fmt"

	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/protocol/ext"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"golang.org/x/net/trace"
)

// ShardStat wraps consumer.ShardStat to provide additional synchronization
// over a |journals| Etcd header carried as a StatRequest & StatResponse
// extension.
func ShardStat(ctx context.Context, claims pb.Claims, svc *consumer.Service, req *pc.StatRequest, journals Journals) (*pc.StatResponse, error) {
	var err error
	var reqJournalEtcd pb.Header_Etcd

	if len(req.Extension) == 0 {
		// Extension is authored by Flow, but not other Gazette tooling (like `gazctl`).
	} else if err = reqJournalEtcd.Unmarshal(req.Extension); err != nil {
		return new(pc.StatResponse), fmt.Errorf("failed to unmarshal journals Etcd extension: %w", err)
	} else if err = reqJournalEtcd.Validate(); err != nil {
		return new(pc.StatResponse), fmt.Errorf("extension journals Etcd: %w", err)
	}

	// Sanity check journals ClusterId, and block on a future revision.
	journals.Mu.RLock()
	if cid := reqJournalEtcd.ClusterId; cid != 0 && cid != journals.Header.ClusterId {
		err = fmt.Errorf("request journals Etcd ClusterId doesn't match our own (%d vs %d)",
			reqJournalEtcd.ClusterId, journals.Header.ClusterId)
	} else if reqJournalEtcd.Revision > journals.Header.Revision {
		if tr, ok := trace.FromContext(ctx); ok {
			tr.LazyPrintf(" ... at journals revision %d, but want at least %d",
				journals.Header.Revision, reqJournalEtcd.Revision)
		}
		err = journals.WaitForRevision(ctx, reqJournalEtcd.Revision)
	}
	journals.Mu.RUnlock()

	if err != nil {
		return new(pc.StatResponse), err
	}

	// Delegate to the underlying Stat implementation.
	resp, err := consumer.ShardStat(ctx, claims, svc, req)
	if err != nil {
		return nil, err
	}

	if resp.Extension != nil {
		// We're returning a proxied response. Don't modify.
	} else {
		journals.Mu.RLock()
		reqJournalEtcd = ext.FromEtcdResponseHeader(journals.Header)
		journals.Mu.RUnlock()

		// Attach current journals keyspace header to the response.
		resp.Extension, _ = reqJournalEtcd.Marshal()
	}
	return resp, nil
}
