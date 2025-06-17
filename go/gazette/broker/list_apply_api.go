package broker

import (
	"context"
	"fmt"
	"net"
	"strings"

	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	"go.gazette.dev/core/keyspace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// List dispatches the JournalServer.List API.
func (svc *Service) List(claims pb.Claims, req *pb.ListRequest, stream pb.Journal_ListServer) (err error) {
	defer instrumentJournalServerRPC("List", &err, nil)()

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(stream.Context()); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served List RPC failed")
		}
	}()

	if err = req.Validate(); err != nil {
		return err
	}

	// Gazette has historically offered a special "prefix" label which matches
	// slash-terminated prefixes of a journal name. Today, it's implemented in
	// terms of a LabelSelector prefix match.
	for _, set := range []*pb.LabelSet{
		&req.Selector.Include,
		&req.Selector.Exclude,
		&claims.Selector.Include,
		&claims.Selector.Exclude,
	} {
		if prefix := set.ValuesOf("prefix"); len(prefix) != 0 {
			for _, val := range prefix {
				set.AddValue("name:prefix", val)
			}
			set.Remove("prefix")
		}
	}

	var (
		// Etcd revision to read through before responding.
		minEtcdRevision int64 = 0
		// Number of journals in the last list snapshot.
		lastMatchedItems = -1
		// Max Etcd mod revision in the last list snapshot.
		lastMaxModRevision int64 = -1
	)
	if req.WatchResume != nil {
		// Resume from the next Etcd revision.
		minEtcdRevision = req.WatchResume.Etcd.Revision + 1
	}

	for {
		var resp, err = listRound(
			stream.Context(),
			&claims,
			&req.Selector,
			svc.resolver.state,
			&minEtcdRevision,
			&lastMatchedItems,
			&lastMaxModRevision,
		)

		if err != nil {
			return pb.SuppressCancellationError(err)
		} else if resp == nil {
			continue // Next snapshot is not ready.
		}

		// Note that listWatchRound() holds an RLock over our service state,
		// but we do *not* hold a lock below while streaming the response.
		// We cannot allow client back-pressure to cause us to hold our lock.

		for len(resp.Journals) > maxJournalsPerListResponse {
			var tail = resp.Journals[maxJournalsPerListResponse:]
			resp.Journals = resp.Journals[:maxJournalsPerListResponse]

			if err = stream.Send(resp); err != nil {
				return err
			}
			*resp = pb.ListResponse{Journals: tail}
		}
		if err = stream.Send(resp); err != nil {
			return err
		}

		if !req.Watch {
			return nil // Unary snapshot is complete.
		} else if err = stream.Send(&pb.ListResponse{}); err != nil {
			return err
		}
	}
}

func listRound(
	ctx context.Context,
	claims *pb.Claims,
	selector *pb.LabelSelector,
	state *allocator.State,
	minEtcdRevision *int64,
	lastMatchedItems *int,
	lastMaxModRevision *int64,
) (*pb.ListResponse, error) {

	defer state.KS.Mu.RUnlock()
	state.KS.Mu.RLock()

	if *minEtcdRevision > state.KS.Header.Revision {
		addTrace(ctx, " ... at revision %d, but want at least %d",
			state.KS.Header.Revision, *minEtcdRevision)

		if err := state.KS.WaitForRevision(ctx, *minEtcdRevision); err != nil {
			return nil, err
		}
		addTrace(ctx, "WaitForRevision(%d) => %d",
			minEtcdRevision, state.KS.Header.Revision)
	}
	// Next iteration, we'll block until we observe a still-larger revision.
	*minEtcdRevision = state.KS.Header.Revision + 1

	var (
		allItems       = state.Items
		assignments    = state.Assignments
		matchedItems   keyspace.KeyValues
		maxModRevision int64 = -1
		scratch        pb.LabelSet
	)

	// If we're matching over a single name (or name prefix), then leverage
	// the fact that Items and Assignments are naturally ordered on name.
	if name := selector.Include.ValuesOf("name"); len(name) == 1 {
		allItems = allItems.Prefixed(state.KS.Root + allocator.ItemsPrefix + name[0])
		assignments = assignments.Prefixed(state.KS.Root + allocator.AssignmentsPrefix + name[0])
	}

	for _, item := range allItems {
		var spec = item.Decoded.(allocator.Item).ItemValue.(*pb.JournalSpec)

		// LabelSetExt() truncates `scratch` while re-using its storage.
		scratch = spec.LabelSetExt(scratch)

		if selector.Matches(scratch) && claims.Selector.Matches(scratch) {
			if item.Raw.ModRevision > maxModRevision {
				maxModRevision = item.Raw.ModRevision
			}
			matchedItems = append(matchedItems, item)
		}
	}

	// If the number of items is unchanged AND the maximum mod revision is unchanged,
	// then the snapshot cannot have changed. This follows because a create or update
	// will increase the mod revision, and a deletion will decrease the number of items.
	if len(matchedItems) == *lastMatchedItems && maxModRevision == *lastMaxModRevision {
		return nil, nil
	}

	// The listing has changed and we must build a new snapshot.
	*lastMatchedItems, *lastMaxModRevision = len(matchedItems), maxModRevision

	var resp = &pb.ListResponse{
		Status: pb.Status_OK,
		Header: pbx.NewUnroutedHeader(state),
	}
	var it = allocator.LeftJoin{
		LenL: len(matchedItems),
		LenR: len(assignments),
		Compare: func(l, r int) int {
			var lID = matchedItems[l].Decoded.(allocator.Item).ID
			var rID = assignments[r].Decoded.(allocator.Assignment).ItemID
			return strings.Compare(lID, rID)
		},
	}
	for cur, ok := it.Next(); ok; cur, ok = it.Next() {
		var journal = pb.ListResponse_Journal{
			Spec:           *matchedItems[cur.Left].Decoded.(allocator.Item).ItemValue.(*pb.JournalSpec),
			ModRevision:    matchedItems[cur.Left].Raw.ModRevision,
			CreateRevision: matchedItems[cur.Left].Raw.CreateRevision,
		}
		pbx.Init(&journal.Route, assignments[cur.RightBegin:cur.RightEnd])
		pbx.AttachEndpoints(&journal.Route, state.KS)
		resp.Journals = append(resp.Journals, journal)
	}

	return resp, nil
}

// Apply dispatches the JournalServer.Apply API.
func (svc *Service) Apply(ctx context.Context, claims pb.Claims, req *pb.ApplyRequest) (resp *pb.ApplyResponse, err error) {
	defer instrumentJournalServerRPC("Apply", &err, nil)()

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(ctx); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served Apply RPC failed")
		}
	}()

	if err = req.Validate(); err != nil {
		return nil, err
	}

	var cmp []clientv3.Cmp
	var ops []clientv3.Op
	var s = svc.resolver.state
	var scratch pb.LabelSet

	for _, change := range req.Changes {
		var key string

		if change.Upsert != nil {
			// For Upserts, authorize against the journal's labels plus meta-labels
			scratch = change.Upsert.LabelSetExt(scratch)
			if !claims.Selector.Matches(scratch) {
				return nil, status.Error(codes.Unauthenticated, fmt.Sprintf("not authorized to %s", change.Upsert.Name))
			}
			key = allocator.ItemKey(s.KS, change.Upsert.Name.String())
			ops = append(ops, clientv3.OpPut(key, change.Upsert.MarshalString()))
		} else {
			// For Deletes, use name-only authorization
			if !claims.Selector.Matches(pb.MustLabelSet("name", change.Delete.String())) {
				return nil, status.Error(codes.Unauthenticated, fmt.Sprintf("not authorized to %s", change.Delete))
			}
			key = allocator.ItemKey(s.KS, change.Delete.String())
			ops = append(ops, clientv3.OpDelete(key))
		}

		// Allow caller to explicitly ignore revision comparison
		// by passing a value of -1 for revision.
		if change.ExpectModRevision != -1 {
			cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(key), "=", change.ExpectModRevision))
		}
	}

	s.KS.Mu.RLock()
	resp = &pb.ApplyResponse{
		Status: pb.Status_OK,
		Header: pbx.NewUnroutedHeader(s),
	}
	s.KS.Mu.RUnlock()

	var txnResp clientv3.OpResponse
	if txnResp, err = svc.etcd.Do(ctx, clientv3.OpTxn(cmp, ops, nil)); err != nil {
		return resp, err
	} else if !txnResp.Txn().Succeeded {
		resp.Status = pb.Status_ETCD_TRANSACTION_FAILED
	} else if len(ops) != 0 {
		// If we made changes, delay responding until we have read our own Etcd write.
		s.KS.Mu.RLock()
		err = s.KS.WaitForRevision(ctx, txnResp.Txn().Header.GetRevision())
		s.KS.Mu.RUnlock()
	}
	resp.Header.Etcd.Revision = txnResp.Txn().Header.GetRevision()
	return resp, err
}

// NOTE(johnny): List was originally a unary API, which had two issues:
// * In large deployments, responses would bump against gRPC maximum messages sizes.
// * Unary responses can't represent a long-lived watch.
//
// List was updated to become a server-streaming API which addresses both issues,
// while remaining compatible with older clients that expect a unary API,
// so long as the entire response set can be sent as a single message.
// This value is large to allow older deployments to continue to function,
// without being SO large that we bump against gRPC message limits.
var maxJournalsPerListResponse = 1000
