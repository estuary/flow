package consumer

import (
	"context"
	"errors"
	"strings"

	"go.etcd.io/etcd/v3/clientv3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/protocol"
	"google.golang.org/grpc"
)

// Stat dispatches the ShardServer.Stat API.
func (srv *Service) Stat(ctx context.Context, req *StatRequest) (*StatResponse, error) {
	var (
		resp     = new(StatResponse)
		res, err = srv.Resolver.Resolve(ResolveArgs{
			Context:     ctx,
			ShardID:     req.Shard,
			MayProxy:    req.Header == nil, // MayProxy if request hasn't already been proxied.
			ProxyHeader: req.Header,
		})
	)
	resp.Status, resp.Header = res.Status, res.Header

	if err != nil || resp.Status != Status_OK {
		return resp, err
	} else if res.Store == nil {
		// Non-local Shard. Proxy to the resolved primary peer.
		req.Header = &res.Header
		return NewShardClient(srv.Loopback).Stat(
			pb.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId), req)
	}
	defer res.Done()

	// Introspect journal consumption offsets from the store.
	if resp.Offsets, err = res.Store.FetchJournalOffsets(); err == nil {
		// Recoverylog & other journal writes reflecting processing through
		// fetched offsets may still be in progress. Block on a WeakBarrier so
		// that, when we return to the caller, they're assured that all writes
		// related to processing through the offsets have also committed.
		var txn = res.Store.Recorder().WeakBarrier()
		_, err = <-txn.Done(), txn.Err()
	}
	return resp, err
}

// List dispatches the ShardServer.List API.
func (srv *Service) List(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	var s = srv.Resolver.state

	var resp = &ListResponse{
		Status: Status_OK,
		Header: pb.NewUnroutedHeader(s),
	}
	if err := req.Validate(); err != nil {
		return resp, err
	}

	defer s.KS.Mu.RUnlock()
	s.KS.Mu.RLock()

	var metaLabels, allLabels pb.LabelSet

	var it = allocator.LeftJoin{
		LenL: len(s.Items),
		LenR: len(s.Assignments),
		Compare: func(l, r int) int {
			var lID = s.Items[l].Decoded.(allocator.Item).ID
			var rID = s.Assignments[r].Decoded.(allocator.Assignment).ItemID
			return strings.Compare(lID, rID)
		},
	}
	for cur, ok := it.Next(); ok; cur, ok = it.Next() {
		var shard = ListResponse_Shard{
			Spec: *s.Items[cur.Left].Decoded.(allocator.Item).ItemValue.(*ShardSpec)}

		metaLabels = ExtractShardSpecMetaLabels(&shard.Spec, metaLabels)
		allLabels = pb.UnionLabelSets(metaLabels, shard.Spec.LabelSet, allLabels)

		if !req.Selector.Matches(allLabels) {
			continue
		}
		shard.ModRevision = s.Items[cur.Left].Raw.ModRevision
		shard.Route.Init(s.Assignments[cur.RightBegin:cur.RightEnd])
		shard.Route.AttachEndpoints(s.KS)

		for _, asn := range s.Assignments[cur.RightBegin:cur.RightEnd] {
			shard.Status = append(shard.Status,
				*asn.Decoded.(allocator.Assignment).AssignmentValue.(*ReplicaStatus))
		}

		resp.Shards = append(resp.Shards, shard)
	}
	return resp, nil
}

// Apply dispatches the ShardServer.Apply API.
func (srv *Service) Apply(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
	var s = srv.Resolver.state

	var resp = &ApplyResponse{
		Status: Status_OK,
		Header: pb.NewUnroutedHeader(s),
	}
	if err := req.Validate(); err != nil {
		return resp, err
	}

	var cmp []clientv3.Cmp
	var ops []clientv3.Op

	for _, changes := range req.Changes {
		var key string

		if changes.Upsert != nil {
			key = allocator.ItemKey(s.KS, changes.Upsert.Id.String())
			ops = append(ops, clientv3.OpPut(key, changes.Upsert.MarshalString()))
		} else {
			key = allocator.ItemKey(s.KS, changes.Delete.String())
			ops = append(ops, clientv3.OpDelete(key))
		}
		cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(key), "=", changes.ExpectModRevision))
	}

	var txnResp, err = srv.Etcd.Do(ctx, clientv3.OpTxn(cmp, ops, nil))
	if err != nil {
		// Pass.
	} else if !txnResp.Txn().Succeeded {
		resp.Status = Status_ETCD_TRANSACTION_FAILED
	} else {
		// Delay responding until we have read our own Etcd write.
		s.KS.Mu.RLock()
		err = s.KS.WaitForRevision(ctx, txnResp.Txn().Header.Revision)
		s.KS.Mu.RUnlock()
	}
	return resp, err
}

// GetHints dispatches the ShardServer.Hints API.
func (srv *Service) GetHints(ctx context.Context, req *GetHintsRequest) (*GetHintsResponse, error) {
	var (
		resp = &GetHintsResponse{
			Status: Status_OK,
			Header: pb.NewUnroutedHeader(srv.State),
		}
		ks   = srv.State.KS
		spec *ShardSpec
	)

	ks.Mu.RLock()
	var item, ok = allocator.LookupItem(ks, req.Shard.String())
	ks.Mu.RUnlock()
	if !ok {
		resp.Status = Status_SHARD_NOT_FOUND
		return resp, nil
	}
	spec = item.ItemValue.(*ShardSpec)

	var h, err = fetchHints(ctx, spec, srv.Etcd)
	if err != nil {
		return nil, err
	}

	if h.hints[0] != nil {
		resp.PrimaryHints = GetHintsResponse_ResponseHints{
			Hints: h.hints[0],
		}
	}

	if len(h.hints) > 1 {
		for _, hints := range h.hints[1:] {
			resp.BackupHints = append(resp.BackupHints, GetHintsResponse_ResponseHints{
				Hints: hints,
			})
		}
	}
	return resp, nil
}

// ListShards invokes the List RPC, and maps a validation or !OK status to an error.
func ListShards(ctx context.Context, sc ShardClient, req *ListRequest) (*ListResponse, error) {
	if r, err := sc.List(pb.WithDispatchDefault(ctx), req, grpc.FailFast(false)); err != nil {
		return r, err
	} else if err = r.Validate(); err != nil {
		return r, err
	} else if r.Status != Status_OK {
		return r, errors.New(r.Status.String())
	} else {
		return r, nil
	}
}

// StatShard invokes the Stat RPC, and maps a validation or !OK status to an error.
func StatShard(ctx context.Context, rc RoutedShardClient, req *StatRequest) (*StatResponse, error) {
	var routedCtx = pb.WithDispatchItemRoute(ctx, rc, req.Shard.String(), false)
	if r, err := rc.Stat(routedCtx, req, grpc.FailFast(false)); err != nil {
		return r, err
	} else if err = r.Validate(); err != nil {
		return r, err
	} else if r.Status != Status_OK {
		return r, errors.New(r.Status.String())
	} else {
		return r, nil
	}

}

// ApplyShards invokes the Apply RPC.
func ApplyShards(ctx context.Context, sc ShardClient, req *ApplyRequest) (*ApplyResponse, error) {
	return ApplyShardsInBatches(ctx, sc, req, 0)
}

// ApplyShardsInBatches applies changes to shards which
// may be larger than the configured etcd transaction size size. The changes in
// |req| will be sent serially in batches of size |size|. If
// |size| is 0 all changes will be attempted as part of a single
// transaction. This function will return the response of the final
// ShardClient.Apply call. Response validation or !OK status from Apply RPC are
// mapped to error.
func ApplyShardsInBatches(ctx context.Context, sc ShardClient, req *ApplyRequest, size int) (*ApplyResponse, error) {
	if len(req.Changes) == 0 {
		return &ApplyResponse{}, nil
	}
	if size == 0 {
		size = len(req.Changes)
	}
	var curReq = &ApplyRequest{}
	var offset = 0

	for {
		if len(req.Changes[offset:]) > size {
			curReq.Changes = req.Changes[offset : offset+size]
		} else {
			curReq.Changes = req.Changes[offset:]
		}

		var resp *ApplyResponse
		var err error
		if resp, err = sc.Apply(pb.WithDispatchDefault(ctx), curReq, grpc.WaitForReady(true)); err != nil {
			return resp, err
		} else if err = resp.Validate(); err != nil {
			return resp, err
		} else if resp.Status != Status_OK {
			return resp, errors.New(resp.Status.String())
		}

		offset = offset + len(curReq.Changes)
		if offset == len(req.Changes) {
			return resp, nil
		}
	}
}

// FetchHints invokes the Hints RPC, and maps a validation or !OK status to an error.
func FetchHints(ctx context.Context, sc ShardClient, req *GetHintsRequest) (*GetHintsResponse, error) {
	if r, err := sc.GetHints(pb.WithDispatchDefault(ctx), req, grpc.FailFast(false)); err != nil {
		return r, err
	} else if err = r.Validate(); err != nil {
		return r, err
	} else if r.Status != Status_OK {
		return r, errors.New(r.Status.String())
	} else {
		return r, nil
	}
}
