package consumer

import (
	"context"
	"errors"
	"strings"

	"go.etcd.io/etcd/clientv3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"google.golang.org/grpc"
)

// Stat dispatches the ShardServer.Stat API.
func (srv *Service) Stat(ctx context.Context, req *pc.StatRequest) (*pc.StatResponse, error) {
	var (
		resp     = new(pc.StatResponse)
		res, err = srv.Resolver.Resolve(ResolveArgs{
			Context:     ctx,
			ShardID:     req.Shard,
			MayProxy:    req.Header == nil, // MayProxy if request hasn't already been proxied.
			ProxyHeader: req.Header,
			ReadThrough: req.ReadThrough,
		})
	)
	resp.Status, resp.Header = res.Status, res.Header

	if err != nil || resp.Status != pc.Status_OK {
		return resp, err
	} else if res.Store == nil {
		// Non-local Shard. Proxy to the resolved primary peer.
		req.Header = &res.Header
		return pc.NewShardClient(srv.Loopback).Stat(
			pb.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId), req)
	}
	defer res.Done()

	resp.ReadThrough, resp.PublishAt = res.Shard.Progress()
	return resp, err
}

// List dispatches the ShardServer.List API.
func (srv *Service) List(ctx context.Context, req *pc.ListRequest) (*pc.ListResponse, error) {
	var s = srv.Resolver.state

	var resp = &pc.ListResponse{
		Status: pc.Status_OK,
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
		var shard = pc.ListResponse_Shard{
			Spec: *s.Items[cur.Left].Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)}

		metaLabels = pc.ExtractShardSpecMetaLabels(&shard.Spec, metaLabels)
		allLabels = pb.UnionLabelSets(metaLabels, shard.Spec.LabelSet, allLabels)

		if !req.Selector.Matches(allLabels) {
			continue
		}
		shard.ModRevision = s.Items[cur.Left].Raw.ModRevision
		shard.Route.Init(s.Assignments[cur.RightBegin:cur.RightEnd])
		shard.Route.AttachEndpoints(s.KS)

		for _, asn := range s.Assignments[cur.RightBegin:cur.RightEnd] {
			shard.Status = append(shard.Status,
				*asn.Decoded.(allocator.Assignment).AssignmentValue.(*pc.ReplicaStatus))
		}

		resp.Shards = append(resp.Shards, shard)
	}
	return resp, nil
}

// Apply dispatches the ShardServer.Apply API.
func (srv *Service) Apply(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	var s = srv.Resolver.state

	var resp = &pc.ApplyResponse{
		Status: pc.Status_OK,
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
		resp.Status = pc.Status_ETCD_TRANSACTION_FAILED
	} else {
		// Delay responding until we have read our own Etcd write.
		s.KS.Mu.RLock()
		err = s.KS.WaitForRevision(ctx, txnResp.Txn().Header.Revision)
		s.KS.Mu.RUnlock()
	}
	return resp, err
}

// GetHints dispatches the ShardServer.Hints API.
func (srv *Service) GetHints(ctx context.Context, req *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {
	var (
		resp = &pc.GetHintsResponse{
			Status: pc.Status_OK,
			Header: pb.NewUnroutedHeader(srv.State),
		}
		ks   = srv.State.KS
		spec *pc.ShardSpec
	)

	ks.Mu.RLock()
	var item, ok = allocator.LookupItem(ks, req.Shard.String())
	ks.Mu.RUnlock()
	if !ok {
		resp.Status = pc.Status_SHARD_NOT_FOUND
		return resp, nil
	}
	spec = item.ItemValue.(*pc.ShardSpec)

	var h, err = fetchHints(ctx, spec, srv.Etcd)
	if err != nil {
		return nil, err
	}

	if h.hints[0] != nil {
		resp.PrimaryHints = pc.GetHintsResponse_ResponseHints{
			Hints: h.hints[0],
		}
	}

	if len(h.hints) > 1 {
		for _, hints := range h.hints[1:] {
			resp.BackupHints = append(resp.BackupHints, pc.GetHintsResponse_ResponseHints{
				Hints: hints,
			})
		}
	}
	return resp, nil
}

// ListShards invokes the List RPC, and maps a validation or !OK status to an error.
func ListShards(ctx context.Context, sc pc.ShardClient, req *pc.ListRequest) (*pc.ListResponse, error) {
	if r, err := sc.List(pb.WithDispatchDefault(ctx), req, grpc.WaitForReady(true)); err != nil {
		return r, err
	} else if err = r.Validate(); err != nil {
		return r, err
	} else if r.Status != pc.Status_OK {
		return r, errors.New(r.Status.String())
	} else {
		return r, nil
	}
}

// StatShard invokes the Stat RPC, and maps a validation or !OK status to an error.
func StatShard(ctx context.Context, rc pc.RoutedShardClient, req *pc.StatRequest) (*pc.StatResponse, error) {
	var routedCtx = pb.WithDispatchItemRoute(ctx, rc, req.Shard.String(), false)
	if r, err := rc.Stat(routedCtx, req, grpc.WaitForReady(true)); err != nil {
		return r, err
	} else if err = r.Validate(); err != nil {
		return r, err
	} else if r.Status != pc.Status_OK {
		return r, errors.New(r.Status.String())
	} else {
		return r, nil
	}

}

// ApplyShards invokes the Apply RPC.
func ApplyShards(ctx context.Context, sc pc.ShardClient, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return ApplyShardsInBatches(ctx, sc, req, 0)
}

// ApplyShardsInBatches applies changes to shards which
// may be larger than the configured etcd transaction size size. The changes in
// |req| will be sent serially in batches of size |size|. If
// |size| is 0 all changes will be attempted as part of a single
// transaction. This function will return the response of the final
// ShardClient.Apply call. Response validation or !OK status from Apply RPC are
// mapped to error.
func ApplyShardsInBatches(ctx context.Context, sc pc.ShardClient, req *pc.ApplyRequest, size int) (*pc.ApplyResponse, error) {
	if size == 0 {
		size = len(req.Changes)
	}
	var offset = 0

	for {
		var r *pc.ApplyRequest
		if len(req.Changes[offset:]) > size {
			r = &pc.ApplyRequest{Changes: req.Changes[offset : offset+size]}
		} else {
			r = &pc.ApplyRequest{Changes: req.Changes[offset:]}
		}

		var resp, err = sc.Apply(pb.WithDispatchDefault(ctx), r, grpc.WaitForReady(true))
		if err != nil {
			return resp, err
		} else if err = resp.Validate(); err != nil {
			return resp, err
		} else if resp.Status != pc.Status_OK {
			return resp, errors.New(resp.Status.String())
		}

		if offset += len(r.Changes); offset == len(req.Changes) {
			return resp, nil
		}
	}
}

// FetchHints invokes the Hints RPC, and maps a validation or !OK status to an error.
func FetchHints(ctx context.Context, sc pc.ShardClient, req *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {
	if r, err := sc.GetHints(pb.WithDispatchDefault(ctx), req, grpc.WaitForReady(true)); err != nil {
		return r, err
	} else if err = r.Validate(); err != nil {
		return r, err
	} else if r.Status != pc.Status_OK {
		return r, errors.New(r.Status.String())
	} else {
		return r, nil
	}
}
