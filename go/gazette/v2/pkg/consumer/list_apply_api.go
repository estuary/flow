package consumer

import (
	"context"
	"errors"
	"strings"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
)

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

	if txnResp, err := srv.etcd.Do(ctx, clientv3.OpTxn(cmp, ops, nil)); err != nil {
		return resp, err
	} else if !txnResp.Txn().Succeeded {
		resp.Status = Status_ETCD_TRANSACTION_FAILED
	} else {
		// Delay responding until we have read our own Etcd write.
		s.KS.Mu.RLock()
		s.KS.WaitForRevision(ctx, txnResp.Txn().Header.Revision)
		s.KS.Mu.RUnlock()
	}

	return resp, nil
}

// ListShards invokes the List RPC, and maps a validation or !OK status to an error.
func ListShards(ctx context.Context, sc ShardClient, req *ListRequest) (*ListResponse, error) {
	if r, err := sc.List(pb.WithDispatchDefault(ctx), req); err != nil {
		return r, err
	} else if err = r.Validate(); err != nil {
		return r, err
	} else if r.Status != Status_OK {
		return r, errors.New(r.Status.String())
	} else {
		return r, nil
	}
}

// ApplyShards invokes the Apply RPC, and maps a validation or !OK status to an error.
func ApplyShards(ctx context.Context, sc ShardClient, req *ApplyRequest) (*ApplyResponse, error) {
	if r, err := sc.Apply(pb.WithDispatchDefault(ctx), req); err != nil {
		return r, err
	} else if err = r.Validate(); err != nil {
		return r, err
	} else if r.Status != Status_OK {
		return r, errors.New(r.Status.String())
	} else {
		return r, nil
	}
}
