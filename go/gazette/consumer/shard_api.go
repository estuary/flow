package consumer

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
	"google.golang.org/grpc"
)

// ShardStat is the default implementation of the ShardServer.Stat API.
func ShardStat(ctx context.Context, srv *Service, req *pc.StatRequest) (*pc.StatResponse, error) {
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

// ShardList is the default implementation of the ShardServer.List API.
func ShardList(ctx context.Context, srv *Service, req *pc.ListRequest) (*pc.ListResponse, error) {
	var s = srv.Resolver.state

	if err := req.Validate(); err != nil {
		return nil, err
	}

	defer s.KS.Mu.RUnlock()
	s.KS.Mu.RLock()

	var resp = &pc.ListResponse{
		Status: pc.Status_OK,
		Header: pbx.NewUnroutedHeader(s),
	}
	var scratch pb.LabelSet

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

		// LabelSetExt() truncates `scratch` while re-using its storage.
		scratch = shard.Spec.LabelSetExt(scratch)

		if !req.Selector.Matches(scratch) {
			continue
		}
		shard.ModRevision = s.Items[cur.Left].Raw.ModRevision
		shard.CreateRevision = s.Items[cur.Left].Raw.CreateRevision
		pbx.Init(&shard.Route, s.Assignments[cur.RightBegin:cur.RightEnd])
		pbx.AttachEndpoints(&shard.Route, s.KS)

		for _, asn := range s.Assignments[cur.RightBegin:cur.RightEnd] {
			shard.Status = append(shard.Status,
				*asn.Decoded.(allocator.Assignment).AssignmentValue.(*pc.ReplicaStatus))
		}

		resp.Shards = append(resp.Shards, shard)
	}
	return resp, nil
}

// ShardApply is the default implementation of the ShardServer.Apply API.
func ShardApply(ctx context.Context, srv *Service, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	var s = srv.Resolver.state

	if err := req.Validate(); err != nil {
		return nil, err
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
		// Allow caller to explicitly ignore revision comparison
		// by passing a value of -1 for revision.
		if changes.ExpectModRevision != -1 {
			cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(key), "=", changes.ExpectModRevision))
		}
	}

	s.KS.Mu.RLock()
	var resp = &pc.ApplyResponse{
		Status: pc.Status_OK,
		Header: pbx.NewUnroutedHeader(s),
	}
	s.KS.Mu.RUnlock()

	var txnResp, err = srv.Etcd.Do(ctx, clientv3.OpTxn(cmp, ops, nil))
	if err != nil {
		// Pass.
	} else if !txnResp.Txn().Succeeded {
		resp.Status = pc.Status_ETCD_TRANSACTION_FAILED
	} else if len(ops) != 0 {
		// If we made changes, delay responding until we have read our own Etcd write.
		s.KS.Mu.RLock()
		err = s.KS.WaitForRevision(ctx, txnResp.Txn().Header.Revision)
		s.KS.Mu.RUnlock()
	}
	resp.Header.Etcd.Revision = txnResp.Txn().Header.Revision
	return resp, err
}

// ShardGetHints is the default implementation of the ShardServer.Hints API.
func ShardGetHints(ctx context.Context, srv *Service, req *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {

	srv.State.KS.Mu.RLock()
	var resp = &pc.GetHintsResponse{
		Status: pc.Status_OK,
		Header: pbx.NewUnroutedHeader(srv.State),
	}
	var item, ok = allocator.LookupItem(srv.State.KS, req.Shard.String())
	srv.State.KS.Mu.RUnlock()

	if !ok {
		resp.Status = pc.Status_SHARD_NOT_FOUND
		return resp, nil
	}
	var spec = item.ItemValue.(*pc.ShardSpec)

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

func ShardUnassign(ctx context.Context, srv *Service, req *pc.UnassignRequest) (*pc.UnassignResponse, error) {
	var resp = &pc.UnassignResponse{
		Status: pc.Status_OK,
		Shards: make([]pc.ShardID, 0),
	}

	if err := req.Validate(); err != nil {
		return resp, err
	}

	var state = srv.Resolver.state
	defer state.KS.Mu.RUnlock()
	state.KS.Mu.RLock()

	var cmp []clientv3.Cmp
	var ops []clientv3.Op

	for _, shard := range req.Shards {
		var assignments = state.Assignments.Prefixed(allocator.ItemAssignmentsPrefix(state.KS, shard.String()))

		for _, kv := range assignments {
			var assignment = kv.Decoded.(allocator.Assignment)
			var status = assignment.AssignmentValue.(*pc.ReplicaStatus)

			if status.Code == pc.ReplicaStatus_FAILED {
				// Always remove a FAILED assignment.
			} else if !req.OnlyFailed && assignment.Slot == 0 {
				// We'll remove a non-failed PRIMARY assignment if asked to do so.
			} else {
				continue // Don't remove this assignment.
			}

			var key = allocator.AssignmentKey(state.KS, assignment)
			cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(key), "=", kv.Raw.ModRevision))
			ops = append(ops, clientv3.OpDelete(key))

			if l := len(resp.Shards); l == 0 || resp.Shards[l-1] != shard {
				resp.Shards = append(resp.Shards, shard)
			}
		}
	}

	if req.DryRun {
		return resp, nil
	}

	etcdResp, err := srv.Etcd.Txn(ctx).If(cmp...).Then(ops...).Commit()
	if err != nil {
		return resp, fmt.Errorf("executing etcd transaction: %w", err)
	} else if !etcdResp.Succeeded {
		resp.Status = pc.Status_ETCD_TRANSACTION_FAILED
	} else if len(ops) != 0 {
		// If we made changes, delay responding until we have read our own Etcd write.
		err = state.KS.WaitForRevision(ctx, etcdResp.Header.Revision)
	}

	return resp, err
}

// ListShards is a convenience for invoking the List RPC, which maps a validation or !OK status to an error.
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

// StatShard is a convenience for invoking the Stat RPC, which maps a validation or !OK status to an error.
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

// VerifyReferencedJournals ensures the referential integrity of journals
// (sources and recovery logs, and their content types) referenced by Shards
// of the ApplyRequest. It returns a descriptive error if any invalid
// references are found.
func VerifyReferencedJournals(ctx context.Context, jc pb.JournalClient, req *pc.ApplyRequest) error {
	var cache = make(map[pb.Journal]*pb.JournalSpec)

	var lookup = func(name pb.Journal) (*pb.JournalSpec, error) {
		if spec, ok := cache[name]; ok {
			return spec, nil
		} else if spec, err := client.GetJournal(ctx, jc, name); err != nil {
			return nil, err
		} else {
			cache[name] = spec
			return spec, nil
		}
	}

	for _, change := range req.Changes {
		if change.Upsert == nil {
			continue
		}
		// Verify shard sources exist with appropriate framed content-types.
		for _, src := range change.Upsert.Sources {
			if spec, err := lookup(src.Journal); err != nil {
				return errors.WithMessagef(err, "Shard[%s]", change.Upsert.Id)
			} else if _, err = message.FramingByContentType(spec.LabelSet.ValueOf(labels.ContentType)); err != nil {
				return errors.WithMessagef(err, "Shard[%s].Sources[%s] message framing", change.Upsert.Id, src.Journal)
			}
		}

		// Verify shard recovery log exists with correct content-type.
		if name := change.Upsert.RecoveryLog(); name == "" {
			// Shard doesn't use a recovery log.
		} else if spec, err := lookup(name); err != nil {
			return errors.WithMessagef(err, "Shard[%s]", change.Upsert.Id)
		} else if ct := spec.LabelSet.ValueOf(labels.ContentType); ct != labels.ContentType_RecoveryLog {
			return errors.Errorf("Shard[%s]: expected %s to have %s %s but was '%s'",
				change.Upsert.Id, change.Upsert.RecoveryLog(), labels.ContentType, labels.ContentType_RecoveryLog, ct)
		}
	}
	return nil
}

// ApplyShards applies shard changes detailed in the ApplyRequest via the consumer Apply RPC.
// Changes are applied as a single Etcd transaction. If the change list is larger than an
// Etcd transaction can accommodate, ApplyShardsInBatches should be used instead.
// ApplyResponse statuses other than OK are mapped to an error.
func ApplyShards(ctx context.Context, sc pc.ShardClient, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return ApplyShardsInBatches(ctx, sc, req, 0)
}

// ApplyShardsInBatches is like ApplyShards, but chunks the ApplyRequest
// into batches of the given size, which should be less than Etcd's maximum
// configured transaction size (usually 128). If size is 0 all changes will
// be attempted in a single transaction. Be aware that ApplyShardsInBatches
// may only partially succeed, with some batches having applied and others not.
// The final ApplyResponse is returned, unless an error occurs.
// ApplyResponse statuses other than OK are mapped to an error.
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

// FetchHints is a convenience for invoking the GetHints RPC, which maps a response validation or !OK status to an error.
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
