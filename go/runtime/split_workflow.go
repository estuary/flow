package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/estuary/flow/go/labels"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
)

// shardGetHints delegates to consumer.ShardGetHints, with one twist:
// If the shard has a labels.SplitSource then hints for *that* shard ID
// are fetched instead of hints for the present shard. In this case,
// shardGetHints will block until assignments of the source shard are
// "ready" (have a PRIMARY and all replicas are STANDBY), and will return
// the mvccpb.KeyValue of the PRIMARY assignment in its response extension.
func shardGetHints(ctx context.Context, claims pb.Claims, svc *consumer.Service, req *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {

	// Inspect the status of the split, extracting a ready LHS primary if
	// there is one. This may block indefinitely if |req| references a
	// splitting RHS shard but its LHS (parent) shard isn't ready.
	//
	// Use a closure for simpler deferred Unlock semantics.
	var lhsPrimary, err = func() (keyspace.KeyValue, error) {
		svc.State.KS.Mu.RLock()
		defer svc.State.KS.Mu.RUnlock()

		for {
			var lhs, lhsPrimary, _, err = splitStatus(svc.State, req.Shard)

			switch err {
			case errNotSplitting:
				return keyspace.KeyValue{}, nil // No-op.

			case errLHSNotReady:
				// We must block until the LHS shard is ready
				// before allowing the splitting child to proceed.
				if err := svc.State.KS.WaitForRevision(ctx, svc.State.KS.Header.Revision+1); err != nil {
					return keyspace.KeyValue{}, err
				}
				continue

			case nil:
				var lhsSpec = lhs.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)
				log.WithFields(log.Fields{
					"rhs": req.Shard,
					"lhs": lhsSpec.Id,
				}).Info("using LHS (parent) hints of splitting shard")

				req.Shard = lhsSpec.Id
				return lhsPrimary, nil

			default:
				// A non-handled error was encountered.
				return keyspace.KeyValue{}, err
			}
		}
	}()
	if err != nil {
		return nil, err
	}

	resp, err := consumer.ShardGetHints(ctx, claims, svc, req)
	if err != nil {
		return nil, err
	}

	if lhsPrimary.Decoded == nil {
		// No-op.
	} else if resp.Extension, err = lhsPrimary.Raw.Marshal(); err != nil {
		return nil, fmt.Errorf("proto.Marshal(lhsPrimary): %w", err)
	}

	return resp, nil
}

func splitStatus(state *allocator.State, rhsID pc.ShardID) (
	lhs, lhsPrimary, rhs keyspace.KeyValue,
	err error,
) {
	// Fetch ShardSpec of RHS shard.
	var rhsInd, ok = state.Items.Search(allocator.ItemKey(state.KS, rhsID.String()))
	if !ok {
		// It's clearly an error for |rhsID| to be missing, but we return
		// errNotSplitting to short-circuit the splitting workflow and let
		// delegated handlers return a proper error.
		err = errNotSplitting
		return
	}
	rhs = state.Items[rhsInd]
	var rhsSpec = rhs.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)

	// Map through the RHS split-source to the LHS shard.
	var rhsSource = rhsSpec.LabelSet.ValuesOf(labels.SplitSource)
	if len(rhsSource) == 0 {
		err = errNotSplitting
		return
	}

	// Fetch ShardSpec of LHS shard.
	lhsInd, ok := state.Items.Search(allocator.ItemKey(state.KS, rhsSource[0]))
	if !ok {
		err = fmt.Errorf("shard %s, which is %s of %s, doesn't exist",
			rhsSource[0], labels.SplitSource, rhsID)
	}
	lhs = state.Items[lhsInd]
	var lhsSpec = lhs.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)

	// Fetch current LHS shard assignments.
	var lhsAssignments = state.KS.KeyValues.Prefixed(
		allocator.ItemAssignmentsPrefix(state.KS, lhsSpec.Id.String()))

	// Extract LHS primary assignment and confirm others are STANDBY
	// and not BACKFILL-ing.
	var lhsIsReady = len(lhsAssignments) == lhsSpec.DesiredReplication()

	for _, kv := range lhsAssignments {
		var asn = kv.Decoded.(allocator.Assignment)
		var status = asn.AssignmentValue.(*pc.ReplicaStatus)

		if asn.Slot == 0 && status.Code == pc.ReplicaStatus_PRIMARY {
			lhsPrimary = kv
		} else if asn.Slot == 0 {
			lhsIsReady = false // Still recovering.
		} else if status.Code != pc.ReplicaStatus_STANDBY {
			lhsIsReady = false
		}
	}

	if !lhsIsReady {
		err = errLHSNotReady
	}

	return
}

func CompleteSplit(svc *consumer.Service, shard consumer.Shard, rec *recoverylog.Recorder) error {
	// Fetch out LHS & RHS shards and the LHS & RHS primary assignment.
	var lhs, lhsPrimary, rhs, err = splitStatus(svc.State, shard.Spec().Id)
	if err == errNotSplitting {
		return nil // No-op.
	} else if err == errLHSNotReady {
		// At this point, we ignore a failure or handoff of a LHS STANDBY
		// shard so long as the LHS primary assignment is still intact.
	} else if err != nil {
		return err
	}
	var rhsPrimary = shard.Assignment()

	// Earlier in the split workflow, shardGetHints encoded the LHS primary
	// assignment into the GetHints RPC response. This is the primary which
	// was active at the onset of this (RHS) shard's recovery. Extract it,
	// and confirm the |lhsPrimary| we see now hasn't changed.
	//
	// We do this because we require that the LHS primary is now a "zombie":
	// we fenced away its ability to write to the recovery log during the
	// recovery of this RHS shard, and the LHS primary is unable to commit
	// further transactions.
	//
	// If that's *not* the case then we must abort and try again because
	// the new LHS primary may have continued processing transactions
	// which we won't see in our recovered fork of the log.
	var recoveredPrimary mvccpb.KeyValue
	if ext := shard.RecoveredHints().Extension; ext == nil {
		return fmt.Errorf("expected recovered hints of split workflow to be extended with LHS assignment")
	} else if err := recoveredPrimary.Unmarshal(ext); err != nil {
		return fmt.Errorf("unmarshal recovered primary assignment: %w", err)
	} else if !bytes.Equal(lhsPrimary.Raw.Key, recoveredPrimary.Key) ||
		lhsPrimary.Raw.CreateRevision != recoveredPrimary.CreateRevision {
		return fmt.Errorf("lhs shard primary has changed (from %s to %s)",
			string(lhsPrimary.Raw.Key), string(recoveredPrimary.Key))
	}

	// We'll build up an Etcd transaction which is applied to effect the split.
	// If the transaction fails, no split occurs and the RHS shard must be
	// re-assigned to attempt the workflow again.
	var cmps []clientv3.Cmp
	var ops []clientv3.Op

	cmps = append(cmps,
		// Assert the LHS and RHS primary assignments continue to exist.
		clientv3.Compare(
			clientv3.CreateRevision(string(lhsPrimary.Raw.Key)),
			"=",
			lhsPrimary.Raw.CreateRevision),
		clientv3.Compare(
			clientv3.CreateRevision(string(rhsPrimary.Raw.Key)),
			"=",
			rhsPrimary.Raw.CreateRevision),

		// Also assert LHS & RHS ShardSpecs are unchanged from our view.
		clientv3.Compare(
			clientv3.ModRevision(string(lhs.Raw.Key)),
			"=",
			lhs.Raw.ModRevision),
		clientv3.Compare(
			clientv3.ModRevision(string(rhs.Raw.Key)),
			"=",
			rhs.Raw.ModRevision),
	)

	// Delete the LHS primary assignment.
	// As noted above, it's a zombie that's unable to commit transactions.
	// Removing its assignment allows a standby to take over
	// under an updated (split) LHS range.
	ops = append(ops, clientv3.OpDelete(string(recoveredPrimary.Key)))

	// Decode LHS & RHS specs, which we'll modify and update.
	var lhsSpec, rhsSpec pc.ShardSpec
	if err := lhsSpec.Unmarshal(lhs.Raw.Value); err != nil {
		return fmt.Errorf("decoding spec: %w", err)
	} else if err := rhsSpec.Unmarshal(rhs.Raw.Value); err != nil {
		return fmt.Errorf("decoding spec: %w", err)
	}
	var lhsLabels, rhsLabels = &lhsSpec.LabelSet, &rhsSpec.LabelSet

	// Narrow the LHS RangeSpec from the parent range to the split LHS child range.
	if lhsLabels.ValueOf(labels.KeyBegin) != rhsLabels.ValueOf(labels.KeyBegin) {
		// KeyBegin is different, use that to split
		rhsKeyBegin, err := labels.ParseHexU32Label(labels.KeyBegin, *rhsLabels)
		if err != nil {
			return fmt.Errorf("parse label KeyBegin: %w", err)
		}
		// Use rhsKeyBegin-1 as lhsKeyEnd.
		*lhsLabels = labels.EncodeHexU32Label(labels.KeyEnd, rhsKeyBegin-1, *lhsLabels)

	} else if lhsLabels.ValueOf(labels.RClockBegin) != rhsLabels.ValueOf(labels.RClockBegin) {
		// RClock is different, use that to split
		rhsRClockBegin, err := labels.ParseHexU32Label(labels.RClockBegin, *rhsLabels)
		if err != nil {
			return fmt.Errorf("parse label RClockBegin: %w", err)
		}
		// Use rhsRClockBegin-1 as lhsRClockEnd.
		*lhsLabels = labels.EncodeHexU32Label(labels.RClockEnd, rhsRClockBegin-1, *lhsLabels)

	} else {
		return fmt.Errorf("expect parent and child to differ on key or r-clock range")
	}

	// Remove split labels from parent & child specs.
	lhsLabels.Remove(labels.SplitTarget)
	rhsLabels.Remove(labels.SplitSource)

	// HotStandbys of the RHS was explicitly zero during the split,
	// as RHS replicas must begin reading from hints which we're only now able
	// to produce within this transaction.
	// Now update HotStandbys to the value of the former parent.
	rhsSpec.HotStandbys = lhsSpec.HotStandbys

	// Marshal updated LHS & RHS ShardSpecs.
	ops = append(ops,
		clientv3.OpPut(string(lhs.Raw.Key), lhsSpec.MarshalString()),
		clientv3.OpPut(string(rhs.Raw.Key), rhsSpec.MarshalString()),
	)

	// Create a file which is the first to be tracked in the forked log.
	// The file ensures there's a lower-bound hinted segment within the
	// new log, which in turn tells a future player of the log where to
	// cut over from the parent log to the forked log.
	var fnode = rec.RecordCreate(path.Join(rec.Dir(), ".split-from"))
	rec.RecordWriteAt(fnode, []byte(lhsSpec.Id), 0)

	// Build and persist primary hints of this RHS shard.
	// If we fault immediately after this transaction commits,
	// these will be recovered from and will honor the fork we've created.
	if hints, err := rec.BuildHints(); err != nil {
		return fmt.Errorf("building hints: %w", err)
	} else if b, err := hints.Marshal(); err != nil {
		return fmt.Errorf("hints.Marshal: %w", err)
	} else {
		ops = append(ops, clientv3.OpPut(shard.Spec().HintPrimaryKey(), string(b)))
	}

	txnResp, err := svc.Etcd.Txn(shard.Context()).
		If(cmps...).
		Then(ops...).
		Commit()

	if err == nil && !txnResp.Succeeded {
		err = errors.New(pc.Status_ETCD_TRANSACTION_FAILED.String())
	}
	if err != nil {
		return fmt.Errorf("etcd transaction: %w", err)
	}

	log.WithFields(log.Fields{
		"lhs": lhsSpec.Id,
		"rhs": rhsSpec.Id,
	}).Info("completed shard split")

	return nil
}

var (
	errNotSplitting = fmt.Errorf("not splitting")
	errLHSNotReady  = fmt.Errorf("source (LHS) shard is not ready to split")
)
