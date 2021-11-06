package flow

import (
	"context"
	"fmt"
	"math"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/protocols/flow"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
)

// ListShardsRequest builds a ListRequest of the Task's shards.
func ListShardsRequest(task pf.Task) pc.ListRequest {
	return pc.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet(
				labels.TaskName, task.TaskName(),
				labels.TaskType, taskType(task),
			),
		},
	}
}

// ListShardsAtBuildRequest builds a ListRequest of the Task's shards which are at the given |buildID|.
func ListShardsAtBuildRequest(task pf.Task, buildID string) pc.ListRequest {
	return pc.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet(
				labels.Build, buildID,
				labels.TaskName, task.TaskName(),
				labels.TaskType, taskType(task),
			),
		},
	}
}

// ListRecoveryLogsRequest builds a ListRequest of the Tasks's recovery logs.
func ListRecoveryLogsRequest(task pf.Task) pb.ListRequest {
	return pb.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet(
				labels.TaskName, task.TaskName(),
				labels.TaskType, taskType(task),
			),
		},
	}
}

// ListPartitionsRequest builds a ListRequest of the collection's partitions.
func ListPartitionsRequest(collection *pf.CollectionSpec) pb.ListRequest {
	return pb.ListRequest{
		Selector: pf.LabelSelector{
			Include: pb.MustLabelSet(labels.Collection, collection.Collection.String()),
		},
	}
}

// ListPartitionsAtBuildRequest builds a ListRequest of the collection's partitions at the given |buildID|.
func ListPartitionsAtBuildRequest(collection *pf.CollectionSpec, buildID string) pb.ListRequest {
	return pb.ListRequest{
		Selector: pf.LabelSelector{
			Include: pb.MustLabelSet(
				labels.Build, buildID,
				labels.Collection, collection.Collection.String(),
			),
		},
	}
}

// MapPartitionsToCurrentSplits passes through current labels of existing partitions.
func MapPartitionsToCurrentSplits(partitions []pb.ListResponse_Journal) []pf.LabelSet {
	var out []pf.LabelSet
	for _, p := range partitions {
		out = append(out, p.Spec.LabelSet)
	}
	return out
}

// MapShardsToCurrentOrInitialSplits passes through current labels of existing shards.
// If no shards exist, then initial label splits are returned which evenly subdivide
// the key range into |initialSplits| chunks.
func MapShardsToCurrentOrInitialSplits(shards []pc.ListResponse_Shard, initialSplits int) []pf.LabelSet {
	var out []pf.LabelSet

	if len(shards) != 0 {
		for _, s := range shards {
			out = append(out, s.Spec.LabelSet)
		}
		return out
	}

	for p := 0; p != initialSplits; p++ {
		out = append(out,
			labels.EncodeRange(pf.RangeSpec{
				KeyBegin:    uint32((1 << 32) * (p + 0) / initialSplits),
				KeyEnd:      uint32(((1 << 32) * (p + 1) / initialSplits) - 1),
				RClockBegin: 0,
				RClockEnd:   math.MaxUint32,
			}, pf.LabelSet{}),
		)
	}

	return out
}

// MapShardToSplit maps a single shard contained in the |shards| response to a
// desired split state, where the shard is evenly subdivided on either key or
// r-clock (depending on the value of |splitOnKey|).
func MapShardToSplit(task pf.Task, shards []pc.ListResponse_Shard, splitOnKey bool) ([]pf.LabelSet, error) {
	if len(shards) != 1 {
		return nil, fmt.Errorf("expected exactly one shard in the response")
	}
	var parent = shards[0].Spec

	// Confirm the shard doesn't have a current split.
	if l := parent.LabelSet.ValuesOf(labels.SplitSource); len(l) != 0 {
		return nil, fmt.Errorf("shard %s is already splitting from source %s", parent.Id, l[0])
	}
	if l := parent.LabelSet.ValuesOf(labels.SplitTarget); len(l) != 0 {
		return nil, fmt.Errorf("shard %s is already splitting into target %s", parent.Id, l[0])
	}

	// Pick a split point of the parent range, which will divide the future
	// LHS & RHS children.
	var parentRange, err = labels.ParseRangeSpec(parent.LabelSet)
	if err != nil {
		return nil, fmt.Errorf("parsing range spec: %w", err)
	}
	var lhsRange, rhsRange = parentRange, parentRange

	if splitOnKey {
		var pivot = uint32((uint64(parentRange.KeyBegin) + uint64(parentRange.KeyEnd) + 1) / 2)
		lhsRange.KeyEnd, rhsRange.KeyBegin = pivot-1, pivot
	} else {
		var pivot = uint32((uint64(parentRange.RClockBegin) + uint64(parentRange.RClockEnd) + 1) / 2)
		lhsRange.RClockEnd, rhsRange.RClockBegin = pivot-1, pivot
	}

	// Deep-copy parent labels for the desired LHS / RHS updates.
	var lhs = pf.LabelSet{Labels: append([]pf.Label(nil), parent.Labels...)}
	var rhs = pf.LabelSet{Labels: append([]pf.Label(nil), parent.Labels...)}

	rhs = labels.EncodeRange(rhsRange, rhs)

	// We don't update the |lhs| range at this time.
	// That will happen when the |rhs| shard finishes playback
	// and completes the split workflow.

	// Determine what the RHS child's shard ID will be.
	rhsSuffix, err := labels.ShardSuffix(rhs)
	if err != nil {
		return nil, fmt.Errorf("building RHS shard suffix: %w", err)
	}
	var rhsId = task.TaskShardTemplate().Id.String() + "/" + rhsSuffix

	// Mark the parent & child specs as having an in-progress split.
	lhs.SetValue(labels.SplitTarget, rhsId)
	rhs.SetValue(labels.SplitSource, parent.Id.String())

	return []pf.LabelSet{lhs, rhs}, nil
}

// CollectionChanges compares a CollectionSpec and |desiredSplits| with the
// collection's |curPartitions|, and appends proposed JournalChanges
// which bring the current state into consistency with the desired state.
func CollectionChanges(
	collection *pf.CollectionSpec,
	curPartitions []pb.ListResponse_Journal,
	desiredSplits []pf.LabelSet,
	into []pb.ApplyRequest_Change,
) ([]pb.ApplyRequest_Change, error) {
	var idx = make(map[pf.Journal]*pb.ListResponse_Journal, len(curPartitions))
	for i := range curPartitions {
		idx[curPartitions[i].Spec.Name] = &curPartitions[i]
	}

	for _, d := range desiredSplits {
		var next, err = BuildPartitionSpec(collection.PartitionTemplate, d)
		if err != nil {
			return nil, fmt.Errorf("building journal spec: %w", err)
		}

		var cur, ok = idx[next.Name]
		if ok && cur == nil {
			return nil, fmt.Errorf("duplicate desired partition journal %s", next.Name)
		} else if ok {
			idx[next.Name] = nil

			if !next.Equal(&cur.Spec) {
				into = append(into, pb.ApplyRequest_Change{
					Upsert:            next,
					ExpectModRevision: cur.ModRevision,
				})
			}
		} else {
			into = append(into, pb.ApplyRequest_Change{
				Upsert:            next,
				ExpectModRevision: 0, // Expected to not exist.
			})
		}
	}

	// Journals still in |idx| were not in |desired| and must be removed.
	for _, cur := range idx {
		if cur == nil {
			continue // Already in |updates|.
		}

		into = append(into, pb.ApplyRequest_Change{
			Delete:            cur.Spec.Name,
			ExpectModRevision: cur.ModRevision,
		})
	}

	return into, nil
}

// TaskChanges compares a Task and |desiredSplits| with the tasks's |curShards|
// and |curRecoveryLogs|, and appends proposed shard and journal changes
// which bring the current state into consistency with the desired state.
func TaskChanges(
	task pf.Task,
	curShards []pc.ListResponse_Shard,
	curRecoveryLogs []pb.ListResponse_Journal,
	desiredSplits []pf.LabelSet,
	intoShards []pc.ApplyRequest_Change,
	intoJournals []pb.ApplyRequest_Change,
) ([]pc.ApplyRequest_Change, []pb.ApplyRequest_Change, error) {

	var shardIdx = make(map[pc.ShardID]*pc.ListResponse_Shard, len(curShards))
	var logIdx = make(map[pb.Journal]*pb.ListResponse_Journal, len(curRecoveryLogs))

	for i := range curShards {
		shardIdx[curShards[i].Spec.Id] = &curShards[i]
	}
	for i := range curRecoveryLogs {
		logIdx[curRecoveryLogs[i].Spec.Name] = &curRecoveryLogs[i]
	}

	for _, d := range desiredSplits {
		var nextShard, err = BuildShardSpec(task.TaskShardTemplate(), d)
		if err != nil {
			return nil, nil, fmt.Errorf("building shard spec: %w", err)
		}
		var nextLog = BuildRecoverySpec(task.TaskRecoveryLogTemplate(), nextShard)

		var curShard, ok = shardIdx[nextShard.Id]
		if ok && curShard == nil {
			return nil, nil, fmt.Errorf("duplicate desired shard %s", nextShard.Id)
		} else if ok {
			shardIdx[nextShard.Id] = nil

			if !nextShard.Equal(&curShard.Spec) {
				intoShards = append(intoShards, pc.ApplyRequest_Change{
					Upsert:            nextShard,
					ExpectModRevision: curShard.ModRevision,
				})
			}
		} else {
			intoShards = append(intoShards, pc.ApplyRequest_Change{
				Upsert:            nextShard,
				ExpectModRevision: 0, // Expected to not exist.
			})
		}

		curLog, ok := logIdx[nextLog.Name]
		if ok && curLog == nil {
			panic("duplicate recovery log; cannot be reached, because it's also a duplicate shard")
		} else if ok {
			logIdx[nextLog.Name] = nil

			if !nextLog.Equal(&curLog.Spec) {
				intoJournals = append(intoJournals, pb.ApplyRequest_Change{
					Upsert:            nextLog,
					ExpectModRevision: curLog.ModRevision,
				})
			}
		} else {
			intoJournals = append(intoJournals, pb.ApplyRequest_Change{
				Upsert:            nextLog,
				ExpectModRevision: 0, // Expected to not exist.
			})
		}
	}

	// Shards still in |shardIdx| were not in |desired| and must be removed.
	for _, cur := range shardIdx {
		if cur == nil {
			continue // Already in |shardOut.Updates|.
		}

		intoShards = append(intoShards, pc.ApplyRequest_Change{
			Delete:            cur.Spec.Id,
			ExpectModRevision: cur.ModRevision,
		})
	}

	// Journals still in |logIdx| were not in |desired| and must be removed.
	for _, cur := range logIdx {
		if cur == nil {
			continue // Already in |logOut.Updates|.
		}

		intoJournals = append(intoJournals, pb.ApplyRequest_Change{
			Delete:            cur.Spec.Name,
			ExpectModRevision: cur.ModRevision,
		})
	}

	return intoShards, intoJournals, nil
}

// ActivationChanges enumerates all shard and journal changes required to bring
// a current data-plane state into consistency with the desired state of each
// of the specified, activated collections and tasks.
func ActivationChanges(
	ctx context.Context,
	jc pb.JournalClient,
	sc pc.ShardClient,
	collections []*pf.CollectionSpec,
	tasks []pf.Task,
	initialTaskSplits int,
) ([]pc.ApplyRequest_Change, []pb.ApplyRequest_Change, error) {

	var shards []pc.ApplyRequest_Change
	var journals []pb.ApplyRequest_Change

	// TODO(johnny): We could parallelize this by scattering / gathering list requests.

	for _, collection := range collections {
		var resp, err = client.ListAllJournals(ctx, jc, ListPartitionsRequest(collection))
		if err != nil {
			return nil, nil, fmt.Errorf("listing partitions of %s: %w", collection.Collection, err)
		}

		var desired = MapPartitionsToCurrentSplits(resp.Journals)
		journals, err = CollectionChanges(collection, resp.Journals, desired, journals)

		if err != nil {
			return nil, nil, fmt.Errorf("processing collection %s: %w", collection.Collection, err)
		}
	}

	for _, task := range tasks {
		var shardsReq = ListShardsRequest(task)
		var logsReq = ListRecoveryLogsRequest(task)

		shardsResp, err := consumer.ListShards(ctx, sc, &shardsReq)
		if err != nil {
			return nil, nil, fmt.Errorf("listing shards of %s: %w", task.TaskName(), err)
		}
		logsResp, err := client.ListAllJournals(ctx, jc, logsReq)
		if err != nil {
			return nil, nil, fmt.Errorf("listing recovery logs of %s: %w", task.TaskName(), err)
		}

		var desired = MapShardsToCurrentOrInitialSplits(shardsResp.Shards, initialTaskSplits)
		shards, journals, err = TaskChanges(
			task, shardsResp.Shards, logsResp.Journals, desired, shards, journals)

		if err != nil {
			return nil, nil, fmt.Errorf("processing task %s: %w", task.TaskName(), err)
		}
	}

	return shards, journals, nil
}

// DeletionChanges enumerates all shard and journal changes required to bring
// a current data-plane state into consistency with the deletion of each of the
// specified collections and tasks, expected to be at |buildID|. If a task ShardSpec
// or partition JournalSpec isn't at |buildID|, then no deletion change is generated.
func DeletionChanges(
	ctx context.Context,
	jc pb.JournalClient,
	sc pc.ShardClient,
	collections []*pf.CollectionSpec,
	tasks []pf.Task,
	buildID string,
) ([]pc.ApplyRequest_Change, []pb.ApplyRequest_Change, error) {

	var shards []pc.ApplyRequest_Change
	var journals []pb.ApplyRequest_Change

	// TODO(johnny): We could parallelize this by scattering / gathering list requests.

	for _, collection := range collections {
		var resp, err = client.ListAllJournals(ctx, jc, ListPartitionsAtBuildRequest(collection, buildID))
		if err != nil {
			return nil, nil, fmt.Errorf("listing partitions of %s: %w", collection.Collection, err)
		}

		for _, cur := range resp.Journals {
			journals = append(journals, pb.ApplyRequest_Change{
				Delete:            cur.Spec.Name,
				ExpectModRevision: cur.ModRevision,
			})
		}
	}

	for _, task := range tasks {
		var shardsReq = ListShardsAtBuildRequest(task, buildID)
		var logsReq = ListRecoveryLogsRequest(task)

		shardsResp, err := consumer.ListShards(ctx, sc, &shardsReq)
		if err != nil {
			return nil, nil, fmt.Errorf("listing shards of %s: %w", task.TaskName(), err)
		}
		logsResp, err := client.ListAllJournals(ctx, jc, logsReq)
		if err != nil {
			return nil, nil, fmt.Errorf("listing recovery logs of %s: %w", task.TaskName(), err)
		}

		var logsIdx = make(map[pf.Journal]pb.ListResponse_Journal, len(logsResp.Journals))
		for _, cur := range logsResp.Journals {
			logsIdx[cur.Spec.Name] = cur
		}

		for _, cur := range shardsResp.Shards {
			shards = append(shards, pc.ApplyRequest_Change{
				Delete:            cur.Spec.Id,
				ExpectModRevision: cur.ModRevision,
			})
			// If we're removing a shard, we remove its recovery log regardless of
			// whether it's converged to the shard's same build ID. It definitely
			// *should* be, but we don't allow the recovery log to dangle either way.

			if log, ok := logsIdx[cur.Spec.RecoveryLog()]; ok {
				journals = append(journals, pb.ApplyRequest_Change{
					Delete:            log.Spec.Name,
					ExpectModRevision: log.ModRevision,
				})
			}
		}
	}
	return shards, journals, nil
}

// taskType returns the label matching this Task.
func taskType(task pf.Task) string {
	switch task.(type) {
	case *pf.CaptureSpec:
		return labels.TaskTypeCapture
	case *pf.DerivationSpec:
		return labels.TaskTypeDerivation
	case *pf.MaterializationSpec:
		return labels.TaskTypeMaterialization
	default:
		panic(task)
	}
}
