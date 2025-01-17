package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"path"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/estuary/flow/go/shuffle"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

type taskBase[TaskSpec pf.Task] struct {
	container    atomic.Pointer[pr.Container]            // Current Container of this shard, or nil.
	extractFn    func(*sql.DB, string) (TaskSpec, error) // Extracts a TaskSpec from a build DB.
	host         *FlowConsumer                           // Host Consumer application of the shard.
	opsCancel    context.CancelFunc                      // Cancels ops.Publisher context.
	opsPublisher *OpsPublisher                           // ops.Publisher of task ops.Logs and ops.Stats.
	recorder     *recoverylog.Recorder                   // Recorder of the shard's recovery log.
	svc          *bindings.TaskService                   // Associated Rust runtime service.
	term         taskTerm[TaskSpec]                      // Current task term.
	termCount    int                                     // Number of initialized task terms.
}

type taskTerm[TaskSpec pf.Task] struct {
	cancel    context.CancelFunc // Cancel the task term.
	ctx       context.Context    // Context for the task term.
	labels    ops.ShardLabeling  // Shard labels of this task term.
	shardSpec *pf.ShardSpec      // Term ShardSpec of the task.
	taskSpec  TaskSpec           // Term TaskSpec of the task.
}

type taskReader[TaskSpec pf.Task] struct {
	*taskBase[TaskSpec]

	coordinator *shuffle.Coordinator                // Coordinator of shuffled reads for this task.
	readBuilder atomic.Pointer[shuffle.ReadBuilder] // Builder of reads under the current term.
}

func newTaskBase[TaskSpec pf.Task](
	host *FlowConsumer,
	shard consumer.Shard,
	recorder *recoverylog.Recorder,
	extractFn func(*sql.DB, string) (TaskSpec, error),
) (*taskBase[TaskSpec], error) {

	var opsCtx, opsCancel = context.WithCancel(host.opsContext)
	opsCtx = pprof.WithLabels(opsCtx, pprof.Labels(
		"shard", shard.Spec().Id.String(), // Same label set by consumer framework.
	))
	var opsPublisher = NewOpsPublisher(message.NewPublisher(
		client.NewAppendService(opsCtx, host.service.Journals), nil))

	term, err := newTaskTerm[TaskSpec](nil, extractFn, host, opsPublisher, shard)
	if err != nil {
		return nil, err
	}

	svc, err := bindings.NewTaskService(
		pr.TaskServiceConfig{
			AllowLocal:       host.config.Flow.AllowLocal,
			ContainerNetwork: host.config.Flow.Network,
			TaskName:         term.labels.TaskName,
			UdsPath:          path.Join(recorder.Dir(), "socket"),
		},
		opsPublisher.PublishLog,
	)
	if err != nil {
		return nil, fmt.Errorf("creating task service: %w", err)
	}

	return &taskBase[TaskSpec]{
		container:    atomic.Pointer[pr.Container]{},
		extractFn:    extractFn,
		host:         host,
		opsCancel:    opsCancel,
		opsPublisher: opsPublisher,
		recorder:     recorder,
		svc:          svc,
		term:         *term,
	}, nil
}

func (t *taskBase[TaskSpec]) initTerm(shard consumer.Shard) error {
	var next, err = newTaskTerm[TaskSpec](&t.term, t.extractFn, t.host, t.opsPublisher, shard)
	if err != nil {
		return err
	}

	ops.PublishLog(t.opsPublisher, ops.Log_info,
		"initialized catalog task term",
		"nextLabels", next.labels,
		"prevLabels", t.term.labels,
		"assignment", shard.Assignment().Decoded,
	)

	t.term.cancel()
	t.term = *next
	t.termCount += 1

	return nil
}

func (t *taskBase[TaskSpec]) ProxyHook() (*pr.Container, ops.Publisher) {
	return t.container.Load(), t.opsPublisher
}

func (t *taskBase[TaskSpec]) drop() {
	t.svc.Drop()
}

func newTaskTerm[TaskSpec pf.Task](
	prev *taskTerm[TaskSpec],
	extractFn func(*sql.DB, string) (TaskSpec, error),
	host *FlowConsumer,
	publisher *OpsPublisher,
	shard consumer.Shard,
) (*taskTerm[TaskSpec], error) {
	var shardSpec = shard.Spec()

	// Create a term Context which is cancelled if:
	// - The shard's Context is cancelled, or
	// - The ShardSpec is updated.
	// A cancellation of the term's Context doesn't invalidate the shard,
	// but does mean the current task term is done and a new one should be started.
	var termCtx, termCancel = context.WithCancel(shard.Context())
	go signalOnSpecUpdate(termCtx, termCancel, host.service.State.KS, shard, shardSpec)

	var labels, err = labels.ParseShardLabels(shardSpec.LabelSet)
	if err != nil {
		return nil, fmt.Errorf("parsing task shard labels: %w", err)
	}

	var taskSpec TaskSpec

	if prev != nil && shardSpec == prev.shardSpec {
		taskSpec = prev.taskSpec
	} else {
		// The ShardSpec has changed. Pull its build and extract its TaskSpec.
		var build = host.builds.Open(labels.Build)
		defer build.Close() // TODO(johnny): Remove build caching.

		if err = build.Extract(func(db *sql.DB) error {
			if taskSpec, err = extractFn(db, labels.TaskName); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, err
		}

		if err = publisher.UpdateLabels(labels); err != nil {
			return nil, fmt.Errorf("creating ops publisher: %w", err)
		}
	}

	return &taskTerm[TaskSpec]{
		cancel:    termCancel,
		ctx:       termCtx,
		labels:    labels,
		shardSpec: shardSpec,
		taskSpec:  taskSpec,
	}, nil
}

func newTaskReader[TaskSpec pf.Task](
	base *taskBase[TaskSpec],
	shard consumer.Shard,
) *taskReader[TaskSpec] {
	var coordinator = shuffle.NewCoordinator(
		shard.Context(),
		base.opsPublisher,
		shard.JournalClient(),
	)
	return &taskReader[TaskSpec]{
		taskBase:    base,
		coordinator: coordinator,
		readBuilder: atomic.Pointer[shuffle.ReadBuilder]{},
	}
}

func (t *taskReader[TaskSpec]) initTerm(shard consumer.Shard) error {
	if err := t.taskBase.initTerm(shard); err != nil {
		return err
	}
	var readBuilder, err = shuffle.NewReadBuilder(
		t.term.ctx,
		shard.JournalClient(),
		t.term.labels.Build,
		t.opsPublisher,
		t.host.service,
		t.term.shardSpec.Id,
		t.term.taskSpec,
	)
	if err != nil {
		return fmt.Errorf("shuffle.NewReadBuilder: %w", err)
	}
	t.readBuilder.Store(readBuilder)

	return nil
}

// StartReadingMessages delegates to shuffle.StartReadingMessages.
func (t *taskReader[TaskSpec]) StartReadingMessages(
	shard consumer.Shard,
	cp pc.Checkpoint,
	tp *flow.Timepoint,
	ch chan<- consumer.EnvelopeOrError,
) {
	shuffle.StartReadingMessages(shard.Context(), t.readBuilder.Load(), cp, tp, ch)
}

// ReplayRange delegates to shuffle's StartReplayRead.
func (t *taskReader[TaskSpec]) ReplayRange(
	shard consumer.Shard,
	journal pb.Journal,
	begin pb.Offset,
	end pb.Offset,
) message.Iterator {
	return shuffle.StartReplayRead(shard.Context(), t.readBuilder.Load(), journal, begin, end)
}

// ReadThrough maps `offsets` to the offsets read by this derivation.
// It delegates to readBuilder.ReadThrough.
func (t *taskReader[TaskSpec]) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	return t.readBuilder.Load().ReadThrough(offsets)
}

func (t *taskReader[TaskSpec]) Coordinator() *shuffle.Coordinator { return t.coordinator }

// heartbeatLoop is a long-lived routine which writes stats at regular intervals,
// and then logs the final exit status of the shard.
func (t *taskBase[TaskSpec]) heartbeatLoop(shard consumer.Shard) {
	var (
		// Period between regularly-published stat intervals.
		// This period must cleanly divide into one hour!
		period = 3 * time.Minute
		// Jitters when interval stats are written cluster-wide.
		jitter = intervalJitter(period, shard.FQN())
		// Op notified when the shard fails.
		op = shard.PrimaryLoop()
	)
	for {
		select {
		case now := <-time.After(jitter + durationToNextInterval(time.Now(), period)):
			var usageRate float32 = 0
			if container := t.container.Load(); container != nil {
				usageRate = container.UsageRate
			}
			_ = t.opsPublisher.PublishStats(
				*intervalStats(now, period, t.opsPublisher.Labels(), usageRate),
				t.opsPublisher.logsPublisher.PublishCommitted,
			)

		case <-op.Done():
			var err = op.Err()

			if err == nil || errors.Is(err, context.Canceled) {
				return
			}

			ops.PublishLog(
				t.opsPublisher,
				ops.Log_error,
				"shard failed",
				"error", err,
				"assignment", shard.Assignment().Decoded,
			)

			// TODO(johnny): Notify control-plane of failure.

			return
		}
	}
}

// intervalStats returns an ops.Stats for a task's current time interval.
func intervalStats(now time.Time, period time.Duration, labels ops.ShardLabeling, usageRate float32) *ops.Stats {
	var ts, err = types.TimestampProto(now)
	if err != nil {
		panic(err)
	}

	return &ops.Stats{
		Shard:     ops.NewShardRef(labels),
		Timestamp: ts,
		Interval: &ops.Stats_Interval{
			UptimeSeconds: uint32(math.Round(period.Seconds())),
			UsageRate:     usageRate,
		},
	}
}

// durationToNextInterval returns the amount of time to wait before the next heartbeat interval.
func durationToNextInterval(now time.Time, period time.Duration) time.Duration {
	// Map `now` into its number of `period`'s since the epoch.
	var num = now.Unix() / int64(period.Seconds())
	// Determine when the next period begins, relative to the epoch and `offset`.
	var next = time.Unix(0, 0).Add(period * time.Duration(num+1))

	return next.Sub(now)
}

// intervalJitter returns a globally consistent, unique jitter offset for `name`
// so that heartbeats are uniformly distributed over time, in aggregate.
func intervalJitter(period time.Duration, name string) time.Duration {
	var w = fnv.New32()
	w.Write([]byte(name))
	return time.Duration(w.Sum32()%uint32(period.Seconds())) * time.Second
}

func signalOnSpecUpdate(
	ctx context.Context,
	cb func(),
	ks *keyspace.KeySpace,
	shard consumer.Shard,
	spec *pf.ShardSpec,
) {
	defer cb()
	var key = shard.FQN()

	ks.Mu.RLock()
	defer ks.Mu.RUnlock()

	for {
		// Pluck the ShardSpec out of the KeySpace, rather than using shard.Spec(),
		// to avoid a re-entrant read lock.
		var next *pf.ShardSpec
		if ind, ok := ks.Search(key); ok {
			next = ks.KeyValues[ind].Decoded.(allocator.Item).ItemValue.(*pf.ShardSpec)
		}

		if next != spec {
			return
		} else if err := ks.WaitForRevision(ctx, ks.Header.Revision+1); err != nil {
			return
		}
	}
}

func doSend[
	Response any,
	Request proto.Message,
	ResponsePtr interface {
		*Response
		proto.Message
	},
	Stream interface {
		Recv() (*Response, error)
		Send(Request) error
	},
](client Stream, request Request) error {
	if err := client.Send(request); err == io.EOF {
		_, err = doRecv[Response, ResponsePtr, Stream](client) // Read to obtain the *actual* error.
		return err
	} else if err != nil {
		panic(err) // gRPC client contract means this never happens
	}
	return nil
}

func doRecv[
	Response any,
	ResponsePtr interface {
		*Response
		proto.Message
	},
	Stream interface {
		Recv() (*Response, error)
	},
](client Stream) (*Response, error) {
	if r, err := client.Recv(); err != nil {
		return nil, pf.UnwrapGRPCError(err)
	} else {
		return r, nil
	}
}
