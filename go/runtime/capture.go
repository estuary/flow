package runtime

import (
	//"encoding/json"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/captures"
	"github.com/estuary/flow/go/captures/kinesis"
	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/shuffle"
	log "github.com/sirupsen/logrus"
	//"go.gazette.dev/core/broker/client"
	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
	//"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// A runtime.Application implementation for capturing from Kinesis
// TODO: move to kinesis package
type KinesisCapture struct {
	// FlowConsumer which owns this Capture shard.
	host *FlowConsumer

	taskName       string
	partitionRange captures.PartitionRange

	// Store delegate for persisting local checkpoints.
	store *consumer.JSONFileStore

	logEntry *log.Entry

	messageCh chan<- consumer.EnvelopeOrError

	currentTerm *kinesisTerm

	shardCtx context.Context

	combine *bindings.Combine

	docsThisTxn int
}

type kinesisTerm struct {
	revision int64
	mapper   flow.Mapper

	captureTerm captures.CaptureTerm
	dataCh      <-chan captures.DataMessage
	ctx         context.Context
	cancelFunc  context.CancelFunc
}

// Implementing runtime.Application for Capture
var _ Application = (*KinesisCapture)(nil)

// Implementing shuffle.Store for Capture
var _ shuffle.Store = (*KinesisCapture)(nil)

func NewCaptureApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (Application, error) {
	var taskName = shard.Spec().LabelSet.ValueOf(labels.TaskName)
	if taskName == "" {
		return nil, fmt.Errorf("missing value of shard label: '%s'", labels.TaskName)
	}
	var task, _, _, err = host.Catalog.GetTask(taskName)
	if err != nil {
		return nil, fmt.Errorf("reading catalog task spec: %w", err)
	}
	if task.Capture == nil {
		return nil, fmt.Errorf("Expected task to be a capture")
	}
	switch task.Capture.EndpointType {
	case pf.EndpointType_KINESIS:
		return NewKinesisCaptureApp(host, shard, recorder)
	default:
		return nil, fmt.Errorf("EndpointType '%s' is not supported for captures", task.Capture.EndpointType.String())
	}
}

func NewKinesisCaptureApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*KinesisCapture, error) {

	var storeState = make(map[string]string)
	store, err := consumer.NewJSONFileStore(recorder, &storeState)
	if err != nil {
		return nil, err
	}

	var taskName = shard.Spec().LabelSet.ValueOf(labels.TaskName)
	partitionRange, err := captures.ParsePartitionRange(shard)
	if err != nil {
		return nil, err
	}

	var combine = bindings.NewCombine(shard.FQN())

	var logEntry = log.WithFields(log.Fields{
		"shardId": shard.FQN(),
	})

	var app = &KinesisCapture{
		host:           host,
		store:          store,
		logEntry:       logEntry,
		shardCtx:       shard.Context(),
		taskName:       taskName,
		partitionRange: partitionRange,
		combine:        combine,
	}

	return app, nil
}

type initTermMessage message.UUID

func (t *initTermMessage) GetUUID() message.UUID {
	return message.UUID(*t)
}
func (t *initTermMessage) SetUUID(message.UUID) {
	panic("cannot SetUUID on initTermMessage")
}
func (t *initTermMessage) NewAcknowledgement(_ pb.Journal) message.Message {
	panic("cannot NewAcknowledgement on initTermMessage")
}

func (c *KinesisCapture) sendInitTermMessage() {
	var msg = initTermMessage(message.BuildUUID(
		message.NewProducerID(),
		message.NewClock(time.Now()),
		message.Flag_OUTSIDE_TXN,
	))
	var envelope = consumer.EnvelopeOrError{
		Envelope: message.Envelope{
			Journal: &pb.JournalSpec{
				Name:  pb.Journal(c.taskName),
				Flags: pb.JournalSpec_O_RDONLY,
			},
			Message: &msg,
		},
	}
	select {
	case c.messageCh <- envelope:
	case <-c.shardCtx.Done():
	}
}

func (c *KinesisCapture) tryInitTerm() (err error) {
	// End any existing term before initializing a new one
	c.endTerm()
	c.logEntry.Info("Initializing a new catalog task term")

	spec, commons, revision, err := c.host.Catalog.GetTask(c.taskName)
	if err != nil {
		return fmt.Errorf("reading catalog task: %w", err)
	}
	if spec.Capture == nil {
		// TODO: Ideally we'd include a description of the actual task in the error message, but we
		// need to be careful not to include the endpoint config.
		return fmt.Errorf("Expected task %s to be a Capture", c.taskName)
	}

	// Copy the state so that the kinesis package can keep it around for the life of the term, while
	// we continue to update it.
	var stateCopy = make(map[string]string)
	for k, v := range *c.store.State.(*map[string]string) {
		stateCopy[k] = v
	}
	schemaIndex, err := bindings.NewSchemaIndex(&commons.Schemas)
	if err != nil {
		return fmt.Errorf("building schemaIndex: %w", err)
	}
	var partitionPtrs []string
	for _, field := range spec.Capture.Collection.PartitionFields {
		var projection = spec.Capture.Collection.GetProjection(field)
		if projection == nil {
			err = fmt.Errorf("Invalid capture spec, collection has no such projection: '%s'", field)
			return
		}
		partitionPtrs = append(partitionPtrs, projection.Ptr)
	}
	err = c.combine.Configure(
		schemaIndex,
		spec.Capture.Collection.SchemaUri,
		spec.Capture.Collection.KeyPtrs,
		partitionPtrs,
		spec.Capture.Collection.UuidPtr,
	)
	if err != nil {
		return fmt.Errorf("configuring combiner: %w", err)
	}

	var ctx, cancelFunc = context.WithCancel(c.shardCtx)
	var captureTerm = captures.CaptureTerm{
		Revision: revision,
		Spec:     *spec.Capture,
		Range:    c.partitionRange,
		Ctx:      ctx,
	}
	dataCh, err := kinesis.Start(captureTerm, stateCopy, c.messageCh)
	if err != nil {
		// Cancel any work that might have been started by kinesis.Start
		cancelFunc()
		return err
	}

	var mapper = flow.Mapper{
		Ctx:           ctx,
		JournalClient: c.host.Service.Journals,
		JournalRules:  commons.JournalRules.Rules,
		Journals:      c.host.Journals,
	}

	c.currentTerm = &kinesisTerm{
		revision:    revision,
		mapper:      mapper,
		captureTerm: captureTerm,
		dataCh:      dataCh,
		ctx:         ctx,
		cancelFunc:  cancelFunc,
	}
	return
}

// RestoreCheckpoint implements consumer.Store.RestoreCheckpoint
func (c *KinesisCapture) RestoreCheckpoint(shard consumer.Shard) (pc.Checkpoint, error) {
	// TODO: register some callback with host.Catalog.SignalOnTaskUpdate to re-start capture reads

	// We don't use flow checkpoints, and instead rely on a system-dependent checkpoint that's
	// managed by Capture.
	return pc.Checkpoint{}, nil
}

// StartCommit implements consumer.Store.StartCommit
func (c *KinesisCapture) StartCommit(shard consumer.Shard, checkpoint pc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	log.WithFields(log.Fields{
		"shardId":    shard.FQN(),
		"checkpoint": checkpoint,
	}).Debug("on StartCommit")
	return c.store.StartCommit(shard, checkpoint, waitFor)
}

// Destroy implements consumer.Store.Destroy
func (c *KinesisCapture) Destroy() {
	if c.currentTerm != nil {
		c.currentTerm.cancelFunc()
		c.currentTerm = nil
	}
	c.store.Destroy()
}

func (c *KinesisCapture) ingestNext(msg *captures.ControlMessage, publisher *message.Publisher) error {
	// A previous task revision may have sent a ControlMessage before being cancelled.
	if msg.Revision != c.currentTerm.revision {
		c.logEntry.WithFields(log.Fields{
			"ignoredRevision": msg.Revision,
			"currentRevision": c.currentTerm.revision,
		}).Info("Ignoring capture control message from previous revision")
		return nil
	}

	// Expect that exactly `Available` messages can now be read from `dataCh`. This relates to the
	// hypothetical scenario where we read `n` messages from `dataCh`, where `n < Available`. In
	// such a scenario, we would need to have another ControlMessage sent since there is still data
	// remaining to be read from the `dataCh`. We avoid that scenario by always reading exactly the
	// number of messages that were added by the kinesis capture.
	for i := 0; i < msg.Available; i++ {
		data := <-c.currentTerm.dataCh

		var err = c.combine.CombineRight(data.Document)
		if err != nil {
			c.endTerm()
			return fmt.Errorf("combining document from kinesis shard: '%s', sequenceID '%s': %w", data.Stream, data.Offset, err)
		}
		//c.ingestion.Add(c.currentTerm.captureTerm.Spec.Collection.Collection, data.Document)
		(*c.store.State.(*map[string]string))[data.Stream] = data.Offset
		// Increment the counter, which allows us skip draining the combiner if there were no
		// documents added.
		c.docsThisTxn++
	}
	c.logEntry.WithField("nDocs", msg.Available).Info("added kinesis docs")
	return nil
}

func (c *KinesisCapture) endTerm() {
	if c.currentTerm != nil {
		c.logEntry.WithField("revision", c.currentTerm.revision).Info("Stopping kinesis capture term")
		c.currentTerm.cancelFunc()
		c.currentTerm = nil
	}
}

// Coordinator implements shuffle.Store.Coordinator
func (m *KinesisCapture) Coordinator() *shuffle.Coordinator {
	// TODO: add comment here
	return nil
}

func (c *KinesisCapture) StartReadingMessages(shard consumer.Shard, cp pc.Checkpoint, timepoint *flow.Timepoint, messageCh chan<- consumer.EnvelopeOrError) {
	c.logEntry.Info("StartReadingMessages for kinesis capture")
	c.messageCh = messageCh
	c.sendInitTermMessage()
}

func (c *KinesisCapture) BeginTxn(shard consumer.Shard) error {
	// reset the counter
	c.docsThisTxn = 0
	return nil
}

func (c *KinesisCapture) ConsumeMessage(shard consumer.Shard, envelope message.Envelope, publisher *message.Publisher) error {
	var err error
	switch msg := envelope.Message.(type) {
	case *captures.ControlMessage:
		err = c.ingestNext(msg, publisher)
	case *initTermMessage:
		err = c.tryInitTerm()
	}
	return err
}

func (c *KinesisCapture) FinalizeTxn(shard consumer.Shard, publisher *message.Publisher) error {
	if c.docsThisTxn == 0 {
		c.logEntry.Info("No docs to finalize for this transaction")
		return nil
	}

	c.logEntry.WithField("nDocs", c.docsThisTxn).Info("FinalizeTxn")

	var err = c.combine.Drain(func(full bool, doc json.RawMessage, packedKey, packedPartitions []byte) error {
		if full {
			panic("capture produces only partially combined documents")
		}

		partitions, err := tuple.Unpack(packedPartitions)
		if err != nil {
			return fmt.Errorf("unpacking partitions: %w", err)
		}
		_, err = publisher.PublishUncommitted(c.currentTerm.mapper.Map, flow.Mappable{
			Spec:       &c.currentTerm.captureTerm.Spec.Collection,
			Doc:        doc,
			PackedKey:  packedKey,
			Partitions: partitions,
		})
		return err
	})
	if err != nil {
		c.endTerm()
	}
	return err
}

func (c *KinesisCapture) FinishedTxn(shard consumer.Shard, fut consumer.OpFuture) {
	// TODO: remove logging
	c.logEntry.WithField("nDocs", c.docsThisTxn).Info("FinishedTxn")
	// no-op
}

func (c *KinesisCapture) ReplayRange(_ consumer.Shard, _ pb.Journal, begin, end pb.Offset) message.Iterator {
	panic("ReplayRange not supported by kinesis capture")
}

func (c *KinesisCapture) ReadThrough(_ pb.Offsets) (pb.Offsets, error) {
	panic("ReadThrough not supported by kinesis capture")
}
