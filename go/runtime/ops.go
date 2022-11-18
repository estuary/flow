package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// ShardRef is a reference to a specific task shard that represents the source of logs and stats.
// This struct definition matches the JSON schema for the ops collections at:
// crates/build/src/ops/ops-task-schema.json
type ShardRef struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	KeyBegin    string `json:"keyBegin"`
	RClockBegin string `json:"rClockBegin"`
}

type Meta struct {
	UUID string `json:"uuid"`
}

func shardAndPartitions(labeling labels.ShardLabeling) (ShardRef, tuple.Tuple) {
	var shard = ShardRef{
		Name:        labeling.TaskName,
		Kind:        labeling.TaskType,
		KeyBegin:    fmt.Sprintf("%08x", labeling.Range.KeyBegin),
		RClockBegin: fmt.Sprintf("%08x", labeling.Range.RClockBegin),
	}
	var partitions = tuple.Tuple{
		shard.Kind,
		shard.Name,
	}
	return shard, partitions
}

// LogEvent is a Go struct definition that matches the log event documents defined by:
// crates/build/src/ops/ops-log-schema.json
type LogEvent struct {
	Meta      Meta      `json:"_meta"`
	Shard     *ShardRef `json:"shard"`
	Timestamp time.Time `json:"ts"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	// Fields will be either a map[string]interface{} or a map[string]json.RawMessage, depending on
	// whether this event was created by Log or LogForwarded.
	Fields interface{} `json:"fields,omitempty"`
}

// SetUUID implements message.Message for LogEvent
func (e *LogEvent) SetUUID(uuid message.UUID) {
	e.Meta.UUID = uuid.String()
}

// GetUUID implements message.Message for LogEvent
func (e *LogEvent) GetUUID() message.UUID {
	panic("not implemented")
}

// NewAcknowledgement implements message.Message for LogEvent
func (e *LogEvent) NewAcknowledgement(pb.Journal) message.Message {
	panic("not implemented")
}

// LogPublisher is an ops.Logger that is scoped to a particular task shard,
// and publishes its logged events to a Flow collection.
type LogPublisher struct {
	level         logrus.Level
	opsCollection *pf.CollectionSpec
	shard         ShardRef
	governerCh    chan<- *client.AsyncAppend
	mapper        flow.Mapper
	publisher     *message.Publisher
	partitions    tuple.Tuple
}

// logCollection returns the collection to which logs of the given task name are written.
func logCollection(taskName string) pf.Collection {
	return pf.Collection(fmt.Sprintf("ops/%s/logs", strings.Split(taskName, "/")[0]))
}

// NewPublisher creates a new LogPublisher for the given task ShardLabeling,
// which publishes to the given collection using the provided journal client
// and Mapper.
func NewLogPublisher(
	labeling labels.ShardLabeling,
	collection *pf.CollectionSpec,
	ajc client.AsyncJournalClient,
	mapper flow.Mapper,
) (*LogPublisher, error) {
	var level, err = logrus.ParseLevel(labeling.LogLevel)
	if err != nil {
		return nil, err
	}

	var shard = ShardRef{
		Name:        labeling.TaskName,
		Kind:        labeling.TaskType,
		KeyBegin:    fmt.Sprintf("%08x", labeling.Range.KeyBegin),
		RClockBegin: fmt.Sprintf("%08x", labeling.Range.RClockBegin),
	}
	var partitions = tuple.Tuple{
		shard.Kind,
		shard.Name,
	}

	// Passing a nil timepoint to NewPublisher means that the timepoint that's encoded in the
	// UUID of log documents will always reflect the current wall-clock time, even when those
	// log documents were produced during test runs, where `readDelay`s might normally cause
	// time to skip forward. This probably only matters in extremely outlandish test scenarios,
	// and so it doesn't seem worth the complexity to modify this timepoint during tests.
	var publisher = message.NewPublisher(ajc, nil)

	// Create a buffered channel that will serve to bound the number of pending appends to a logs
	// collection. We'll loop over all the append operations in the channel and wait for them to
	// complete. When publishing logs, we'll push each AsyncAppend operation to this channel,
	// blocking until the channel has space available. The specific size of the buffer was chosen
	// somewhat arbitrarily, with the aim of providing _some_ resilience to temporary network errors
	// without blocking the rest of the shard's processing, while also limiting the number of log
	// messages that could potentially be lost forever if someone pulls the plug on the machine.
	var governerCh = make(chan *client.AsyncAppend, 100)
	go func(ch <-chan *client.AsyncAppend) {
		for op := range ch {
			if logErr := op.Err(); logErr != nil && logErr != context.Canceled {
				ops.StdLogger().Log(logrus.ErrorLevel, logrus.Fields{
					"shard": shard,
					"error": logErr,
				}, "failed to append to log collection")
			}
		}
	}(governerCh)

	logrus.WithFields(logrus.Fields{
		"logCollection": collection.Collection,
		"level":         level.String(),
	}).Debug("starting new log publisher")

	var out = &LogPublisher{
		opsCollection: collection,
		shard:         shard,
		level:         level,
		governerCh:    governerCh,
		mapper:        mapper,
		publisher:     publisher,
		partitions:    partitions,
	}
	// Use a finalizer to close the governer channel when the publisher is no longer used.
	// LogPublishers don't currently have an explicit Close function, so we assume they may be used
	// right up until they're garbage collected.
	//
	// TODO(johnny): I'm not sure we should be using finalizers in this way.
	// Rather, we should use them only to assert that the resource was closed
	// before it was dropped. See:
	// https://crawshaw.io/blog/sharp-edged-finalizers
	runtime.SetFinalizer(out, func(pub *LogPublisher) {
		close(pub.governerCh)
	})
	return out, nil
}

// Level implements the ops.Logger interface.
func (p *LogPublisher) Level() logrus.Level {
	return p.level
}

// Log implements the ops.Logger interface. It publishes log messages to the configured ops
// collection, and also forwards them to the normal logger.
func (p *LogPublisher) Log(level logrus.Level, fields logrus.Fields, message string) error {
	if p.level < level {
		return nil
	}
	// It's common practice to treat `nil` and an empty map equivalently. But that doesn't work when
	// you pass a `nil` of type `logrus.Fields` to `doLog`, which accepts `fields interface{}`.
	// See: https://stackoverflow.com/questions/44320960/omitempty-doesnt-omit-interface-nil-values-in-json
	if fields == nil {
		fields = logrus.Fields{}
	}
	return p.doLog(level, time.Now().UTC(), fields, message)
}

// LogForwarded implements the ops.Logger interface. It publishes log messages to the
// configured ops collection, and also forwards them to the normal logger.
func (p *LogPublisher) LogForwarded(ts time.Time, level logrus.Level, fields map[string]json.RawMessage, message string) error {
	// It's common practice to treat `nil` and an empty map equivalently. But that doesn't work when
	// you pass a `nil` of type `map[string]json.RawMessage` to `doLog`, which accepts `fields interface{}`.
	// See: https://stackoverflow.com/questions/44320960/omitempty-doesnt-omit-interface-nil-values-in-json
	if fields == nil {
		fields = map[string]json.RawMessage{}
	}
	return p.doLog(level, time.Now().UTC(), fields, message)
}

func levelString(level logrus.Level) string {
	switch level {
	case logrus.TraceLevel:
		return "trace"
	case logrus.DebugLevel:
		return "debug"
	case logrus.InfoLevel:
		return "info"
	case logrus.WarnLevel:
		return "warn"
	default:
		return "error"
	}
}

// doLog publishes a log event, and returns an error if it fails. The `fields` here are an
// `interface{}` so that this can accept either the `logrus.Fields` from a normal message or the
// `map[string]json.RawMessage` from a forwarded log event.
func (p *LogPublisher) doLog(level logrus.Level, ts time.Time, fields interface{}, message string) error {
	var err = p.tryLog(level, ts, fields, message)
	if err != nil && !errors.Is(err, context.Canceled) {
		logrus.WithFields(logrus.Fields{
			"origMessage":   message,
			"logPublishErr": err,
			"origFields":    fields,
		}).Error("failed to publish log message")
	}
	return err
}

func (p *LogPublisher) tryLog(level logrus.Level, ts time.Time, fields interface{}, message string) error {
	// Literalize `error` implementations, as they're otherwise ignored by `encoding/json`.
	// See: https://github.com/sirupsen/logrus/issues/137
	if m, ok := fields.(logrus.Fields); ok {
		for k, v := range m {
			if e, ok := v.(error); ok {
				m[k] = e.Error()
			}
		}
	}

	var event = LogEvent{
		Meta: Meta{
			UUID: string(pf.DocumentUUIDPlaceholder),
		},
		Shard:     &p.shard,
		Timestamp: ts,
		Level:     levelString(level),
		Fields:    fields,
		Message:   message,
	}
	var eventJson, err = json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("serializing log event: %w", err)
	}
	// We currently omit the key from this Mappable, which is fine because we don't actually use it
	// for publishing logs.
	var mappable = flow.Mappable{
		Spec:       p.opsCollection,
		Doc:        json.RawMessage(eventJson),
		Partitions: p.partitions,
	}

	op, err := p.publisher.PublishCommitted(p.mapper.Map, mappable)
	if err != nil {
		return err
	}
	// Push the AsyncAppend onto the governer channel, which will block until the channel has
	// capacity. This helps apply back pressure on shards that do a lot of verbose logging.
	p.governerCh <- op
	return nil
}

// validateOpsCollection ensures that the collection spec has the expected partition fields.
// We manually extract partition fields for logs and stats collections, instead of running them
// through a combiner. This function ensures that the fields we extract here will match the ops
// collection that we'll publish to. This validation should fail if someone were to change the
// generated ops collections without updating this file to match.
func validateOpsCollection(c *pf.CollectionSpec) error {
	var expectedPartitionFields = []string{"kind", "name"}
	if err := validateStringSliceEq(expectedPartitionFields, c.PartitionFields); err != nil {
		return fmt.Errorf("invalid partition fields: %w", err)
	}
	return nil
}

func validateStringSliceEq(expected []string, actual []string) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("expected %v, got %v", expected, actual)
	}
	for i, exp := range expected {
		if exp != actual[i] {
			return fmt.Errorf("expected element %d to be %q, but was %q", i, exp, actual[i])
		}
	}
	return nil
}
