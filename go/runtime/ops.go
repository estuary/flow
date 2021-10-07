package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
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

// LogEvent is a Go struct definition that matches the log event documents defined by:
// crates/build/src/ops/ops-log-schema.json
type LogEvent struct {
	Meta      Meta        `json:"_meta"`
	Shard     *ShardRef   `json:"shard"`
	Timestamp interface{} `json:"ts"`
	Level     string      `json:"level"`
	Message   string      `json:"message"`
	Fields    interface{} `json:"fields,omitempty"`
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

// LogPublisher is an ops.LogPublisher that is scoped to a particular task, and publishes log events
// to a Flow collection.
type LogPublisher struct {
	level         logrus.Level
	opsCollection *pf.CollectionSpec
	shard         ShardRef
	root          *LogService
	governerCh    chan<- *client.AsyncAppend
	mapper        *flow.Mapper
	partitions    tuple.Tuple
}

// LogService is used to create LogPublishers at runtime. There only needs to be a single
// LogService instance for the entire flow consumer application.
type LogService struct {
	ctx              context.Context
	ajc              *client.AppendService
	journals         flow.Journals
	catalog          flow.Catalog
	messagePublisher *message.Publisher
}

// NewPublisher creates a new LogPublisher, which can be used to publish logs that are scoped to
// the given task and appended as documents to the given |opsCollectionName|.
func (r *LogService) NewPublisher(opsCollectionName string, shard ShardRef, taskRevision string, level logrus.Level) (*LogPublisher, error) {
	var catalogTask, _, _, err = r.catalog.GetTask(r.ctx, opsCollectionName, taskRevision)
	if err != nil {
		return nil, err
	}

	if catalogTask.Ingestion == nil {
		return nil, fmt.Errorf("expected ops collection to be an ingestion, got: %+v", catalogTask)
	}
	var opsCollection = catalogTask.Ingestion
	if err = validateLogCollection(opsCollection); err != nil {
		return nil, fmt.Errorf("logs collection spec is invalid: %w", err)
	}
	var mapper = flow.NewMapper(r.ctx, r.ajc, r.journals)
	var partitions = tuple.Tuple{
		shard.Kind,
		shard.Name,
		shard.KeyBegin,
		shard.RClockBegin,
	}

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
				ops.StdLogPublisher().Log(log.ErrorLevel, log.Fields{
					"shard": shard,
					"error": logErr,
				}, "failed to append to log collection")
			}
		}
	}(governerCh)

	logrus.WithFields(logrus.Fields{
		"logCollection": opsCollectionName,
		"level":         level.String(),
	}).Debug("starting new log publisher")

	var publisher = &LogPublisher{
		opsCollection: catalogTask.Ingestion,
		shard:         shard,
		root:          r,
		level:         level,
		governerCh:    governerCh,
		mapper:        &mapper,
		partitions:    partitions,
	}
	// Use a finalizer to close the governer channel when the publisher is no longer used.
	// LogPublishers don't currently have an explicit Close function, so we assume they may be used
	// right up until they're garbage collected.
	runtime.SetFinalizer(publisher, func(pub *LogPublisher) {
		close(pub.governerCh)
	})
	return publisher, nil
}

// Level implements the ops.LogPublisher interface.
func (p *LogPublisher) Level() log.Level {
	return p.level
}

// Log implements the ops.LogPublisher interface. It publishes log messages to the configured ops
// collection, and also forwards them to the normal logger.
func (p *LogPublisher) Log(level logrus.Level, fields logrus.Fields, message string) error {
	if p.level < level {
		return nil
	}
	var err = p.doLog(level, time.Now().UTC(), fields, message)
	if err == nil && logrus.IsLevelEnabled(level) {
		ops.StdLogPublisher().Log(level, fields, message)
	}
	return err
}

// LogForwarded implements the ops.LogPublisher interface. It publishes log messages to the
// configured ops collection, and also forwards them to the normal logger.
func (p *LogPublisher) LogForwarded(ts time.Time, level logrus.Level, fields map[string]json.RawMessage, message string) error {
	if p.level < level {
		return nil
	}
	var err = p.doLog(level, time.Now().UTC(), fields, message)
	if err == nil && logrus.IsLevelEnabled(level) {
		ops.StdLogPublisher().LogForwarded(ts, level, fields, message)
	}
	return err
}

func levelString(level log.Level) string {
	switch level {
	case log.TraceLevel:
		return "trace"
	case log.DebugLevel:
		return "debug"
	case log.InfoLevel:
		return "info"
	case log.WarnLevel:
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
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"origMessage":   message,
			"logPublishErr": err,
			"origFields":    fields,
		}).Error("failed to publish log message")
	}
	return err
}

func (p *LogPublisher) tryLog(level logrus.Level, ts time.Time, fields interface{}, message string) error {
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
	var mappable = flow.Mappable{
		Spec:       p.opsCollection,
		Doc:        json.RawMessage(eventJson),
		Partitions: p.partitions,
	}

	op, err := p.root.messagePublisher.PublishCommitted(p.mapper.Map, mappable)
	if err != nil {
		return err
	}
	// Push the AsyncAppend onto the governer channel, which will block until the channel has
	// capacity. This helps apply back pressure on shards that do a lot of verbose logging.
	p.governerCh <- op
	return nil
}

// constMapper provides a message.MappingFunc that always returns the same journal and content type.
type constMapper struct {
	journal            pb.Journal
	journalContentType string
}

func (m *constMapper) Map(msg message.Mappable) (pb.Journal, string, error) {
	return m.journal, m.journalContentType, nil
}

// validateLogCollection ensures that the collection spec has the expected key and partition fields.
// We manually extract keys and partition fields for logs collections, instead of running them
// through a combiner. This function ensures that the fields we extract here will match the logs
// collection that we'll publish to. This validation should fail if someone were to change the
// generated ops collections without updating this file to match.
func validateLogCollection(c *pf.CollectionSpec) error {
	var expectedPartitionFields = []string{"kind", "name", "rangeKeyBegin", "rangeRClockBegin"}
	if err := validateStringSliceEq(expectedPartitionFields, c.PartitionFields); err != nil {
		return fmt.Errorf("invalid partition fields: %w", err)
	}
	var expectedKeyPtrs = []string{
		"/shard/name",
		"/shard/keyBegin",
		"/shard/rClockBegin",
		"/ts",
	}
	if err := validateStringSliceEq(expectedKeyPtrs, c.KeyPtrs); err != nil {
		return fmt.Errorf("invalid key pointers: %w", err)
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
