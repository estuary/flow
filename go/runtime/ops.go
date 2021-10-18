package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
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

// LogEvent is a Go struct definition that matches the log event documents defined by:
// crates/build/src/ops/ops-log-schema.json
type LogEvent struct {
	Shard     *ShardRef       `json:"shard"`
	Timestamp interface{}     `json:"ts"`
	Level     string          `json:"level"`
	Message   string          `json:"message"`
	Fields    json.RawMessage `json:"fields,omitempty"`
}

// LogPublisher is an ops.LogPublisher that is scoped to a particular task, and publishes log events
// to a Flow collection.
type LogPublisher struct {
	level         logrus.Level
	opsCollection *pf.CollectionSpec
	shard         ShardRef
	root          *LogService
	mapper        flow.Mapper
	// We currently use a combiner to extract the key and partition fields, perform validation, and
	// add the UUID placeholder. Another approach would be to validate that the target collection
	// has the expected key and partition fields, and just set all those things manually, which
	// would avoid the cost of going through the combiner. This is being left as a future exercise,
	// since the combiner affords more flexibility in the configuration of the logs collection spec,
	// and since it's only a performance optimization. So it seems best to wait until we have more
	// confidence that the spec of the logs collection is stable.
	combiner *bindings.Combine
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
func (r *LogService) NewPublisher(opsCollectionName string, task ShardRef, taskRevision string, level logrus.Level) (*LogPublisher, error) {
	var catalogTask, commons, _, err = r.catalog.GetTask(r.ctx, opsCollectionName, taskRevision)
	if err != nil {
		return nil, err
	}

	if catalogTask.Ingestion == nil {
		return nil, fmt.Errorf("expected ops collection to be an ingestion, got: %+v", task)
	}
	var opsCollection = catalogTask.Ingestion
	var mapper = flow.Mapper{
		Ctx:           r.ctx,
		JournalClient: r.ajc,
		Journals:      r.journals,
		JournalRules:  commons.JournalRules.Rules,
	}
	logrus.WithFields(logrus.Fields{
		"logCollection": opsCollectionName,
		"level":         level.String(),
	}).Info("starting new log publisher")

	var partitionPtrs = flow.PartitionPointers(opsCollection)
	schemaIndex, err := commons.SchemaIndex()
	if err != nil {
		return nil, fmt.Errorf("building schema index: %w", err)
	}
	// Create the combiner with logs forwarded to the logrus logger, so that we don't create a
	// recursive logging loop.
	combiner, err := bindings.NewCombine(ops.FilteredStdLogPublisher(log.WarnLevel))
	if err != nil {
		return nil, fmt.Errorf("creating combiner: %w", err)
	}
	if err = combiner.Configure(
		opsCollectionName,
		schemaIndex,
		opsCollection.Collection,
		opsCollection.SchemaUri,
		opsCollection.UuidPtr,
		opsCollection.KeyPtrs,
		partitionPtrs,
	); err != nil {
		return nil, fmt.Errorf("configuring combiner: %w", err)
	}

	return &LogPublisher{
		opsCollection: catalogTask.Ingestion,
		shard:         task,
		root:          r,
		mapper:        mapper,
		combiner:      combiner,
		level:         level,
	}, nil
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

// LogWithTime implements the ops.LogPublisher interface. It publishes log messages to the
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

	var fieldsJson json.RawMessage
	var err error
	// The goal here is to leave fieldsJson empty if there are no fields, instead of serializing an
	// empty object.
	if fields != nil {
		fieldsJson, err = json.Marshal(fields)
		if err != nil {
			return fmt.Errorf("marshalling fields json: %w", err)
		}
	}

	var event = LogEvent{
		Shard:     &p.shard,
		Timestamp: ts,
		Level:     levelString(level),
		Fields:    fieldsJson,
		Message:   message,
	}

	docJson, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshalling log document: %w", err)
	}
	if err = p.combiner.CombineRight(docJson); err != nil {
		return fmt.Errorf("combine right: %w", err)
	}
	return p.combiner.Drain(func(full bool, doc json.RawMessage, packedKey, packedPartitions []byte) error {
		if full {
			panic("publishing logs only produces partitally combined documents")
		}
		var partitions, err = tuple.Unpack(packedPartitions)
		if err != nil {
			return fmt.Errorf("unpacking partition key")
		}
		var mappable = flow.Mappable{
			Spec:       p.opsCollection,
			Doc:        doc,
			PackedKey:  packedKey,
			Partitions: partitions,
		}
		_, err = p.root.messagePublisher.PublishCommitted(p.mapper.Map, mappable)
		if err != nil {
			return fmt.Errorf("publishing log: %w", err)
		}
		return nil
	})
}
