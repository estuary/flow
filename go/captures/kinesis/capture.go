package kinesis

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/flow/go/captures"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/message"
)

type kinesisRecord struct {
	document   []byte
	shardID    string
	sequenceID string
	endOfShard bool
	err        error
}

type kinesisCapture struct {
	client             *kinesis.Kinesis
	config             Config
	controlCh          chan<- consumer.EnvelopeOrError
	dataCh             chan captures.DataMessage
	buffer             []captures.DataMessage
	nAvailable         int
	bufferMutex        sync.Mutex
	term               captures.CaptureTerm
	readingShards      map[string]bool
	readingShardsMutex sync.Mutex
	producerID         message.ProducerID

	// shardSequences is a copy of the capture state, which just tracks the sequenceID for each
	// kinesis shard. We keep this as a struct field so that we can ensure that all reads will use
	// the same state, regardless of whether they're triggered by the initial shard listing or
	// returned as a child shard id when reaching the end of an existing shard.
	shardSequences map[string]string
}

// Start begins reading data from Kinesis and sends the data on the returned channel. The channel is
// buffered to allow making subsequent kinesis read requests while in-flight records are processed.
// Returns an error immediately if listing streams fails, since that indicates a condition that's
// likely not recoverable (invalid configuration or insufficient permissions).
func Start(term captures.CaptureTerm, state map[string]string, controlCh chan<- consumer.EnvelopeOrError) (<-chan captures.DataMessage, error) {
	var config Config
	err := json.Unmarshal([]byte(term.Spec.EndpointConfigJson), &config)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling endpoing config: %w", err)
	}

	client, err := connect(&config)
	if err != nil {
		return nil, fmt.Errorf("connecting to kinesis: %w", err)
	}
	// Use a buffered channel here to allow for sending the next read request before all data is
	// processed. It might be useful to change the size of this buffer depending on the size of
	// records and the append rate, but that's being left for the future.
	var dataCh = make(chan captures.DataMessage, 128)

	var kc = &kinesisCapture{
		client:        client,
		config:        config,
		controlCh:     controlCh,
		dataCh:        dataCh,
		term:          term,
		readingShards: make(map[string]bool),
		producerID:    message.NewProducerID(),

		shardSequences: state,
	}
	allShardIds, err := kc.listShards()
	if err != nil {
		return nil, fmt.Errorf("listing kinesis shards: %w", err)
	} else if len(allShardIds) == 0 {
		// TODO: Verify if it's even possible for a kinesis stream to have 0 shards
		return nil, fmt.Errorf("No kinesis shards found for the given stream")
	}
	// Start the background goroutine that will buffer data and send control messages to notify the
	// Flow consumer when data is available.
	//go kc.startMessagePump()

	// Start reading from all the known shards.
	for _, kinesisShardID := range allShardIds {
		go kc.startReadingShard(kinesisShardID)
	}
	// TODO: Compare new shards listing with state and see what can be pruned
	// We can't prune state for shards when the reader reaches the end because the shard will still
	// exist until the retention period expires, and we need to know that we've already read it so
	// we don't read it again.

	return kc.dataCh, nil
}

func (kc *kinesisCapture) listShards() ([]string, error) {
	var shards []string

	var nextToken = ""
	for {
		var listShardsReq = kinesis.ListShardsInput{}
		if nextToken != "" {
			listShardsReq.NextToken = &nextToken
		} else {
			listShardsReq.StreamName = &kc.config.Stream
		}
		listShardsResp, err := kc.client.ListShardsWithContext(kc.term.Ctx, &listShardsReq)
		if err != nil {
			return nil, fmt.Errorf("listing shards: %w", err)
		}
		for _, shard := range listShardsResp.Shards {
			shards = append(shards, *shard.ShardId)
		}

		if listShardsResp.NextToken != nil && (*listShardsResp.NextToken) != "" {
			nextToken = *listShardsResp.NextToken
		} else {
			break
		}
	}

	return shards, nil
}

// startReadingShard is always intented to be called within its own new goroutine. It will first
// check whether the shard is already being read and return early if so.
func (kc *kinesisCapture) startReadingShard(shardID string) {
	var logEntry = log.WithFields(log.Fields{
		"kinesisShardId": shardID,
		"capture":        kc.term.Spec.Capture,
	})
	if !kc.term.Range.Includes([]byte(shardID)) {
		logEntry.Debug("Ignoring kinesis shard because it is outside the assigned range of this flow consumer shard")
		return
	}

	// Kinesis shards can merge or split, forming new child shards. We need to guard against reading
	// the same kinesis shard multiple times. This could happen if a shard is merged with another,
	// since they would both return the same child shard id. It could also happen on initialization
	// if we start reading an old shard that returns a child shard ID that we have already started
	// reading. To guard against this, we use a mutex around a map that tracks which shards we've
	// already started reading.
	kc.readingShardsMutex.Lock()
	var isReadingShard = kc.readingShards[shardID]
	if !isReadingShard {
		kc.readingShards[shardID] = true
	}
	kc.readingShardsMutex.Unlock()
	if isReadingShard {
		logEntry.Debug("A read for this kinesis shard is already in progress")
		return
	}

	var nextSeqID = kc.shardSequences[shardID]
	var err error
	for {
		nextSeqID, err = kc.readShard(shardID, nextSeqID)
		if err != nil {
			logEntry.WithField("error", err).Warn("reading kinesis shard failed")
			var message = consumer.EnvelopeOrError{
				Error: fmt.Errorf("Failed reading kinesis shard: '%s': %w", shardID, err),
			}
			select {
			case kc.controlCh <- message:
				return
			case <-kc.term.Ctx.Done():
				// TODO: maybe log something different?
				return
			}
		} else {
			// nil error means that we've reached the end of the kinesis shard.
			logEntry.Info("Reached the end of kinesis shard")
			return
		}
	}
}

// Perform a single read of a shard, which continuously loops and reads records until it encounters
// an error that requires acquisition of a new shardIterator.
func (kc *kinesisCapture) readShard(shardID, sequenceID string) (string, error) {
	var nextSeqID = sequenceID

	var shardIterReq = kinesis.GetShardIteratorInput{
		StreamName: &kc.config.Stream,
		ShardId:    &shardID,
	}
	if nextSeqID != "" {
		shardIterReq.StartingSequenceNumber = &nextSeqID
		shardIterReq.ShardIteratorType = &START_AFTER_SEQ
	} else {
		shardIterReq.ShardIteratorType = &START_AT_BEGINNING
	}

	shardIterResp, err := kc.client.GetShardIteratorWithContext(kc.term.Ctx, &shardIterReq)
	if err != nil {
		return nextSeqID, err
	}

	var shardIter = shardIterResp.ShardIterator
	// GetRecords will immediately return a response without any records if there are none available
	// immediately. This means that this loop is executed very frequently, even when there is no
	// data available.
	for shardIter != nil && (*shardIter) != "" {
		var getRecordsReq = kinesis.GetRecordsInput{
			ShardIterator: shardIter,
			// TODO: determine a good value for Limit. This needs to be small enough to allow all
			// the records that are returned to come in under 10MiB total, or else we'll start
			// getting ProvisionedThroughputExceededException. Still looking into the specifics of
			// how this should work.
		}
		getRecordsResp, err := kc.client.GetRecordsWithContext(kc.term.Ctx, &getRecordsReq)
		// TODO: check if the err is retry-able and retry. For example, handle
		// ProvisionedThroughputExceededException, which indicates that we would exceed the 2MiB/S
		// throughput limit, by re-trying after a backoff.
		if err != nil {
			return nextSeqID, err
		}

		// If the response includes ChildShards, then this means that we've reached the end of the
		// shard because it has been either split or merged, so we need to start new reads of the
		// child shards.
		for _, childShard := range getRecordsResp.ChildShards {
			go kc.startReadingShard(*childShard.ShardId)
		}

		// TODO: handle the case where there's no records by delaying with a backoff
		if len(getRecordsResp.Records) > 0 {
			// send the records on the data channel. This could potentially take a while if our
			// batch of records is large and there's a lot of other shards competing to stuff the
			// same channel. If this takes over 5 minutes, then our shardIterator will expire,
			// requiring us to acquire a new one. But that's OK, because it means that the
			// backpressure is working as intended.
			nextSeqID = kc.stuffDataCh(shardID, getRecordsResp.Records)
		}

		// A new ShardIterator will be returned even when there's no records returned. We need to
		// pass this value in the next GetRecords call
		shardIter = getRecordsResp.NextShardIterator

	}
	return nextSeqID, nil
}

func (kc *kinesisCapture) stuffDataCh(shardID string, records []*kinesis.Record) (lastSequenceID string) {
	var lastI, i int
	for i < len(records) {
		// Wait for at least one record to be sent on the channel
		select {
		case kc.dataCh <- toDataMessage(shardID, records[i]):
			lastSequenceID = *records[i].SequenceNumber
			i++
		case <-kc.term.Ctx.Done():
			log.WithFields(log.Fields{
				"kinesisShardId": shardID,
				"capture":        kc.term.Spec.Capture,
			}).Info("Stopped reading kinesis shard due to context cancellation")
			return
		}

		// Send whatever messages the channel has room for at the moment without blocking
		for i < len(records) {
			select {
			case kc.dataCh <- toDataMessage(shardID, records[i]):
				lastSequenceID = *records[i].SequenceNumber
				i++
			default:
				break
			}
		}

		// Send a ControlMessage informing the runtime that there is data available
		kc.controlCh <- consumer.EnvelopeOrError{
			Envelope: message.Envelope{
				Journal: &pb.JournalSpec{
					Name:  pb.Journal(kc.term.Spec.Capture),
					Flags: pb.JournalSpec_O_RDONLY,
				},
				Message: captures.NewControlMessage(kc.producerID, i-lastI, kc.term.Revision),
			},
		}
		lastI = i
	}
	return
}

func toDataMessage(kinesisShardID string, record *kinesis.Record) captures.DataMessage {
	return captures.DataMessage{
		Document: record.Data,
		Stream:   kinesisShardID,
		Offset:   *record.SequenceNumber,
	}
}

var (
	START_AFTER_SEQ    = "AFTER_SEQUENCE_NUMBER"
	START_AT_BEGINNING = "TRIM_HORIZON"
)
