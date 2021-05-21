package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/service/kinesis"
	log "github.com/sirupsen/logrus"
)

type kinesisCapture struct {
	client             *kinesis.Kinesis
	ctx                context.Context
	stream             string
	config             Config
	dataCh             chan<- readResult
	readingShards      map[string]bool
	readingShardsMutex sync.Mutex
	// shardSequences is a copy of the capture state, which just tracks the sequenceID for each
	// kinesis shard. We keep this as a struct field so that we can ensure that all reads will use
	// the same state, regardless of whether they're triggered by the initial shard listing or
	// returned as a child shard id when reaching the end of an existing shard.
	shardSequences map[string]string
}

type kinesisSource struct {
	stream  string
	shardID string
}

type readResult struct {
	Source         *kinesisSource
	Error          error
	Records        []map[string]interface{}
	SequenceNumber string
}

func readStream(ctx context.Context, config Config, client *kinesis.Kinesis, stream string, state map[string]string, dataCh chan<- readResult) {
	var kc = &kinesisCapture{
		client:         client,
		ctx:            ctx,
		stream:         stream,
		config:         config,
		dataCh:         dataCh,
		readingShards:  make(map[string]bool),
		shardSequences: state,
	}
	var err = kc.startReadindStream()
	if err != nil {
		select {
		case dataCh <- readResult{
			Source: &kinesisSource{
				stream: stream,
			},
			Error: err,
		}:
		case <-ctx.Done():
		}
	} else {
		log.WithField("stream", stream).Infof("Started reading kinesis stream")
	}
}

func (kc *kinesisCapture) startReadindStream() error {
	allShardIds, err := kc.listShards()
	if err != nil {
		return fmt.Errorf("listing kinesis shards: %w", err)
	} else if len(allShardIds) == 0 {
		// TODO: Verify if it's even possible for a kinesis stream to have 0 shards
		return fmt.Errorf("No kinesis shards found for the given stream")
	}
	// Start the background goroutine that will buffer data and send control messages to notify the
	// Flow consumer when data is available.
	//go kc.startMessagePump()

	// Start reading from all the known shards.
	for _, kinesisShardID := range allShardIds {
		go kc.startReadingShard(kinesisShardID)
	}
	return nil
}

// TODO: we may not want to return _all_ shards here, but only the oldest parent shards, since child
// shards will be read automatically after reaching the end of the parents. If we start reading
// child shards immediately, then people may see events ingested out of order if they've merged or
// split shards recently.
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
		listShardsResp, err := kc.client.ListShardsWithContext(kc.ctx, &listShardsReq)
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
		"kinesisStream":  kc.stream,
		"kinesisShardId": shardID,
	})
	if !kc.config.PartitionRange.Includes([]byte(shardID)) {
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
	var source = &kinesisSource{
		stream:  kc.stream,
		shardID: shardID,
	}
	var shardReader = kinesisShardReader{
		parent:         kc,
		source:         source,
		lastSequenceID: kc.shardSequences[shardID],
		noDataBackoff: backoff{
			initialMillis: 200,
			maxMillis:     1000,
			multiplier:    1.5,
		},
		errorBackoff: backoff{
			initialMillis: 200,
			maxMillis:     5000,
			multiplier:    1.5,
		},
		// The maximum number of records to return in a single GetRecords request. We start with the
		// maximum allowed by kinesis, which is also their default. But this will be decreased if we
		// start seeing shardIterators expiring, since that indicates that it's taking more than 5
		// minutes to process all the records that were returned.
		limitPerReq: 10000,
		logEntry:    logEntry,
	}

	var err error
	var shardIter string
	for {
		shardIter, err = kc.getShardIterator(shardID, shardReader.lastSequenceID)
		if err != nil {
			if isRetryable(err) {
				select {
				case <-shardReader.errorBackoff.nextBackoff():
					err = nil
					// loop around and try again
				case <-kc.ctx.Done():
					err = kc.ctx.Err()
					break
				}
			} else if isMissingResource(err) {
				// This means that the shard fell off the end of the kinesis retention period
				// sometime after we started trying to read it. This is probably not indicative of
				// any problem, but rather just peculiar timing where we start reading the shard
				// right before the last record in the shard expires. This should only really be
				// possible
				logEntry.Info("Stopping read of kinesis shard because it has been deleted")
				break
			} else {
				// oh well, we tried. Time to call it a day
				logEntry.WithField("error", err).Error("reading kinesis shard failed")
				var message = readResult{
					Error:  err,
					Source: source,
				}
				select {
				case kc.dataCh <- message:
					break
				case <-kc.ctx.Done():
					break
				}
			}
		} else {
			err = shardReader.readShardIterator(shardIter)
			if err != nil {
				logEntry.WithField("error", err).Warn("reading kinesis shardIterator returned error (will retry)")
				switch err.(type) {
				case *kinesis.ExpiredIteratorException:

				}
			}
		}
	}
	logEntry.Info("Finished reading kinesis shard")
}

func isMissingResource(err error) bool {
	switch err.(type) {
	case *kinesis.ResourceNotFoundException:
		return true
	default:
		return false
	}
}

type kinesisShardReader struct {
	parent         *kinesisCapture
	source         *kinesisSource
	lastSequenceID string
	noDataBackoff  backoff
	errorBackoff   backoff
	limitPerReq    int64
	logEntry       *log.Entry
}

// Continuously loops and reads records until it encounters an error that requires acquisition of a new shardIterator.
func (r *kinesisShardReader) readShardIterator(iteratorID string) (err error) {
	var shardIter = &iteratorID

	var errorBackoff = backoff{
		initialMillis: 250,
		maxMillis:     5000,
		multiplier:    2.0,
	}
	// This separate backoff is used only for cases where GetRecords returns no data.
	// The initialMillis is set to match the 5 TPS rate limit of the api.
	var noDataBackoff = backoff{
		initialMillis: 200,
		maxMillis:     1000,
		multiplier:    1.5,
	}
	// GetRecords will immediately return a response without any records if there are none available
	// immediately. This means that this loop is executed very frequently, even when there is no
	// data available.
	for shardIter != nil && (*shardIter) != "" {
		var getRecordsReq = kinesis.GetRecordsInput{
			ShardIterator: shardIter,
			Limit:         &r.limitPerReq,
		}
		var getRecordsResp *kinesis.GetRecordsOutput
		getRecordsResp, err = r.parent.client.GetRecordsWithContext(r.parent.ctx, &getRecordsReq)
		if err != nil {
			if isRetryable(err) {
				r.logEntry.WithField("error", err).Warn("got kinesis error (will retry)")
				err = nil
				select {
				case <-errorBackoff.nextBackoff():
				case <-r.parent.ctx.Done():
					err = r.parent.ctx.Err()
					return
				}
			} else {
				r.logEntry.WithField("error", err).Warn("reading kinesis shard iterator failed")
				return
			}
		} else {
			errorBackoff.reset()
		}

		// If the response includes ChildShards, then this means that we've reached the end of the
		// shard because it has been either split or merged, so we need to start new reads of the
		// child shards.
		for _, childShard := range getRecordsResp.ChildShards {
			go r.parent.startReadingShard(*childShard.ShardId)
		}

		if len(getRecordsResp.Records) > 0 {
			noDataBackoff.reset()
			var parsed []map[string]interface{}
			parsed, err = r.parseRecords(getRecordsResp.Records)
			var lastSequenceID = *getRecordsResp.Records[len(getRecordsResp.Records)-1].SequenceNumber
			var msg = readResult{
				Source:         r.source,
				Records:        parsed,
				Error:          err,
				SequenceNumber: lastSequenceID,
			}
			select {
			case r.parent.dataCh <- msg:
				r.lastSequenceID = lastSequenceID
			case <-r.parent.ctx.Done():
				err = r.parent.ctx.Err()
				return
			}
		} else {
			// If there were no records in the response then we'll wait at least a while before
			// making another request. The amount of time we wait depends on whether we're caught up
			// or not. If the response indicates that there is more data in the shard, then we'll
			// wait the minimum amount of time so that we don't overflow the 5 TPS rate limit on
			// GetRecords. If we're caught up, then we'll increase the backoff a bit.
			if getRecordsResp.MillisBehindLatest != nil && *getRecordsResp.MillisBehindLatest > 0 {
				noDataBackoff.reset()
			}
			<-noDataBackoff.nextBackoff()
		}

		// A new ShardIterator will be returned even when there's no records returned. We need to
		// pass this value in the next GetRecords call. If we've reached the end of a shard, then
		// NextShardIterator will be empty, causing us to exit this loop and finish the read.
		shardIter = getRecordsResp.NextShardIterator
	}
	return nil
}

// Currently this expects all kinesis records to just be json, but we may add configuration options
// to allow for parsing other formats.
func (r *kinesisShardReader) parseRecords(records []*kinesis.Record) ([]map[string]interface{}, error) {
	// TODO: It may be worth recycling readResults so that we can alleviate some pressure on the GC
	var results = make([]map[string]interface{}, len(records))
	for i, record := range records {
		var doc = make(map[string]interface{})
		var err = json.Unmarshal(record.Data, &doc)
		if err != nil {
			return nil, fmt.Errorf("error parsing kinesis record with sequenceNumber '%s': %w", *record.SequenceNumber, err)
		}
		results[i] = doc
	}
	return results, nil
}

func (kc *kinesisCapture) getShardIterator(shardID, sequenceID string) (string, error) {
	var shardIterReq = kinesis.GetShardIteratorInput{
		StreamName: &kc.config.Stream,
		ShardId:    &shardID,
	}
	if sequenceID != "" {
		shardIterReq.StartingSequenceNumber = &sequenceID
		shardIterReq.ShardIteratorType = &START_AFTER_SEQ
	} else {
		shardIterReq.ShardIteratorType = &START_AT_BEGINNING
	}

	shardIterResp, err := kc.client.GetShardIteratorWithContext(kc.ctx, &shardIterReq)
	if err != nil {
		return "", err
	}
	return *shardIterResp.ShardIterator, nil
}

var (
	START_AFTER_SEQ    = "AFTER_SEQUENCE_NUMBER"
	START_AT_BEGINNING = "TRIM_HORIZON"
)
