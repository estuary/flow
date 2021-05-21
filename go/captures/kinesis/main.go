package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/flow/go/captures"
	log "github.com/sirupsen/logrus"
)

func main() {
	var args = captures.ParseArgsOrExit()
	args.Run(spec, doCheck, doDiscover, doRead)
}

// TODO: update docs link to kinesis connector-specific docs after they are written
var spec = captures.Spec{
	SupportsIncremental:           true,
	SupportedDestinationSyncModes: captures.AllDestinationSyncModes,
	ConnectionSpecification: map[string]interface{}{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "Kinesis Source Spec",
		"type":    "object",
		"required": []string{
			"stream",
			"region",
			"awsAccessKeyId",
			"awsSecretAccessKey",
		},
		"properties": map[string]interface{}{
			"stream": map[string]interface{}{
				"type":        "string",
				"title":       "Kinesis Stream",
				"description": "The name of the Kinesis stream",
				"default":     "example-stream-name",
			},
			"region": map[string]interface{}{
				"type":        "string",
				"title":       "AWS Region",
				"description": "The name of the AWS region where the Kinesis stream is located",
				"default":     "us-east-1",
			},
			"awsAccessKeyId": map[string]interface{}{
				"type":        "string",
				"title":       "AWS Access Key ID",
				"description": "Part of the AWS credentials that will be used to connect to Kinesis",
				"default":     "example-aws-access-key-id",
			},
			"awsSecretAccessKey": map[string]interface{}{
				"type":        "string",
				"title":       "AWS Secret Access Key",
				"description": "Part of the AWS credentials that will be used to connect to Kinesis",
				"default":     "example-aws-secret-access-key",
			},
			"partitionRange": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"end": map[string]interface{}{
						"type":        "string",
						"pattern":     "^[0-9a-fA-F]{8}$",
						"title":       "Partition range begin",
						"description": "Unsigned 32 bit integer represented as a hexidecimal string, which is used to determine which partitions this instance will be responsible for",
					},
					"begin": map[string]interface{}{
						"type":        "string",
						"pattern":     "^[0-9a-fA-F]{8}$",
						"title":       "Partition range begin",
						"description": "Unsigned 32 bit integer represented as a hexidecimal string, which is used to determine which partitions this instance will be responsible for",
					},
				},
			},
		},
	},
}

func doCheck(args captures.CheckCmd) error {
	var result = &captures.ConnectionStatus{
		Status: captures.StatusSucceeded,
	}
	var _, err = tryListingStreams(args.ConfigFile)
	if err != nil {
		result.Status = captures.StatusFailed
		result.Message = err.Error()
	}
	return captures.NewStdoutEncoder().Encode(captures.Message{
		Type:             captures.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func tryListingStreams(configFile captures.ConfigFile) ([]string, error) {
	var _, client, err = parseConfigAndConnect(configFile)
	if err != nil {
		return nil, err
	}
	var ctx = context.Background()
	return listAllStreams(ctx, client)
}

func doDiscover(args captures.DiscoverCmd) error {
	var _, client, err = parseConfigAndConnect(args.ConfigFile)
	if err != nil {
		return err
	}
	var ctx = context.Background()
	streamNames, err := listAllStreams(ctx, client)

	var schema = captures.UnknownSchema()

	var catalog = captures.Catalog{
		Streams: make([]captures.Stream, len(streamNames)),
	}
	for i, name := range streamNames {
		catalog.Streams[i] = captures.Stream{
			Name:                name,
			JSONSchema:          schema,
			SupportedSyncModes:  []captures.SyncMode{captures.SyncModeIncremental},
			SourceDefinedCursor: true,
		}
	}

	var encoder = json.NewEncoder(os.Stdout)
	return encoder.Encode(&catalog)
}

func put(state *captures.State, source *kinesisSource, sequenceNumber string) {
	var streamMap, ok = state.Data[source.stream]
	if !ok {
		streamMap = make(map[string]interface{})
		state.Data[source.stream] = streamMap
	}
	streamMap.(map[string]interface{})[source.shardID] = sequenceNumber
}

func copyStreamState(state *captures.Message, stream string) (map[string]string, error) {
	if ss, ok := state.State.Data[stream].(map[string]interface{}); ok {
		var dest = make(map[string]string)
		for k, v := range ss {
			if vstr, ok := v.(string); ok {
				dest[k] = vstr
			} else {
				return nil, fmt.Errorf("found a non-string value in state map")
			}
		}
		return dest, nil
	} else {
		return nil, fmt.Errorf("invalid state object, expected values to be maps of string to string")
	}
}

func doRead(args captures.ReadCmd) error {
	var config, client, err = parseConfigAndConnect(args.ConfigFile)
	if err != nil {
		return err
	}
	var catalog captures.ConfiguredCatalog
	err = args.CatalogFile.Parse(&catalog)
	if err != nil {
		return fmt.Errorf("parsing configured catalog: %w", err)
	}
	err = catalog.Validate()
	if err != nil {
		return fmt.Errorf("configured catalog is invalid: %w", err)
	}

	var stateMessage = captures.Message{
		Type: captures.MessageTypeState,
		State: &captures.State{
			Data: make(map[string]interface{}),
		},
	}
	err = args.StateFile.Parse(&stateMessage.State.Data)
	if err != nil {
		return fmt.Errorf("parsing state file: %w", err)
	}

	var dataCh = make(chan readResult, 8)
	var ctx, cancelFunc = context.WithCancel(context.Background())

	log.WithField("streamCount", len(catalog.Streams)).Info("Starting to read stream(s)")

	for _, stream := range catalog.Streams {
		streamState, err := copyStreamState(&stateMessage, stream.Stream.Name)
		if err != nil {
			cancelFunc()
			return fmt.Errorf("invalid state for stream %s: %w", stream.Stream.Name, err)
		}
		go readStream(ctx, config, client, stream.Stream.Name, streamState, dataCh)
	}

	// We'll re-use this same message instance for all records we print
	var recordMessage = captures.Message{
		Type:   captures.MessageTypeRecord,
		Record: &captures.Record{},
	}
	// We're all set to start printing data to stdout
	var encoder = json.NewEncoder(os.Stdout)
	for {
		var next = <-dataCh
		if next.Error != nil {
			// time to bail
			var errMessage = captures.NewLogMessage(captures.LogLevelFatal, "read failed due to error: %v", next.Error)
			// Printing the error may fail, but we'll ignore that error and return the original
			_ = encoder.Encode(errMessage)
			cancelFunc()
			return next.Error
		} else {
			recordMessage.Record.Stream = next.Source.stream
			for _, record := range next.Records {
				recordMessage.Record.Data = record
				recordMessage.Record.EmittedAt = time.Now().UTC().UnixNano() / int64(time.Millisecond)
				var err = encoder.Encode(recordMessage)
				if err != nil {
					cancelFunc()
					return err
				}
			}
			put(stateMessage.State, next.Source, next.SequenceNumber)
			err = encoder.Encode(stateMessage)
			if err != nil {
				cancelFunc()
				return err
			}
		}
	}
}

func parseConfigAndConnect(configFile captures.ConfigFile) (config Config, client *kinesis.Kinesis, err error) {
	err = configFile.ConfigFile.Parse(&config)
	if err != nil {
		err = fmt.Errorf("parsing config file: %w", err)
		return
	}
	client, err = connect(&config)
	if err != nil {
		err = fmt.Errorf("failed to connect: %w", err)
	}
	return
}
