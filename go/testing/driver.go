package testing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/bindings"
	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/nsf/jsondiff"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// Stat implements Driver for a Cluster.
func (c *Cluster) Stat(stat PendingStat) (readThrough *Clock, writeAt *Clock, err error) {
	log.WithField("stat", stat).Debug("starting stat")

	var ctx = c.Tasks.Context()
	shards, err := consumer.ListShards(ctx, c.Shards, &pc.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet(flowLabels.Derivation, stat.Derivation.String()),
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list shards: %w", err)
	}

	extension, err := stat.ReadThrough.Etcd.Marshal()
	if err != nil {
		return nil, nil, err
	}

	// Build two clocks:
	//  - Clock which is the *minimum read* progress across all shard responses.
	//  - Clock which is the *maximum write* progress across all shard responses.
	readThrough = new(Clock)
	writeAt = new(Clock)

	for _, shard := range shards.Shards {
		resp, err := c.Shards.Stat(ctx, &pc.StatRequest{
			Shard:       shard.Spec.Id,
			ReadThrough: stat.ReadThrough.Offsets,
			Extension:   extension,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to stat shard: %w", err)
		}

		var journalEtcd pb.Header_Etcd
		if err = journalEtcd.Unmarshal(resp.Extension); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal stat response extension: %w", err)
		}

		readThrough.ReduceMin(journalEtcd, resp.ReadThrough)
		writeAt.ReduceMax(journalEtcd, resp.PublishAt)
	}

	log.WithFields(log.Fields{
		"stat":        stat,
		"readThrough": *readThrough,
		"writeAt":     *writeAt,
	}).Debug("stat complete")

	return readThrough, writeAt, nil
}

// Ingest implements Driver for a Cluster.
func (c *Cluster) Ingest(test *pf.TestSpec, testStep int) (writeAt *Clock, _ error) {
	log.WithFields(log.Fields{
		"test":     test.Test,
		"testStep": testStep,
	}).Debug("starting ingest")
	var step = test.Steps[testStep]

	var resp, err = pf.NewIngesterClient(c.Server.GRPCLoopback).
		Ingest(c.Tasks.Context(),
			&pf.IngestRequest{
				Collections: []pf.IngestRequest_Collection{
					{
						Name:          step.Collection,
						DocsJsonLines: []byte(step.DocsJsonLines),
					},
				},
			})

	if err != nil {
		return nil, err
	}

	writeAt = new(Clock)
	writeAt.ReduceMax(resp.JournalEtcd, resp.JournalWriteHeads)

	log.WithFields(log.Fields{
		"test":     test.Test,
		"testStep": testStep,
		"writeAt":  *writeAt,
	}).Debug("ingest complete")

	return writeAt, nil
}

// Advance implements Driver for a Cluster.
func (c *Cluster) Advance(delta TestTime) error {
	log.WithField("delta", delta).Debug("advancing time")

	var t1 = atomic.AddInt64((*int64)(&c.Ingester.PublishClockDelta), int64(delta))
	var t2 = atomic.AddInt64((*int64)(&c.Consumer.Service.PublishClockDelta), int64(delta))

	if t1 != t2 {
		panic("ingester & consumer clock deltas should match")
	}

	// Kick current timepoint to unblock gated shuffled reads.
	c.Consumer.Timepoint.Mu.Lock()
	c.Consumer.Timepoint.Now.Next.Resolve(time.Now())
	c.Consumer.Timepoint.Now = c.Consumer.Timepoint.Now.Next
	c.Consumer.Timepoint.Mu.Unlock()

	return nil
}

// Verify implements Driver for a Cluster.
func (c *Cluster) Verify(test *pf.TestSpec, testStep int, from, to *Clock) error {
	log.WithFields(log.Fields{
		"test":     test.Test,
		"testStep": testStep,
		"from":     *from,
		"to":       *to,
	}).Debug("starting verify")
	var step = test.Steps[testStep]

	var ctx = c.Tasks.Context()
	var listing, err = client.ListAllJournals(ctx, c.Journals,
		pb.ListRequest{
			Selector: step.Partitions,
		})
	if err != nil {
		return fmt.Errorf("failed to list journals: %w", err)
	}

	// Collect all content written across all journals in |listing| between |from| and |to|.
	var content bytes.Buffer

	for _, journal := range listing.Journals {
		var req = pb.ReadRequest{
			Journal:   journal.Spec.Name,
			Offset:    from.Offsets[journal.Spec.Name],
			EndOffset: to.Offsets[journal.Spec.Name],
			Block:     true,
		}
		log.WithField("req", req).Debug("reading journal content")

		if req.Offset == req.EndOffset {
			// A read at the journal head blocks until the offset is written,
			// despite EndOffset, so don't issue the read.
		} else if _, err = io.Copy(&content, client.NewReader(ctx, c.Journals, req)); err != nil {
			return fmt.Errorf("failed to read journal: %w", err)
		}
	}

	// Split |content| into newline-separated documents.
	var documents = bytes.Split(bytes.TrimRight(content.Bytes(), "\n"), []byte{'\n'})
	if len(documents) == 1 && len(documents[0]) == 0 {
		documents = nil // Split([]byte{nil}) => [][]byte{{}} ; map to nil.
	}

	// Feed documents into an extractor, to extract UUIDs.
	extractor, err := bindings.NewExtractor(step.CollectionUuidPtr, nil)
	if err != nil {
		return fmt.Errorf("failed to build extractor: %w", err)
	}
	for _, d := range documents {
		extractor.Document(d)
	}
	uuids, _, err := extractor.Extract()
	if err != nil {
		return fmt.Errorf("failed to extract UUIDs: %w", err)
	}

	// Now feed documents into a combiner, filtering documents which are ACKs.
	combiner, err := bindings.NewCombineBuilder(c.SchemaIndex).Open(
		step.CollectionSchemaUri,
		step.CollectionKeyPtr,
		nil,
		"", // Don't populate UUID placeholder.
	)
	if err != nil {
		return fmt.Errorf("failed to build combiner: %w", err)
	}
	for d := range documents {
		if uuids[d].ProducerAndFlags&uint64(message.Flag_ACK_TXN) != 0 {
			continue
		}
		log.WithFields(log.Fields{
			"document": string(documents[d]),
		}).Debug("combining non-ack document")

		var err = combiner.CombineRight(json.RawMessage(documents[d]))
		if err != nil {
			return fmt.Errorf("combine-right failed: %w", err)
		}
	}

	// Drain actual documents from the combiner.
	var actual [][]byte
	err = combiner.Finish(func(_ bool, doc json.RawMessage, _, _ []byte) error {
		actual = append(actual, doc)
		return nil
	})
	if err != nil {
		return fmt.Errorf("combiner.Finish failed: %w", err)
	}

	var expected = strings.Split(step.DocsJsonLines, "\n")
	if len(expected) == 1 && len(expected[0]) == 0 {
		expected = nil // Split("") => [][]string{""} ; map to nil.
	}

	var diffOptions = jsondiff.DefaultConsoleOptions()
	var failed bool
	var index int

	// Compare matched |expected| and |actual| documents.
	for index = 0; index != len(expected) && index != len(actual); index++ {
		var mode, diffs = jsondiff.Compare(actual[index], []byte(expected[index]), &diffOptions)

		switch mode {
		case jsondiff.FullMatch, jsondiff.SupersetMatch:
			// Pass.
		default:
			log.WithFields(log.Fields{
				"test":          test.Test,
				"testStep":      testStep,
				"documentIndex": index,
			}).Error("actual and expected documents don't match")
			fmt.Fprintln(os.Stderr, diffs)
			failed = true
		}
	}

	// Error on remaining |expected| or |actual| documents.
	var prettyEnc = json.NewEncoder(os.Stderr)
	prettyEnc.SetIndent("", "    ")

	for ; index < len(expected); index++ {
		log.WithFields(log.Fields{
			"test":          test.Test,
			"testStep":      testStep,
			"documentIndex": index,
		}).Error("expected document not seen")

		if err = prettyEnc.Encode(json.RawMessage(expected[index])); err != nil {
			return fmt.Errorf("encoding extra expected document: %w", err)
		}
		failed = true
	}

	for ; index < len(actual); index++ {
		log.WithFields(log.Fields{
			"test":          test.Test,
			"testStep":      testStep,
			"documentIndex": index,
		}).Error("actual document not expected")

		if err = prettyEnc.Encode(json.RawMessage(actual[index])); err != nil {
			return fmt.Errorf("encoding extra actual document: %w", err)
		}
		failed = true
	}

	if failed {
		return fmt.Errorf("actual and expected documents don't match")
	}

	log.WithFields(log.Fields{
		"test":     test.Test,
		"testStep": testStep,
	}).Debug("verify complete")
	return nil
}
