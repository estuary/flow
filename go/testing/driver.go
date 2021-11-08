package testing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/flow/ops"
	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/protocols/flow"
	"github.com/nsf/jsondiff"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// ClusterDriver implements a Driver which drives actions against a data plane.
type ClusterDriver struct {
	sc  pc.ShardClient
	rjc pb.RoutedJournalClient
	tc  pf.TestingClient
	// ID of the build under test.
	buildID string
	// Index of collection specs which may be referenced by test steps.
	collections map[pf.Collection]*pf.CollectionSpec
	// Compiled schema index of tests.
	schemas *bindings.SchemaIndex
}

// NewClusterDriver builds a ClusterDriver from the provided cluster clients,
// schemas, and collections.
func NewClusterDriver(
	ctx context.Context,
	sc pc.ShardClient,
	rjc pb.RoutedJournalClient,
	tc pf.TestingClient,
	buildID string,
	bundle *pf.SchemaBundle,
	collections []*pf.CollectionSpec,
) (*ClusterDriver, error) {
	var schemas, err = bindings.NewSchemaIndex(bundle)
	if err != nil {
		return nil, fmt.Errorf("compiling schema index: %w", err)
	}

	var collectionIndex = make(map[pf.Collection]*pf.CollectionSpec, len(collections))
	for _, spec := range collections {
		collectionIndex[spec.Collection] = spec
	}

	var driver = &ClusterDriver{
		sc:          sc,
		rjc:         rjc,
		tc:          tc,
		buildID:     buildID,
		schemas:     schemas,
		collections: collectionIndex,
	}

	return driver, nil
}

// Stat implements Driver for a Cluster.
func (c *ClusterDriver) Stat(ctx context.Context, stat PendingStat) (readThrough *Clock, writeAt *Clock, err error) {
	log.WithFields(log.Fields{
		"task":        stat.TaskName,
		"readyAt":     stat.ReadyAt,
		"readThrough": stat.ReadThrough.Offsets,
		"revision":    stat.ReadThrough.Etcd.Revision,
	}).Debug("starting stat")

	shards, err := consumer.ListShards(ctx, c.sc, &pc.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet(flowLabels.TaskName, string(stat.TaskName)),
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
		resp, err := c.sc.Stat(ctx, &pc.StatRequest{
			Shard:       shard.Spec.Id,
			ReadThrough: stat.ReadThrough.Offsets,
			Extension:   extension,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("stating shard: %w", err)
		} else if resp.Status != pc.Status_OK {
			return nil, nil, fmt.Errorf("shard !OK: %v", resp.Status)
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
func (c *ClusterDriver) Ingest(ctx context.Context, test *pf.TestSpec, testStep int) (writeAt *Clock, _ error) {
	log.WithFields(log.Fields{
		"test":     test.Test,
		"testStep": testStep,
	}).Debug("starting ingest")
	var step = test.Steps[testStep]

	resp, err := c.tc.Ingest(ctx, &pf.IngestRequest{
		Collection:    step.Collection,
		BuildId:       c.buildID,
		DocsJsonLines: step.DocsJsonLines,
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
func (c *ClusterDriver) Advance(ctx context.Context, delta TestTime) error {
	var _, err = c.tc.AdvanceTime(ctx, &pf.AdvanceTimeRequest{
		AdvanceSeconds: uint64(delta / TestTime(time.Second)),
	})
	return err
}

// Verify implements Driver for a Cluster.
func (c *ClusterDriver) Verify(ctx context.Context, test *pf.TestSpec, testStep int, from, to *Clock) error {
	log.WithFields(log.Fields{
		"test":     test.Test,
		"testStep": testStep,
	}).Debug("starting verify")
	var step = test.Steps[testStep]

	var fetched, err = FetchDocuments(ctx, c.rjc, step.Partitions, from.Offsets, to.Offsets)
	if err != nil {
		return err
	}
	var collection, ok = c.collections[step.Collection]
	if !ok {
		return fmt.Errorf("unknown collection %s", step.Collection)
	}
	actual, err := CombineDocuments(collection, c.schemas, fetched)
	if err != nil {
		return err
	}

	var expected = strings.Split(step.DocsJsonLines, "\n")
	if len(expected) == 1 && len(expected[0]) == 0 {
		expected = nil // Split("") => [][]string{""} ; map to nil.
	}

	var diffOptions = jsondiff.DefaultConsoleOptions()
	// The default behavior of jsondiff is to compare the exact string representations of numbers.
	// This isn't what we want here, since the numbers in the "actual" documents may be produced by
	// mathematical operations on floats, which can result in some loss of precision. Additionally,
	// we want to accept cases like `1.0` and `1` by treating them as equal.
	diffOptions.CompareNumbers = compareNumbers
	var index int
	var failures testFailures

	// Compare matched |expected| and |actual| documents.
	for index = 0; index != len(expected) && index != len(actual); index++ {
		var mode, diffs = jsondiff.Compare(actual[index], []byte(expected[index]), &diffOptions)

		switch mode {
		case jsondiff.FullMatch, jsondiff.SupersetMatch:
			// Pass.
		default:
			failures = append(failures, failure{
				docIndex: index,
				diff:     diffs,
			})
		}
	}

	// Error on remaining |expected| or |actual| documents.
	var prettyEnc = json.NewEncoder(os.Stdout)
	prettyEnc.SetIndent("", "    ")

	for ; index < len(expected); index++ {
		failures = append(failures, failure{
			docIndex: index,
			expected: json.RawMessage(expected[index]),
		})
	}

	for ; index < len(actual); index++ {
		failures = append(failures, failure{
			docIndex: index,
			actual:   json.RawMessage(actual[index]),
		})
	}

	if failures != nil {
		return failures
	}

	log.WithFields(log.Fields{
		"test":     test.Test,
		"testStep": testStep,
	}).Debug("verify complete")
	return nil
}

// epsilon is used when comparing floating point numbers. This is the same value as FLT_EPSILON
// from C, also known as the "machine epsilon".
var epsilon = math.Nextafter(1.0, 2.0) - 1.0

func compareNumbers(a, b json.Number) bool {
	// If the string representations are the same, then we always return true. This allows
	// for a somewhat meaningful comparison if the two numbers are out of range for a float64, and
	// is also a fast path for numbers that happen to match exactly.
	if a == b {
		return true
	}
	var aFloat, aErr = a.Float64()
	var bFloat, bErr = b.Float64()
	if aErr != nil || bErr != nil {
		// Parsing the numbers as floats can fail if they're out of range. In this case, we return
		// false because we already know that their string representations are different.
		return false
	}

	// Scale the epsilon based on the relative size of the numbers being compared.
	// For numbers greater than 2.0, EPSILON will be smaller than the difference between two
	// adjacent floats, so it needs to be scaled up. For numbers smaller than 1.0, EPSILON could
	// easily be larger than the numbers we're comparing and thus needs scaled down. This method
	// could still break down for numbers that are very near 0, but it's the best we can do
	// without knowing the relative scale of such numbers ahead of time.
	var scaledEpsilon = epsilon * math.Max(math.Abs(aFloat), math.Abs(bFloat))
	return math.Abs(aFloat-bFloat) < scaledEpsilon
}

type testFailures []failure

type failure struct {
	docIndex int
	actual   json.RawMessage
	expected json.RawMessage
	diff     string
}

func (f failure) describe(b *strings.Builder) {
	var encoder = json.NewEncoder(b)
	encoder.SetIndent("", "    ")
	if len(f.actual) > 0 {
		b.WriteString("Unexpected actual document:\n")
		encoder.Encode(f.actual)
	} else if len(f.expected) > 0 {
		fmt.Fprintf(b, "Missing expected document at index %d:\n", f.docIndex)
		encoder.Encode(f.expected)
	} else {
		fmt.Fprintf(b, "mismatched document at index %d:\n", f.docIndex)
		b.WriteString(f.diff)
	}
}

func (r testFailures) Error() string {
	var b strings.Builder
	b.WriteString("actual and expected document(s) did not match:\n")
	for _, f := range r {
		f.describe(&b)
		b.WriteRune('\n')
	}
	return b.String()
}

// FetchDocuments fetches the documents contained in journals matching the given
// selector, within the offset ranges bounded by |from| and |to|. If a journal
// isn't contained in |from|, then it's read from byte offset zero. If a journal
// isn't contained in |to|, then it's read through its current write head.
func FetchDocuments(ctx context.Context, rjc pb.RoutedJournalClient, selector pb.LabelSelector, from, to pb.Offsets) ([][]byte, error) {
	var listing, err = client.ListAllJournals(ctx, rjc, pb.ListRequest{Selector: selector})
	if err != nil {
		return nil, fmt.Errorf("listing journals: %w", err)
	}

	// Collect all content written across all journals in |listing| between |from| and |to|.
	var content bytes.Buffer

	for _, journal := range listing.Journals {
		var req = pb.ReadRequest{
			Journal:   journal.Spec.Name,
			Offset:    from[journal.Spec.Name],
			EndOffset: to[journal.Spec.Name],
			Block:     false,
		}
		log.WithField("req", req.String()).Debug("reading journal content")

		if req.Offset == req.EndOffset {
			// Skip.
		} else if _, err = io.Copy(&content, client.NewReader(ctx, rjc, req)); err != nil {
			return nil, fmt.Errorf("reading journal %s: %w", journal.Spec.Name, err)
		}
	}

	// Split |content| into newline-separated documents.
	var documents = bytes.Split(bytes.TrimRight(content.Bytes(), "\n"), []byte{'\n'})
	if len(documents) == 1 && len(documents[0]) == 0 {
		documents = nil // Split([]byte{nil}) => [][]byte{{}} ; map to nil.
	}

	return documents, nil
}

// CombineDocuments input |documents| under the collection's key and schema,
// and using the provided SchemaIndex. Non-content documents (ACKs) are filtered.
// Combined documents, one per collection key, are returned.
func CombineDocuments(
	collection *pf.CollectionSpec,
	schemas *bindings.SchemaIndex,
	documents [][]byte,
) ([][]byte, error) {
	// Feed documents into an extractor, to extract UUIDs.
	var extractor, err = bindings.NewExtractor()
	if err != nil {
		return nil, fmt.Errorf("creating extractor: %w", err)
	} else if err = extractor.Configure(collection.UuidPtr, nil, "", nil); err != nil {
		return nil, fmt.Errorf("configuring extractor: %w", err)
	}
	for _, d := range documents {
		extractor.Document(d)
	}
	uuids, _, err := extractor.Extract()
	if err != nil {
		return nil, fmt.Errorf("extracting UUIDs: %w", err)
	}

	combiner, err := bindings.NewCombine(ops.StdLogger())
	if err != nil {
		return nil, fmt.Errorf("creating combiner: %w", err)
	} else if err = combiner.Configure(
		collection.Collection.String(),
		schemas,
		collection.Collection,
		collection.SchemaUri,
		"", // Don't populate UUID placeholder.
		collection.KeyPtrs,
		nil, // Don't extract additional fields.
	); err != nil {
		return nil, fmt.Errorf("configuring combiner: %w", err)
	}

	for d := range documents {
		if uuids[d].ProducerAndFlags&uint64(message.Flag_ACK_TXN) != 0 {
			continue
		}

		var err = combiner.CombineRight(json.RawMessage(documents[d]))
		if err != nil {
			return nil, fmt.Errorf("combine-right failed: %w", err)
		}
	}

	// Drain actual documents from the combiner.
	var actual [][]byte
	err = combiner.Drain(func(_ bool, doc json.RawMessage, _, _ []byte) error {
		actual = append(actual, doc)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("combiner.Finish failed: %w", err)
	}

	return actual, nil
}

// Initialize fetches existing collection offsets from the cluster,
// models them as completed ingestions, and ensures all downstream dataflows have
// completed. On its return, the internal write clock of the Graph reflects the
// current cluster state.
func Initialize(ctx context.Context, driver *ClusterDriver, graph *Graph) error {
	for _, collection := range driver.collections {
		// List journals of the collection.
		list, err := client.ListAllJournals(ctx, driver.rjc,
			flow.ListPartitionsRequest(collection))
		if err != nil {
			return fmt.Errorf("listing journals of %s: %w", collection.Collection, err)
		}

		// Fetch offsets of each journal.
		var offsets = make(pb.Offsets)
		for _, journal := range list.Journals {
			var r = client.NewReader(ctx, driver.rjc, pb.ReadRequest{
				Journal:      journal.Spec.Name,
				Offset:       -1,
				Block:        false,
				MetadataOnly: true,
			})
			if _, err := r.Read(nil); err != client.ErrOffsetNotYetAvailable {
				return fmt.Errorf("reading head of journal %v: %w", journal.Spec.Name, err)
			}

			offsets[journal.Spec.Name] = r.Response.Offset
		}

		// Track it as a completed ingestion.
		graph.CompletedIngest(collection.Collection, &Clock{Etcd: pb.Header_Etcd{}, Offsets: offsets})
	}

	// Run an empty test to poll all Stats implied by the completed ingests.
	// This ensures that all downstream effects of data already in the cluster
	// have completed.
	var _, err = RunTestCase(ctx, graph, driver, &pf.TestSpec{})

	return err
}
