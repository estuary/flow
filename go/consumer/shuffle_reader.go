package consumer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// EnvelopeOrError is an Envelope or an Error.
type EnvelopeOrError struct {
	message.Envelope
	Err error
}

type shuffleReader struct {
	shard   consumer.Shard
	offsets map[pb.Journal]pb.Offset
	ch      chan<- EnvelopeOrError
}

func startReadingMessages(shard consumer.Shard, checkpoint pc.Checkpoint, ch chan<- EnvelopeOrError) error {
	var offsets = make(map[pb.Journal]pb.Offset)
	for j, js := range checkpoint.Sources {
		offsets[j] = js.ReadThrough
	}

	var sr = &shuffleReader{
		shard:   shard,
		offsets: offsets,
		ch:      ch,
	}
	return nil
}

func (sr *shuffleReader) converge(shard consumer.Shard, offsets map[pb.Journal]pb.Offset) error {

	catalogURL, err := getLabel(shard.Spec(), labels.CatalogURL)
	if err == nil {
		return err
	}
	catalogURL += "?immutable=true"

	// TODO: pull down database, if not already cached.
	db, err := sql.Open("sqlite3", catalogURL)
	if err != nil {
		return fmt.Errorf("opening catalog database %v: %w", catalogURL, err)
	}
	defer db.Close()

	derivation, err := getLabel(shard.Spec(), labels.Derivation)
	if err == nil {
		return err
	}
	transforms, err := loadTransforms(db, derivation)
	if err != nil {
		return err
	}

	// Build union selector over all transform collections.
	// We'll filter down from here by applying partition selectors.
	var selector pb.LabelSelector
	for _, t := range transforms {
		selector.Include.AddValue(labels.Collection, t.sourceName)
	}

	list, err := client.ListAllJournals(shard.Context(), shard.JournalClient(), pb.ListRequest{
		Selector: selector,
	})
	if err != nil {
		return fmt.Errorf("failed to poll collection journals: %w", err)
	}

	var desiredReads = make(map[pb.Journal]struct{})

	for _, journal := range list.Journals {

		for _, transform := range transforms {
			if transform.sourcePartitions.Matches(journal.Spec.LabelSet) {
				continue
			}

		}

	}

	var workerIndex int
	var workerRing []pf.ShuffleRequest_ReaderRing

	if l, err := getLabel(shard.Spec(), labels.WorkerIndex); err != nil {
		return err
	} else if workerIndex, err = strconv.Atoi(l); err != nil {
		return fmt.Errorf("failed to parse worker index: %w", err)
	}

	if l, err := getLabel(shard.Spec(), labels.WorkerRing); err != nil {
		return err
	} else if workerRing, err = parseRingLabel(l); err != nil {
		return fmt.Errorf("failed to parse %q: %w", labels.WorkerRing, err)
	}

	return nil
}

func parseRingLabel(label string) ([]pf.ShuffleRequest_ReaderRing, error) {
	var arr []uint64
	if err := json.Unmarshal([]byte(label), &arr); err != nil {
		return nil, err
	} else if l := len(arr); l%2 != 1 {
		return nil, fmt.Errorf("expected array to be odd-length (got %v)", l)
	}
	// Map delta-encoded unix timestamps into absolute ones.
	for i := 3; i < len(arr); i += 2 {
		arr[i] += arr[i-2]
	}
	var out = []pf.ShuffleRequest_ReaderRing{
		{
			ClockLowerBound: 0,
			TotalReaders:    uint32(arr[0]),
		},
	}
	for i := 1; i != len(arr); i += 2 {
		out = append(out, pf.ShuffleRequest_ReaderRing{
			ClockLowerBound: message.NewClock(time.Unix(int64(arr[i]), 0)),
			TotalReaders:    uint32(arr[i+1]),
		})
	}
	return out, nil
}
