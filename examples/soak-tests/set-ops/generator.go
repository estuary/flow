package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"math/bits"
	"math/rand"
	"os"
	"time"

	"golang.org/x/time/rate"
)

// Generates operation documents. This is kept as a separate file so that it can be run by itself for
// other manual tests.

// We're using these budget model flags because they can easily be used in tests.
var streamCount = flag.Int("streams", 100, "Number of concurrent streams")
var keys = flag.String("keys", "abcdefghijklmnopqrstuvwxyz", "Characters from which sets are drawn")
var keysPerOp = flag.Int("keys-per-op", 7, "Maximum number of keys in a single set operation")
var maxOpsPerSecond = flag.Int("ops-per-second", 1000, "Maximum number of operations per second to generate")

// Stream holds incremental stream state.
type Stream struct {
	id, add, rem, verify int
	values               map[string]int
}

func (s *Stream) opCounter() int {
	return s.add + s.rem + s.verify
}

// Matches the schema of the soak/set-ops/operations collection
type Operation struct {
	Author       int            `json:"author"`
	ID           int            `json:"id"`
	Ones         int            `json:"ones"`
	Op           int            `json:"op"`
	Type         string         `json:"type"`
	Values       map[string]int `json:"values"`
	ExpectValues map[string]int `json:"expectValues"`
	Timestamp    string         `json:"timestamp"`
}

type generatorConfig struct {
	Author       int
	Concurrent   int
	Keys         string
	KeysPerOp    int
	OpsPerSecond int
}

func newGeneratorConfig() generatorConfig {
	return generatorConfig{
		Author:     rand.Intn(1 << 16),
		Concurrent: *streamCount,
		Keys:       *keys,
		KeysPerOp:  *keysPerOp,
	}
}

func clear(m map[string]int) {
	for k := range m {
		delete(m, k)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()
	var cfg = newGeneratorConfig()
	var opsCh = make(chan json.RawMessage)
	var ctx = context.Background()
	go generateOps(ctx, cfg, opsCh)

	var writer = bufio.NewWriter(os.Stdout)
	for op := range opsCh {
		_, err := writer.Write(op)
		if err != nil {
			panic(err)
		}
		_, err = writer.WriteRune('\n')
		if err != nil {
			panic(err)
		}
	}
}

// Continuously feeds operation documents into the dest channel. The documents are serialized as
// json without any newline characters or anything at the end.
func generateOps(ctx context.Context, cfg generatorConfig, dest chan<- json.RawMessage) {
	var limiter = rate.NewLimiter(rate.Every(time.Second), cfg.OpsPerSecond)
	var streams = make([]Stream, cfg.Concurrent)

	for s := range streams {
		streams[s] = Stream{
			id:     rand.Intn(1 << 32),
			values: make(map[string]int),
		}
	}

	var counter int
	var update = make(map[string]int)
	for {
		_ = limiter.Wait(ctx)
		// Use a round-robin to select which streams to produce operations on.
		// Our soak tests verify that every stream gets updated periodically, and
		// this ensures that we don't randomly fail to produce any events for a given
		// stream within that period.
		var stream = &streams[counter%len(streams)]
		counter++

		clear(update)
		var keys = rand.Intn(cfg.KeysPerOp) + 1
		for i := 0; i != keys; i++ {
			var key = rand.Intn(len(cfg.Keys))
			update[cfg.Keys[key:key+1]] = 1
		}

		var opType string
		if rand.Intn(2) == 0 {
			opType = "remove"

			for k := range update {
				delete(stream.values, k)
			}
			stream.rem++
		} else {
			opType = "add"

			for k := range update {
				stream.values[k] = stream.values[k] + 1
			}
			stream.add++
		}

		var op = Operation{
			Author:       cfg.Author,
			ID:           stream.id,
			Ones:         bits.OnesCount(uint(stream.id)),
			Op:           stream.opCounter(),
			Type:         opType,
			Values:       update,
			ExpectValues: stream.values,
			Timestamp:    time.Now().UTC().Format(time.RFC3339),
		}
		opJson, err := json.Marshal(op)
		if err != nil {
			panic(err)
		}
		select {
		case <-ctx.Done():
			return
		case dest <- opJson:
		}
	}
}
