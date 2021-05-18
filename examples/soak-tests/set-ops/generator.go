package main

import (
	"bufio"
	"context"
	"encoding/json"
	"math/bits"
	"math/rand"
	"os"
	"time"

	"golang.org/x/time/rate"
)

// Generates operation documents. This is kept as a separate file so that it can be run by itself for
// other manual tests.

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

type cmdGenerate struct {
	Author       int    `long:"author" env:"AUTHOR" default:"0" description:"Unique ID used to identify all sets that are part of this test. Randomly generated if 0"`
	Streams      int    `long:"streams" env:"STREAMS" default:"100" description:"Number of distinct streams of operations to generate"`
	Keys         string `long:"keys" env:"SET_KEYS" default:"abcdefghijklmnopqrstuvwxyz" description:"Characters from which set keys are drawn"`
	KeysPerOp    int    `long:"keys-per-op" env:"KEYS_PER_OP" default:"7" description:"Maximum number of keys in a single set operation"`
	OpsPerSecond int    `long:"ops-per-second" env:"OPS_PER_SECOND" default:"1000" description:"Maximum number of operations per second to generate"`
}

// resolveAuthor randomly generates the Author if the current value is 0
func (cmd *cmdGenerate) resolveAuthor() {
	if cmd.Author == 0 {
		cmd.Author = rand.Intn(1 << 32)
	}
}

func (cmd cmdGenerate) Execute(_ []string) error {
	cmd.resolveAuthor()
	var opsCh = make(chan json.RawMessage)
	var ctx = context.Background()
	go generateOps(ctx, cmd, opsCh)

	var writer = bufio.NewWriter(os.Stdout)
	for op := range opsCh {
		_, err := writer.Write(op)
		if err != nil {
			return err
		}
		_, err = writer.WriteRune('\n')
		if err != nil {
			return err
		}
	}
	return nil
}

func clear(m map[string]int) {
	for k := range m {
		delete(m, k)
	}
}

// Continuously feeds operation documents into the dest channel. The documents are serialized as
// json without any newline characters or anything at the end.
func generateOps(ctx context.Context, cfg cmdGenerate, dest chan<- json.RawMessage) {
	var author = cfg.Author
	var limiter = rate.NewLimiter(rate.Limit(cfg.OpsPerSecond), cfg.OpsPerSecond)
	var streams = make([]Stream, cfg.Streams)

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
			Author:       author,
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
