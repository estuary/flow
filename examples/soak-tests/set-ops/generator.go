package main

import (
	"bufio"
	"encoding/json"
	"math/bits"
	"math/rand"
	"os"
	"time"

	"github.com/jessevdk/go-flags"
)

var cfg struct {
	Concurrent   int    `long:"concurrent" default:"100" description:"Number of concurrent streams"`
	OpsPerStream int    `long:"operations" default:"10" description:"Number of operations per stream"`
	Keys         string `long:"keys" default:"ABCDEFGHIJKLMNOPQRSTUVWXYZ" description:"Characters from which set keys are drawn"`
	KeysPerOp    int    `long:"keys-per-op" default:"7" description:"Maximum number of keys in a single set operation"`
}

// Stream holds incremental stream state.
type Stream struct {
	id, add, rem int
	values       map[string]int
}

func clear(m map[string]int) {
	for k := range m {
		delete(m, k)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	if _, err := flags.Parse(&cfg); err != nil {
		return
	}

	var (
		err     error
		author  = rand.Intn(1 << 16)
		bw      = bufio.NewWriter(os.Stdout)
		enc     = json.NewEncoder(bw)
		streams = make([]Stream, cfg.Concurrent)
		nextID  int
	)

	for s := range streams {
		streams[s] = Stream{
			id:     nextID,
			add:    0,
			rem:    0,
			values: make(map[string]int),
		}
		nextID++
	}

	var update = make(map[string]int)
	for {
		var stream = &streams[rand.Intn(cfg.Concurrent)]

		if op := stream.add + stream.rem; op == cfg.OpsPerStream {
			if err = enc.Encode(struct {
				Author      int
				ID          int
				Ones        int
				Op          int
				Type        string
				TotalAdd    int
				TotalRemove int
				Values      map[string]int
			}{author, stream.id, bits.OnesCount(uint(stream.id)), op + 1, "verify", stream.add, stream.rem, stream.values}); err != nil {
				panic(err)
			}

			*stream = Stream{
				id:     nextID,
				values: stream.values,
			}
			nextID++
			clear(stream.values)
		}

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

		if err = enc.Encode(struct {
			Author int
			ID     int
			Ones   int
			Op     int
			Type   string
			Values map[string]int
		}{author, stream.id, bits.OnesCount(uint(stream.id)), stream.add + stream.rem, opType, update}); err != nil {
			panic(err)
		}
	}
}
