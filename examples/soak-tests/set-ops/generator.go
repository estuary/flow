package main

import (
	"bufio"
	"encoding/json"
	"math/rand"
	"os"
	"time"
)

const (
	// NConcurrent is the number of concurrent streams.
	NConcurrent = 2
	// NStreamOps is the number of operations per stream.
	NStreamOps = 2
	// Keys is the set of characters from which set keys are drawn.
	Keys = string("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	// MaxKeysPerOp is the maximum number of keys appearing in a set operation.
	MaxKeysPerOp = len(Keys) / 3
)

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
	var author = rand.Intn(1 << 16)

	var bw = bufio.NewWriter(os.Stdout)
	var enc = json.NewEncoder(bw)
	var err error

	var streams [NConcurrent]Stream

	var nextID int
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
		var stream = &streams[rand.Intn(NConcurrent)]

		if op := stream.add + stream.rem; op == NStreamOps {
			if err = enc.Encode(struct {
				Author      int
				ID          int
				Op          int
				Type        string
				TotalAdd    int
				TotalRemove int
				Values      map[string]int
			}{author, stream.id, op + 1, "verify", stream.add, stream.rem, stream.values}); err != nil {
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
		var keys = rand.Intn(MaxKeysPerOp) + 1
		for i := 0; i != keys; i++ {
			var key = rand.Intn(len(Keys))
			update[Keys[key:key+1]] = 1
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
			Op     int
			Type   string
			Values map[string]int
		}{author, stream.id, stream.add + stream.rem, opType, update}); err != nil {
			panic(err)
		}
	}
}
