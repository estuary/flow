package cmd

import log "github.com/sirupsen/logrus"

// When shard_compose finds the same key in multiple different input sources, it has to select a single value
// to take the place of the n incoming values. Implement this interface to choose a different behavior for how
// multiple keys will be merged.
// Once you have defined your desired merging behavior as a function, you must add it to the list of options
// by calling registerMerge(NAME, MERGE_FUNCTION). Then you can use it when running shard compose by passing NAME
// as the merger function.
type Merge func(key []byte, values [][]byte) []byte

var MergeFunctions = make(map[string]Merge)

func registerMerge(name string, mergeFunction Merge) {
	if _, hasKey := MergeFunctions[name]; !hasKey {
		MergeFunctions[name] = mergeFunction
	} else {
		log.WithField("name", name).Warn("Merge function defined twice, execution will choose one arbitrarily")
	}
}

func First(key []byte, values [][]byte) []byte {
	return values[0]
}

func init() {
	registerMerge("first", First)
}
