package cmd

// When shard_compose finds the same key in multiple different input sources, it has to select a single value
// to take the place of the n incoming values. Implement this interface to choose a different behavior for how
// multiple keys will be merged.
type Merge func(key []byte, values [][]byte) []byte

func First(key []byte, values [][]byte) []byte {
	return values[0]
}
