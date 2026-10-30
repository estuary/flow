// Package counter runs the word_count.Counter consumer.
package main

import (
	"go.gazette.dev/core/examples/word-count"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(new(word_count.Counter)) }
