// Package counter runs the word_count.Counter consumer.
package main

import (
	"github.com/LiveRamp/gazette/v2/examples/word-count"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(new(word_count.Counter)) }
