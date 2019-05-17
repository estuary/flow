// Package counter runs the word_count.Counter consumer.
package main

import (
	"github.com/gazette/gazette/v2/examples/word-count"
	"github.com/gazette/gazette/v2/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(new(word_count.Counter)) }
