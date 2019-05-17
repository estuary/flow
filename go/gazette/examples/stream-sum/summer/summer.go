// Package summer runs the stream_sum.Summer consumer.
package main

import (
	"github.com/gazette/gazette/v2/examples/stream-sum"
	"github.com/gazette/gazette/v2/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(stream_sum.Summer{}) }
