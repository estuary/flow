// Package summer runs the stream_sum.Summer consumer.
package main

import (
	"go.gazette.dev/core/examples/stream-sum"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(stream_sum.Summer{}) }
