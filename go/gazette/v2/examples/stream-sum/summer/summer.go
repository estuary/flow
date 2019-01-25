// Package summer runs the stream_sum.Summer consumer.
package main

import (
	"github.com/LiveRamp/gazette/v2/examples/stream-sum"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(stream_sum.Summer{}) }
