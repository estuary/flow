// +build !norocksdb

// Package counter is a consumer plugin (eg, should be built with
// `go build --buildmode=plugin`).
package main

import (
	"github.com/LiveRamp/gazette/v2/cmd/run-consumer/consumermodule"
	"github.com/LiveRamp/gazette/v2/examples/word-count"
)

func main() {} // Not called.
var Module consumermodule.Module = word_count.Counter{}
