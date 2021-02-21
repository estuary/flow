package main

import (
	"github.com/estuary/flow/go/runtime"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
)

func main() {
	runconsumer.Main(new(runtime.FlowConsumer))
}
