package main

import (
	"go.gazette.dev/core/examples/bike-share"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(new(bike_share.Application)) }
