package main

import (
	"fmt"

	"github.com/estuary/flow/go/bindings"
)

type cmdBuildIdentity struct{}

func (cmd cmdBuildIdentity) Execute(_ []string) error {
	fmt.Println(bindings.ConnectorBuildIdentity())
	return nil
}
