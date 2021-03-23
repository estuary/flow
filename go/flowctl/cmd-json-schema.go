package main

import (
	"os"

	"github.com/estuary/flow/go/bindings"
)

type cmdJSONSchema struct{}

func (cmdJSONSchema) Execute(_ []string) error {
	var _, err = os.Stdout.WriteString(bindings.CatalogJSONSchema())
	return err
}
