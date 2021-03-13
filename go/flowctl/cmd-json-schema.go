package main

import (
	"os"

	"github.com/estuary/flow/go/bindings"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdJSONSchema struct{}

func (cmdJSONSchema) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(Config.Diagnostics)()
	initLog(Config.Log)

	var _, err = os.Stdout.WriteString(bindings.CatalogJSONSchema())
	return err
}
