package main

import (
	"context"
	"fmt"
	"path/filepath"

	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdCheck struct {
	Directory   string                `long:"directory" default:"." description:"Build directory"`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd cmdCheck) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	var err error
	if cmd.Directory, err = filepath.Abs(cmd.Directory); err != nil {
		return fmt.Errorf("filepath.Abs: %w", err)
	}
	var ctx = context.Background()

	_, err = buildCatalog(ctx, pf.BuildAPI_Config{
		CatalogPath: filepath.Join(cmd.Directory, "catalog.db"),
		Directory:   cmd.Directory,
		Source:      cmd.Source,
		SourceType:  pf.ContentType_CATALOG_SPEC,

		// Check doesn't compile or package TypeScript modules.
		TypescriptGenerate: true,
	})
	if err != nil {
		return err
	}

	return nil
}
