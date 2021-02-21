package main

import (
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdBuild struct {
	Source    string `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Directory string `long:"directory" default:"." description:"Build directory"`
}

func (cmd cmdBuild) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(Config.Diagnostics)()
	mbp.InitLog(Config.Log)

	log.WithFields(log.Fields{
		"config":    Config,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")

	var config = pf.BuildAPI_Config{
		Source:            cmd.Source,
		Directory:         cmd.Directory,
		CatalogPath:       filepath.Join(cmd.Directory, "catalog.db"),
		TypescriptCompile: true,
		TypescriptPackage: true,
	}

	var _, err = build(config)
	return err
}

func build(config pf.BuildAPI_Config) (*flow.Catalog, error) {
	var transport = new(http.Transport)
	*transport = *http.DefaultTransport.(*http.Transport) // Clone.
	transport.RegisterProtocol("file", http.NewFileTransport(http.Dir("/")))
	var httpClient = &http.Client{Transport: transport}

	if _, err := bindings.BuildCatalog(config, httpClient); err != nil {
		return nil, fmt.Errorf("building catalog: %w", err)
	}
	catalog, err := flow.NewCatalog(config.CatalogPath, "")
	if err != nil {
		return nil, fmt.Errorf("opening built catalog: %w", err)
	}

	// If there were build errors, present them and bail out.
	buildErrors, err := catalog.LoadBuildErrors()
	if err != nil {
		return nil, fmt.Errorf("loading build errors: %w", err)
	}
	for _, be := range buildErrors {
		log.WithField("scope", be.Scope).Error(be.Error)
	}

	if len(buildErrors) != 0 {
		return nil, fmt.Errorf("one or more catalog errors")
	}
	return catalog, nil
}
