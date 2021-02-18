package main

import (
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

	var config = pf.BuildAPI_Config{
		Source:            cmd.Source,
		Directory:         cmd.Directory,
		CatalogPath:       filepath.Join(cmd.Directory, "catalog.db"),
		TypescriptCompile: true,
		TypescriptPackage: true,
	}
	var _ = build(config)

	return nil
}

func build(config pf.BuildAPI_Config) *flow.Catalog {
	var transport = new(http.Transport)
	*transport = *http.DefaultTransport.(*http.Transport) // Clone.
	transport.RegisterProtocol("file", http.NewFileTransport(http.Dir("/")))
	var httpClient = &http.Client{Transport: transport}

	var _, err = bindings.BuildCatalog(config, httpClient)
	mbp.Must(err, "failed to build catalog")
	catalog, err := flow.NewCatalog(config.CatalogPath, "")
	mbp.Must(err, "failed to open catalog")

	// If there were build errors, present them and bail out.
	buildErrors, err := catalog.LoadBuildErrors()
	mbp.Must(err, "failed to load build errors")
	for _, be := range buildErrors {
		log.WithField("scope", be.Scope).Error(be.Error)
	}

	if len(buildErrors) != 0 {
		log.Fatal("catalog build failed")
	}
	return catalog
}
