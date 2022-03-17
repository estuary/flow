package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/capture"
	"github.com/estuary/flow/go/materialize"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiBuild struct {
	BuildID      string                `long:"build-id" required:"true" description:"ID of this build"`
	Directory    string                `long:"directory" default:"." description:"Build directory"`
	FileRoot     string                `long:"fs-root" default:"/" description:"Filesystem root of fetched file:// resources"`
	Network      string                `long:"network" default:"host" description:"The Docker network that connector containers are given access to."`
	Source       string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	SourceType   string                `long:"source-type" default:"catalog" choice:"catalog" choice:"jsonSchema" description:"Type of the source to build."`
	SourceFormat string                `long:"source-format" default:"yaml" choice:"yaml" choice:"json" description:"Format of the source."`
	TSCompile    bool                  `long:"ts-compile" description:"Should TypeScript modules be compiled and linted? Implies generation."`
	TSGenerate   bool                  `long:"ts-generate" description:"Should TypeScript types be generated?"`
	TSPackage    bool                  `long:"ts-package" description:"Should TypeScript modules be packaged? Implies generation and compilation."`
	Log          mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics  mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiBuild) execute(ctx context.Context) error {
	var err error
	if cmd.Directory, err = filepath.Abs(cmd.Directory); err != nil {
		return fmt.Errorf("filepath.Abs: %w", err)
	}

	var sourceType pf.ContentType
	switch cmd.SourceType {
	case "catalog":
		switch cmd.SourceFormat {
		case "json":
			sourceType = pf.ContentType_CATALOG_JSON
		case "yaml":
			sourceType = pf.ContentType_CATALOG_YAML
		}
	case "jsonSchema":
		switch cmd.SourceFormat {
		case "json":
			sourceType = pf.ContentType_JSON_SCHEMA_JSON
		case "yaml":
			sourceType = pf.ContentType_JSON_SCHEMA_YAML
		}
	}

	var args = bindings.BuildArgs{
		Context: ctx,
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:          cmd.BuildID,
			Directory:        cmd.Directory,
			Source:           cmd.Source,
			SourceType:       sourceType,
			ConnectorNetwork: cmd.Network,

			TypescriptGenerate: cmd.TSGenerate,
			TypescriptCompile:  cmd.TSCompile,
			TypescriptPackage:  cmd.TSPackage,
		},
		FileRoot:            cmd.FileRoot,
		CaptureDriverFn:     capture.NewDriver,
		MaterializeDriverFn: materialize.NewDriver,
	}
	if err := bindings.BuildCatalog(args); err != nil {
		return err
	}

	// We manually open the database, rather than use catalog.Extract,
	// because we explicitly check for and handle errors.
	// Essentially all other accesses of the catalog DB should prefer catalog.Extract.
	db, err := sql.Open("sqlite3", fmt.Sprintf("file://%s?mode=ro", args.OutputPath()))
	if err != nil {
		return fmt.Errorf("opening DB: %w", err)
	}
	defer db.Close()

	errors, err := catalog.LoadAllErrors(db)
	if err != nil {
		return fmt.Errorf("loading catalog errors: %w", err)
	}

	for _, be := range errors {
		var path, ptr = scopeToPathAndPtr(args.Directory, be.Scope)
		fmt.Println(yellow(path), "error at", red(ptr), ":")
		fmt.Println(be.Error)
	}
	if len(errors) != 0 {
		return fmt.Errorf("%d build errors", len(errors))
	}

	return nil
}

func (cmd apiBuild) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")

	return cmd.execute(context.Background())
}

func scopeToPathAndPtr(dir, scope string) (path, ptr string) {
	u, err := url.Parse(scope)
	if err != nil {
		panic(err)
	}

	ptr, u.Fragment = u.Fragment, ""
	path = u.String()

	if u.Scheme == "file" && strings.HasPrefix(u.Path, dir) {
		path = path[len(dir)+len("file://")+1:]
	}
	if ptr == "" {
		ptr = "<root>"
	}
	return path, ptr
}

var green = color.New(color.FgGreen).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()
var red = color.New(color.FgRed).SprintFunc()
