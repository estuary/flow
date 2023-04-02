package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiBuild struct {
	BuildID     string                `long:"build-id" required:"true" description:"ID of this build"`
	BuildDB     string                `long:"build-db" required:"true" description:"Output build database"`
	FileRoot    string                `long:"fs-root" default:"/" description:"Filesystem root of fetched file:// resources"`
	Network     string                `long:"network" description:"The Docker network that connector containers are given access to."`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	SourceType  string                `long:"source-type" default:"catalog" choice:"catalog" choice:"jsonSchema" description:"Type of the source to build."`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiBuild) execute(ctx context.Context) error {
	var sourceType pf.ContentType
	switch cmd.SourceType {
	case "catalog":
		sourceType = pf.ContentType_CATALOG
	case "jsonSchema":
		sourceType = pf.ContentType_JSON_SCHEMA
	}

	var args = bindings.BuildArgs{
		Context: ctx,
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:          cmd.BuildID,
			BuildDb:          cmd.BuildDB,
			Source:           cmd.Source,
			SourceType:       sourceType,
			ConnectorNetwork: cmd.Network,
		},
		FileRoot: cmd.FileRoot,
	}
	if err := bindings.BuildCatalog(args); err != nil {
		return err
	}

	// We manually open the database, rather than use catalog.Extract,
	// because we explicitly check for and handle errors.
	// Essentially all other accesses of the catalog DB should prefer catalog.Extract.
	var db, err = sql.Open("sqlite3", fmt.Sprintf("file://%s?mode=ro", args.BuildDb))
	if err != nil {
		return fmt.Errorf("opening DB: %w", err)
	}
	defer db.Close()

	errors, err := catalog.LoadAllErrors(db)
	if err != nil {
		return fmt.Errorf("loading catalog errors: %w", err)
	}

	for _, be := range errors {
		var path, ptr = scopeToPathAndPtr(args.Source, be.Scope)
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
	var ctx, cancelFn = context.WithTimeout(context.Background(), executeTimeout)
	defer cancelFn()

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	return cmd.execute(ctx)
}

func scopeToPathAndPtr(source, scope string) (path, ptr string) {
	sourceURL, err := url.Parse(source)
	if err != nil {
		panic(err)
	}

	// If `source` is relative, attempt to resolve it as an absolute path to a local file.
	if !sourceURL.IsAbs() {
		if abs, err := filepath.Abs(source); err == nil {
			sourceURL.Scheme = "file"
			sourceURL.Path = abs
		}
	}

	scopeURL, err := url.Parse(scope)
	if err != nil {
		panic(err)
	}

	if sourceURL.Scheme == "file" && scopeURL.Scheme == "file" {
		if rel, err := filepath.Rel(filepath.Dir(sourceURL.Path), scopeURL.Path); err == nil {
			return rel, scopeURL.Fragment
		}
	}

	ptr, scopeURL.Fragment = scopeURL.Fragment, ""
	path = scopeURL.String()

	if ptr == "" {
		ptr = "<root>"
	}
	return path, ptr
}

var green = color.New(color.FgGreen).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()
var red = color.New(color.FgRed).SprintFunc()

const executeTimeout = time.Minute * 5
