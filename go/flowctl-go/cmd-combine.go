package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/capture"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/materialize"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdCombine struct {
	Directory   string                `long:"directory" default:"." description:"Build directory"`
	Network     string                `long:"network" default:"host" description:"The Docker network that connector containers are given access to."`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Collection  string                `long:"collection" required:"true" description:"The name of the collection from which to take the schema and key"`
	MaxDocs     uint64                `long:"max-docs" default:"0" description:"Maximum number of documents to add to the combiner before draining it. If 0, then there is no maximum"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd cmdCombine) Execute(_ []string) error {
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

	var config = pf.BuildAPI_Config{
		BuildId:          newBuildID(),
		Directory:        cmd.Directory,
		Source:           cmd.Source,
		SourceType:       catalogSourceType(cmd.Source),
		ConnectorNetwork: cmd.Network,
	}
	// Cleanup output database.
	defer func() { _ = os.Remove(config.OutputPath()) }()

	if err = bindings.BuildCatalog(bindings.BuildArgs{
		Context:             ctx,
		BuildAPI_Config:     config,
		FileRoot:            "/",
		CaptureDriverFn:     capture.NewDriver,
		MaterializeDriverFn: materialize.NewDriver,
	}); err != nil {
		return fmt.Errorf("building catalog: %w", err)
	}

	var collection *pf.CollectionSpec
	var bundle pf.SchemaBundle

	if err = catalog.Extract(config.OutputPath(), func(db *sql.DB) error {
		if collection, err = catalog.LoadCollection(db, cmd.Collection); err != nil {
			return fmt.Errorf("loading collection %s: %w", cmd.Collection, err)
		}
		if bundle, err = catalog.LoadSchemaBundle(db); err != nil {
			return fmt.Errorf("loading schemas: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	schemaIndex, err := bindings.NewSchemaIndex(&bundle)
	if err != nil {
		return fmt.Errorf("building schema index: %w", err)
	}

	combine, err := bindings.NewCombine(ops.StdLogger())
	if err != nil {
		return fmt.Errorf("creating combiner: %w", err)
	}
	combine.Configure(
		"flowctl/combine",
		schemaIndex,
		collection.Collection,
		collection.SchemaUri,
		"",
		collection.KeyPtrs,
		nil,
	)

	type FlowDoc struct {
		Meta struct {
			Ack bool `json:"ack"`
		} `json:"_meta"`
	}

	var scanner = bufio.NewScanner(os.Stdin)
	var inputDocs uint64 = 0
	var inputBytes uint64 = 0
	var outputDocs uint64 = 0
	var outputBytes uint64 = 0
	var drained = true
	for scanner.Scan() {
		var bytes = append([]byte(nil), scanner.Bytes()...)
		inputDocs++
		inputBytes = inputBytes + uint64(len(bytes))

		// Filter out acknowledgements, and also ensure that each input document is valid json.
		var doc FlowDoc
		if err = json.Unmarshal(bytes, &doc); err != nil {
			return fmt.Errorf("invalid json at line %d: %w", inputDocs, err)
		}
		if doc.Meta.Ack {
			continue
		}

		log.WithField("line", string(bytes)).Trace("adding input line")
		if err = combine.CombineRight(json.RawMessage(bytes)); err != nil {
			return fmt.Errorf("at stdin line %d: %w", inputDocs, err)
		}
		drained = false

		if cmd.MaxDocs > 0 && inputDocs%cmd.MaxDocs == 0 {
			log.WithFields(log.Fields{
				"inputDocs":  inputDocs,
				"inputBytes": inputBytes,
			}).Info("draining combiner")
			stats, err := drainToStdout(combine)
			if err != nil {
				return fmt.Errorf("draining combiner: %w", err)
			}
			outputDocs += stats.Out.Docs
			outputBytes += stats.Out.Bytes
			drained = true
		}
	}
	if !drained {
		stats, err := drainToStdout(combine)
		if err != nil {
			return fmt.Errorf("draining combiner: %w", err)
		}
		outputDocs += stats.Out.Docs
		outputBytes += stats.Out.Bytes
	}

	log.WithFields(log.Fields{
		"inputDocs":   inputDocs,
		"inputBytes":  inputBytes,
		"outputDocs":  outputDocs,
		"outputBytes": outputBytes,
	}).Info("completed combine")
	return nil
}

func drainToStdout(combiner *bindings.Combine) (*pf.CombineAPI_Stats, error) {
	return combiner.Drain(func(full bool, doc json.RawMessage, packedKey []byte, packedFields []byte) error {
		fmt.Println(string(doc))
		return nil
	})
}

func catalogSourceType(source string) pf.ContentType {
	if strings.HasSuffix(source, ".json") {
		return pf.ContentType_CATALOG_JSON
	}
	return pf.ContentType_CATALOG_YAML
}
