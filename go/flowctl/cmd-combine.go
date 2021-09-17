package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/estuary/flow/go/bindings"
	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdCombine struct {
	Directory   string                `long:"directory" default:"." description:"Build directory"`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Collection  string                `long:"collection" required:"true" description:"The name of the collection from which to take the schema and key"`
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

	catalog, err := buildCatalog(ctx, pf.BuildAPI_Config{
		CatalogPath: filepath.Join(cmd.Directory, "catalog.db"),
		Directory:   cmd.Directory,
		Source:      cmd.Source,
		SourceType:  pf.ContentType_CATALOG_SPEC,
	})
	if err != nil {
		return err
	}

	var collection *pf.CollectionSpec
	for _, c := range catalog.Collections {
		if c.Collection.String() == cmd.Collection {
			collection = &c
			break
		}
	}
	if collection == nil {
		return fmt.Errorf("The catalog does not define a collection named: %q", cmd.Collection)
	}
	schemaIndex, err := bindings.NewSchemaIndex(&catalog.Schemas)
	if err != nil {
		return fmt.Errorf("building schema bundle: %w", err)
	}

	var combine = bindings.NewCombine()
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
	var inputDocs = 0
	var inputBytes uint64 = 0
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

		if inputDocs%10000 == 0 {
			log.WithFields(log.Fields{
				"inputDocs":  inputDocs,
				"inputBytes": inputBytes,
			}).Info("read input progress")
		}
	}
	if err = combine.PrepareToDrain(); err != nil {
		return fmt.Errorf("preparing to drain: %w", err)
	}

	var outputDocs = 0
	var outputBytes uint64 = 0
	err = combine.Drain(func(full bool, doc json.RawMessage, packedKey []byte, packedFields []byte) error {
		outputDocs++
		outputBytes = outputBytes + uint64(len(doc))
		fmt.Println(string(doc))

		if len(packedKey) <= 1 {
			return fmt.Errorf("Failed to extract key from output docuement: %d", outputDocs)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("draining document %d: %w", outputDocs, err)
	}

	log.WithFields(log.Fields{
		"inputDocs":   inputDocs,
		"inputBytes":  inputBytes,
		"outputDocs":  outputDocs,
		"outputBytes": outputBytes,
	}).Info("completed combine")
	return nil
}
