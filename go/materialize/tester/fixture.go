package tester

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/materialize/driver"
	"github.com/estuary/flow/go/materialize/lifecycle"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/nsf/jsondiff"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

type Fixture struct {
	ShardId         string
	Ctx             context.Context
	Driver          pm.DriverClient
	Materialization *pf.MaterializationSpec
	KeyExtractor    *bindings.Extractor

	DocGenerator *testDocGenerator
	DeltaUpdates bool
}

func NewFixture(endpointType pf.EndpointType, endpointConfig string) (*Fixture, error) {
	var ctx = context.Background()
	var spec, err = NewTestMaterialization(endpointType, endpointConfig)
	if err != nil {
		return nil, err
	}
	driverClient, err := driver.NewDriver(ctx, endpointType, json.RawMessage(endpointConfig))
	if err != nil {
		return nil, fmt.Errorf("creating driver client: %w", err)
	}
	extractor, err := bindings.NewExtractor(spec.Shuffle.SourceUuidPtr, spec.Collection.KeyPtrs)
	if err != nil {
		return nil, fmt.Errorf("creating extractor: %w", err)
	}
	docGenerator, err := newTestDocGenerator(spec)
	if err != nil {
		return nil, fmt.Errorf("creating test document generator: %w", err)
	}
	return &Fixture{
		ShardId:         "materialization-test",
		Ctx:             ctx,
		Driver:          driverClient,
		Materialization: spec,
		KeyExtractor:    extractor,
		DocGenerator:    docGenerator,
	}, nil
}

func (f *Fixture) OpenTransactions(req **pm.TransactionRequest, driverCheckpoint []byte) (pm.Driver_TransactionsClient, *pm.TransactionResponse_Opened, error) {
	var stream, err = f.Driver.Transactions(f.Ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("starting transactions rpc: %w", err)
	}
	err = lifecycle.WriteOpen(stream, req, f.Materialization.EndpointType, f.Materialization.EndpointConfig, f.Materialization.FieldSelection, f.ShardId, driverCheckpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("opening transactions: %w", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, nil, fmt.Errorf("reading Opened message: %w", err)
	}
	if resp.Opened == nil {
		return nil, nil, fmt.Errorf("Expected Opened message, got: %+v", resp)
	}
	f.DeltaUpdates = resp.Opened.DeltaUpdates
	return stream, resp.Opened, nil
}

func (f *Fixture) LoadDocuments(stream pm.Driver_TransactionsClient, req **pm.TransactionRequest, flowCheckpoint int64, toLoad []*TestDoc) (*pm.TransactionResponse_Prepared, error) {
	var expected = make(map[string]*TestDoc)
	if !f.DeltaUpdates {
		for _, doc := range toLoad {
			var key = doc.Key.Pack()
			var err = lifecycle.StageLoad(stream, req, key)
			if err != nil {
				return nil, err
			}
			if doc.Exists {
				expected[string(key)] = doc
			}
		}
	}
	var err = lifecycle.WritePrepare(stream, req, newCheckpoint(flowCheckpoint))
	if err != nil {
		return nil, fmt.Errorf("writing prepare: %w", err)
	}

	var response *pm.TransactionResponse
	for {
		response, err = stream.Recv()
		if err != nil {
			return nil, fmt.Errorf("receiving load response: %w", err)
		}
		if response.Loaded != nil {
			for _, slice := range response.Loaded.DocsJson {
				var loadedBytes = response.Loaded.Arena.Bytes(slice)
				f.KeyExtractor.Document(loadedBytes)
			}
			_, loadedKeys, err := f.KeyExtractor.Extract()
			if err != nil {
				return nil, fmt.Errorf("extracting keys of loaded documents: %w", err)
			}
			for i, loadedKey := range loadedKeys {
				var expectedDoc = expected[string(loadedKey)]
				if expectedDoc == nil {
					return nil, fmt.Errorf("Unexpected document with key: '%s', expected: %v", string(loadedKey), expected)
				}
				delete(expected, string(loadedKey))
				var actualDoc = response.Loaded.Arena.Bytes(response.Loaded.DocsJson[i])
				// Assert that the actual and expected docs are the same
				// TODO: account for float epsilons here once this is rebased on master
				jsondiff.Compare(expectedDoc.docJson(), actualDoc, &jsonDiffOptions)
			}
		} else if response.Prepared != nil {
			break
		} else {
			return nil, fmt.Errorf("Expected a Loaded or Prepared message, got: %+v", *response)
		}
	}
	if len(expected) > 0 {
		return nil, fmt.Errorf("Load responses missing expected documents: %v", expected)
	}
	return response.Prepared, nil
}

func (f *Fixture) StoreDocuments(stream pm.Driver_TransactionsClient, req **pm.TransactionRequest, toStore []*TestDoc) error {
	for _, doc := range toStore {
		var err = lifecycle.StageStore(stream, req, doc.Key.Pack(), doc.Values.Pack(), doc.docJson(), doc.Exists)
		if err != nil {
			return fmt.Errorf("staging store: %w", err)
		}
	}
	var err = lifecycle.WriteCommit(stream, req)
	if err != nil {
		return fmt.Errorf("writing commit: %w", err)
	}
	response, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receiving commit response: %w", err)
	}
	if response.Committed == nil {
		return fmt.Errorf("Expected Committed, got: %+v", response)
	}
	// Now that we've confirmed that the documents have been stored, set Exists to true so we handle
	// future Loads and Stores correctly.
	for _, doc := range toStore {
		doc.Exists = true
	}
	return nil
}

func (f *Fixture) Validate() (*pm.ValidateResponse, error) {
	var validateRequest = pm.ValidateRequest{
		EndpointType:       f.Materialization.EndpointType,
		EndpointConfigJson: f.Materialization.EndpointConfig,
		Collection:         f.Materialization.Collection,
	}
	return f.Driver.Validate(f.Ctx, &validateRequest)
}

func (f *Fixture) Apply(dryRun bool) (*pm.ApplyResponse, error) {
	var applyRequest = pm.ApplyRequest{
		Materialization: f.Materialization,
		DryRun:          dryRun,
	}
	return f.Driver.Apply(f.Ctx, &applyRequest)
}

func flowCheckpointBytes(id int64) []byte {
	var cp = newCheckpoint(id)
	var bytes, err = (&cp).Marshal()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal test checkpoint: %v", err))
	}
	return bytes
}
func newCheckpoint(id int64) pc.Checkpoint {
	var args = pc.BuildCheckpointArgs{
		ReadThrough: pb.Offsets{
			pb.Journal("mockJournal"): id,
		},
	}
	return pc.BuildCheckpoint(args)
}

// NewTestMaterialization returns a MaterializationSpec for use by tests. The collection spec and
// field selection are taken from `materialization-test-flow.yaml`, which uses a sqlite endpoint
// during builds. The endpoint type and configuration json will be replaced by those provided. Note
// that this function may require modification if we want to re-use it in a binary for testing
// arbitrary remote drivers because it currently expects the built catalog to exist at runtime. A
// better approach might be to have the tester binary require `--source` and `--materialization`
// flags, and go through the normal build process as part of testing the materialization.
func NewTestMaterialization(endpointType pf.EndpointType, endpointConfigJson string) (*pf.MaterializationSpec, error) {
	// TODO: consider factoring out a flow.OpenTestCatalog function that does this.
	var catPath, _ = os.LookupEnv("FLOW_TEST_CATALOG")
	if catPath == "" {
		panic("Expected FLOW_TEST_CATALOG env variable with path to catalog")
	}
	var cat, err = flow.NewCatalog(catPath, "")
	if err != nil {
		return nil, fmt.Errorf("reading catalog: %w", err)
	}

	materialization, err := cat.LoadMaterialization("materialization/test/sqlite")
	if err != nil {
		return nil, fmt.Errorf("loading materialization: %w", err)
	}
	materialization.EndpointType = endpointType
	materialization.EndpointConfig = endpointConfigJson
	return materialization, nil
}

var jsonDiffOptions = jsondiff.DefaultJSONOptions()
