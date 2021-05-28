package tester

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/materialize/driver"
	"github.com/estuary/flow/go/materialize/lifecycle"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/nsf/jsondiff"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

// Fixture encapsulates the data that's needed to test materializations, along with common
// functionality for executing tests.
type Fixture struct {
	ShardId string
	Ctx     context.Context
	Driver  pm.DriverClient

	materialization *pf.MaterializationSpec
	keyExtractor    *bindings.Extractor
	generator       *generator
	deltaUpdates    bool
}

// NewFixture creates a new fixture for a given endpoint and endpoint configuration.
func NewFixture(endpointType pf.EndpointType, endpointConfig string) (*Fixture, error) {
	var ctx = context.Background()
	var spec = NewMaterialization(endpointType, endpointConfig)
	driverClient, err := driver.NewDriver(ctx, endpointType, json.RawMessage(endpointConfig), "")
	if err != nil {
		return nil, fmt.Errorf("creating driver client: %w", err)
	}
	var extractor = bindings.NewExtractor()

	if err = extractor.Configure(spec.Shuffle.SourceUuidPtr, spec.Collection.KeyPtrs, "", nil); err != nil {
		return nil, fmt.Errorf("creating extractor: %w", err)
	}
	docGenerator, err := newGenerator(spec)
	if err != nil {
		return nil, fmt.Errorf("creating test document generator: %w", err)
	}
	return &Fixture{
		ShardId:         "materialization-test",
		Ctx:             ctx,
		Driver:          driverClient,
		materialization: spec,
		keyExtractor:    extractor,
		generator:       docGenerator,
	}, nil
}

func (f *Fixture) OpenTransactions(req **pm.TransactionRequest, driverCheckpoint []byte) (pm.Driver_TransactionsClient, *pm.TransactionResponse_Opened, error) {
	var stream, err = f.Driver.Transactions(f.Ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("starting transactions rpc: %w", err)
	}
	err = lifecycle.WriteOpen(stream, req, f.materialization, f.ShardId, driverCheckpoint)
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
	f.deltaUpdates = resp.Opened.DeltaUpdates
	return stream, resp.Opened, nil
}

func (f *Fixture) LoadDocuments(stream pm.Driver_TransactionsClient, req **pm.TransactionRequest, flowCheckpoint int64, toLoad []*document) (*pm.TransactionResponse_Prepared, error) {
	var expected = make(map[string]*document)
	if !f.deltaUpdates {
		for _, doc := range toLoad {
			var key = doc.key.Pack()
			var err = lifecycle.StageLoad(stream, req, key)
			if err != nil {
				return nil, err
			}
			if doc.exists {
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
				f.keyExtractor.Document(loadedBytes)
			}
			_, loadedKeys, err := f.keyExtractor.Extract()
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

func (f *Fixture) StoreDocuments(stream pm.Driver_TransactionsClient, req **pm.TransactionRequest, toStore []*document) error {
	for _, doc := range toStore {
		var err = lifecycle.StageStore(stream, req, doc.key.Pack(), doc.values.Pack(), doc.docJson(), doc.exists)
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
		doc.exists = true
	}
	return nil
}

func (f *Fixture) Validate() (*pm.ValidateResponse, error) {
	var validateRequest = pm.ValidateRequest{
		EndpointType:     f.materialization.EndpointType,
		EndpointSpecJson: f.materialization.EndpointSpecJson,
		Collection:       &f.materialization.Collection,
	}
	return f.Driver.Validate(f.Ctx, &validateRequest)
}

func (f *Fixture) Apply(dryRun bool) (*pm.ApplyResponse, error) {
	var applyRequest = pm.ApplyRequest{
		Materialization: f.materialization,
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

// NewMaterialization returns a MaterializationSpec for use by tests. This is a hard coded
// materialization that includes a field of each type.
func NewMaterialization(endpointType pf.EndpointType, endpointSpecJSON string) *pf.MaterializationSpec {
	var inf = func(mustExist bool, types ...string) pf.Inference {
		return pf.Inference{
			Types:     types,
			MustExist: mustExist,
		}
	}
	var proj = func(prt, field string, isKey bool, inference pf.Inference) pf.Projection {
		return pf.Projection{
			Ptr:          "/" + field,
			Field:        field,
			IsPrimaryKey: isKey,
			Inference:    inference,
		}
	}

	var valueProj = func(ty string) pf.Projection {
		return proj("/"+ty, ty, false, inf(false, ty))
	}

	return &pf.MaterializationSpec{
		Collection: pf.CollectionSpec{
			Collection: "materialization/test",
			SchemaUri:  "http://test.test/schema.json",
			KeyPtrs:    []string{"/key1", "/key2"},
			UuidPtr:    "/_meta/uuid",
			Projections: []pf.Projection{
				proj("/key1", "key1", true, inf(true, "integer")),
				proj("/key2", "key2", true, inf(true, "string")),
				proj("", "flow_document", false, inf(true, "object")),
				valueProj("string"),
				valueProj("integer"),
				valueProj("number"),
				valueProj("boolean"),
				valueProj("array"),
				valueProj("object"),
			},
		},
		FieldSelection: pf.FieldSelection{
			Keys:     []string{"key1", "key2"},
			Values:   []string{"boolean", "integer", "number", "string"},
			Document: "flow_document",
		},
		Shuffle: pf.Shuffle{
			GroupName:        "materialize/materialization/test/driver_test",
			SourceCollection: "materialization/test",
			SourcePartitions: pb.LabelSelector{
				Include: pb.LabelSet{
					Labels: []pb.Label{
						{Name: "estuary.dev/collection", Value: "materialization/test"},
					},
				},
			},
			SourceUuidPtr:    "/_meta/uuid",
			ShuffleKeyPtr:    []string{"/key1", "/key2"},
			UsesSourceKey:    true,
			SourceSchemaUri:  "http://test.test/schema.json",
			UsesSourceSchema: true,
		},
		EndpointType:     endpointType,
		EndpointSpecJson: endpointSpecJSON,
	}
}

var jsonDiffOptions = jsondiff.DefaultJSONOptions()
