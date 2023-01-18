package bindings

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

func TestDeriveWithIntStrings(t *testing.T) {
	var args = BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			Directory:  t.TempDir(),
			Source:     "file:///int-strings.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, BuildCatalog(args))

	var derivation *pf.DerivationSpec

	require.NoError(t, catalog.Extract(args.OutputPath(), func(db *sql.DB) (err error) {
		if derivation, err = catalog.LoadDerivation(db, "int-strings"); err != nil {
			return fmt.Errorf("loading collection: %w", err)
		}
		return nil
	}))

	var lambdaClient, stop = NewTestLambdaServer(t, map[string]TestLambdaHandler{
		"/derive/int-strings/appendStrings/Publish": func(source, _, _ json.RawMessage) ([]interface{}, error) {
			var m struct {
				I int
				S string
			}
			if err := json.Unmarshal(source, &m); err != nil {
				return nil, err
			}
			return []interface{}{
				struct {
					I int      `json:"i"`
					S []string `json:"s"`
				}{m.I, []string{m.S}},
			}, nil
		},
	})
	defer stop()

	// Tweak fixture so that the derive API produces partition fields.
	// These aren't actually valid partitions, as they're not required to exist.
	for _, field := range []string{"part_a", "part_b"} {
		derivation.Collection.GetProjection(field).IsPartitionKey = true
	}

	derive, err := NewDerive(nil, t.TempDir(), localPublisher)
	require.NoError(t, err)

	// Loop to exercise multiple transactions.
	for i := 0; i != 5; i++ {
		// Even transactions start with a re-configuration.
		// Odd ones re-use the previous configuration.
		if i%2 == 0 {
			derive.Configure("test/derive/withIntStrings", derivation, lambdaClient)
		}

		// Expect we can restore the last checkpoint in between transactions.
		_, err = derive.RestoreCheckpoint()
		require.NoError(t, err)
		// Expect we can clear registers in between transactions.
		require.NoError(t, derive.ClearRegisters())

		var fixtures = []struct {
			key int
			doc string
		}{
			{32, `{"i":32, "s":"one"}`},
			{42, `{"i":42, "s":"two"}`},
			{42, `{"i":42, "s":"three"}`},
			{32, `{"i":32, "s":"four"}`},
		}

		derive.BeginTxn()
		for _, fixture := range fixtures {
			require.NoError(t, derive.Add(
				pf.UUIDParts{ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
				tuple.Tuple{fixture.key}.Pack(),
				0,
				json.RawMessage(fixture.doc),
			))

			// For half of our loops, add extra ACK transactions to coerce
			// the service to process our fixture using multiple blocks.
			if i%2 == 1 {
				require.NoError(t, derive.Add(
					pf.UUIDParts{ProducerAndFlags: uint64(message.Flag_ACK_TXN)},
					tuple.Tuple{nil}.Pack(),
					0,
					json.RawMessage(fixture.doc),
				))
			}
		}
		// Drain transaction, and look for expected roll-ups.
		expectDeriveFixture(t, derive.Drain)

		require.NoError(t, derive.PrepareCommit(protocol.Checkpoint{}))
	}

	// Safe to call Destroy multiple times.
	derive.Destroy()
	derive.Destroy()
}

func expectDeriveFixture(t *testing.T, finish func(CombineCallback) (*pf.DeriveAPI_Stats, error)) {
	var stats, err = finish(expectCombineCallback(t))
	require.NoError(t, err)
	t.Log(stats)
	require.Equal(t, 1, len(stats.Transforms))
	// The expected inputs and outputs of the transform will vary depending on which iteration of
	// the loop we're in, so we just ensure that these are non-zero. The stats assertions here are
	// more to ensure that we're actually reading the stats from the output, rather than asserting
	// the correctness of the numbers themselves. Correctness of the numbers is instead tested on
	// the rust side.
	require.Greater(t, stats.Transforms[0].Input.Docs, uint32(0))
	require.Greater(t, stats.Transforms[0].Input.Bytes, uint32(0))
	require.Greater(t, stats.Transforms[0].Publish.Output.Docs, uint32(0))
	require.Greater(t, stats.Transforms[0].Publish.Output.Bytes, uint32(0))

	// Extra sanity check for the output, since these should be the same for all iterations.
	require.Equal(t, uint32(2), stats.Output.Docs)
	require.Equal(t, uint32(167), stats.Output.Bytes)
	require.Equal(t, uint32(0), stats.Registers.Created)
	require.Equal(t, uint32(0), stats.Transforms[0].Update.Output.Docs)
}

func TestDeriveWithIncResetPublish(t *testing.T) {
	var args = BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			Directory:  t.TempDir(),
			Source:     "file:///inc-reset-publish.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, BuildCatalog(args))

	var derivation *pf.DerivationSpec

	require.NoError(t, catalog.Extract(args.OutputPath(), func(db *sql.DB) (err error) {
		if derivation, err = catalog.LoadDerivation(db, "derivation"); err != nil {
			return fmt.Errorf("loading collection: %w", err)
		}
		return nil
	}))

	type sourceDoc struct {
		Key     string `json:"key"`
		Reset   int    `json:"reset"`
		Invalid string `json:"invalid-property,omitempty"`
	}
	type regDoc struct {
		Type  string `json:"type"`
		Value int    `json:"value"`
	}
	type derivedDoc struct {
		Key     string `json:"key"`
		Reset   int    `json:"reset"`
		Values  []int  `json:"values"`
		Invalid string `json:"invalid-property,omitempty"`
	}

	var handlers = map[string]TestLambdaHandler{
		"/derive/derivation/increment/Update": func(_, _, _ json.RawMessage) ([]interface{}, error) {
			// Return two register updates with an effective increment of 1.
			return []interface{}{
				json.RawMessage(`{"type": "add", "value": 3}`),
				json.RawMessage(`{"type": "add", "value": -2}`),
			}, nil
		},
		"/derive/derivation/publish/Publish": func(source, previous, _ json.RawMessage) ([]interface{}, error) {
			// Join |src| with the register value before its update.
			var src sourceDoc
			if err := json.Unmarshal(source, &src); err != nil {
				return nil, err
			}

			var reg regDoc
			if err := json.Unmarshal(previous, &reg); err != nil {
				return nil, err
			}

			if src.Key == "an-error" {
				return nil, fmt.Errorf("a gnarly error occurred")
			}

			return []interface{}{
				derivedDoc{
					Key:     src.Key,
					Reset:   src.Reset,
					Values:  []int{reg.Value},
					Invalid: src.Invalid,
				},
			}, nil
		},
		"/derive/derivation/reset/Update": func(source, _, _ json.RawMessage) ([]interface{}, error) {
			var src sourceDoc
			if err := json.Unmarshal(source, &src); err != nil {
				return nil, err
			}

			// Emit an invalid register document on seeing value -1.
			if src.Reset == -1 {
				return []interface{}{json.RawMessage(`{"type": "set", "value": "negative one!"}`)}, nil
			} else {
				return []interface{}{regDoc{Type: "set", Value: src.Reset}}, nil
			}
		},
	}
	// Transform "reset" copies the publish behavior of transform "publish".
	handlers["/derive/derivation/reset/Publish"] = handlers["/derive/derivation/publish/Publish"]

	var lambdaClient, stop = NewTestLambdaServer(t, handlers)
	defer stop()

	// Transforms are indexed alphabetically ("increment", "publish", "reset").
	var TF_INC = 0
	var TF_PUB = 1
	var TF_RST = 2

	var apply = func(t *testing.T, d *Derive, tfIndex int, inst sourceDoc) {
		var b, err = json.Marshal(&inst)
		require.NoError(t, err)

		require.NoError(t, d.Add(
			pf.UUIDParts{ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
			tuple.Tuple{inst.Key}.Pack(),
			uint32(tfIndex),
			json.RawMessage(b),
		))
	}

	var ack = func(t *testing.T, d *Derive) {
		require.NoError(t, d.Add(
			pf.UUIDParts{ProducerAndFlags: uint64(message.Flag_ACK_TXN)},
			tuple.Tuple{nil}.Pack(),
			0,
			json.RawMessage("garbage not used"),
		))
	}

	var drainOK = func(t *testing.T, d *Derive) []string {
		var drained []string
		var stats, err = d.Drain(
			func(reduced bool, raw json.RawMessage, packedKey, packedFields []byte) error {
				key, err := tuple.Unpack(packedKey)
				require.NoError(t, err)
				fields, err := tuple.Unpack(packedFields)
				require.NoError(t, err)

				drained = append(drained,
					fmt.Sprintf("reduced %v raw %s key %v fields %v", reduced, string(raw), key, fields))
				return nil
			})
		require.NoError(t, err)
		require.NotNil(t, stats)
		return drained
	}

	var drainError = func(t *testing.T, d *Derive) string {
		var _, err = d.Drain(func(_ bool, _ json.RawMessage, _, _ []byte) error {
			t.Error("not called")
			return nil
		})
		require.Error(t, err)
		return err.Error()
	}

	var opsLogs = make(chan ops.Log)

	var build = func(t *testing.T) *Derive {
		d, err := NewDerive(nil, t.TempDir(), newChanPublisher(opsLogs, pf.LogLevel_warn))
		require.NoError(t, err)
		require.NoError(t, d.Configure("test/derive/withIncReset", derivation, lambdaClient))
		return d
	}

	t.Run("basicRPC", func(t *testing.T) {
		var d = build(t)

		// Apply a batch of documents.
		d.BeginTxn()
		apply(t, d, TF_INC, sourceDoc{Key: "a"})  // => 1.
		apply(t, d, TF_INC, sourceDoc{Key: "a"})  // => 2.
		apply(t, d, TF_INC, sourceDoc{Key: "bb"}) // => 1.
		apply(t, d, TF_PUB, sourceDoc{Key: "bb"}) // Pub 1.
		apply(t, d, TF_PUB, sourceDoc{Key: "a"})  // Pub 2.
		apply(t, d, TF_INC, sourceDoc{Key: "bb"}) // => 2.
		apply(t, d, TF_INC, sourceDoc{Key: "bb"}) // => 3.
		ack(t, d)

		apply(t, d, TF_PUB, sourceDoc{Key: "ccc"})
		apply(t, d, TF_INC, sourceDoc{Key: "bb"})            // => 4.
		apply(t, d, TF_RST, sourceDoc{Key: "bb", Reset: 15}) // Pub 4, => 15.
		apply(t, d, TF_INC, sourceDoc{Key: "bb"})            // => 16.
		apply(t, d, TF_RST, sourceDoc{Key: "a", Reset: 0})   // Pub 2, => 0.
		apply(t, d, TF_INC, sourceDoc{Key: "a"})             // => 1.
		apply(t, d, TF_INC, sourceDoc{Key: "a"})             // => 2.
		apply(t, d, TF_PUB, sourceDoc{Key: "a"})             // Pub 2.
		apply(t, d, TF_PUB, sourceDoc{Key: "bb"})            // Pub 16.
		ack(t, d)

		// Drain transaction, and look for expected roll-ups.
		// Expect nothing is logged to `opsLog`.
		cupaloy.SnapshotT(t, drainOK(t, d))
		require.NoError(t, d.PrepareCommit(protocol.Checkpoint{}))
	})

	t.Run("registerValidationErr", func(t *testing.T) {
		var d = build(t)

		// Send a fixture which tickles our reset lambda to emit an invalid value.
		d.BeginTxn()
		apply(t, d, TF_RST, sourceDoc{Key: "foobar", Reset: -1})
		ack(t, d)

		var err, opsLog = drainError(t, d), <-opsLogs
		cupaloy.SnapshotT(t, err, opsLog.Level, opsLog.Message, string(opsLog.Fields))
	})

	t.Run("derivedValidationErr", func(t *testing.T) {
		var d = build(t)

		// Send a fixture which tickles our reset lambda to emit an invalid value.
		d.BeginTxn()
		apply(t, d, TF_PUB, sourceDoc{Key: "foobar", Invalid: "not empty"})
		ack(t, d)

		var err, opsLog = drainError(t, d), <-opsLogs
		cupaloy.SnapshotT(t, err, opsLog.Level, opsLog.Message, string(opsLog.Fields))
	})

	t.Run("processingErr", func(t *testing.T) {
		var d = build(t)

		// Send a fixture which causes our lambda to return an error.
		d.BeginTxn()
		apply(t, d, TF_PUB, sourceDoc{Key: "an-error"})
		ack(t, d)

		var err, opsLog = drainError(t, d), <-opsLogs
		cupaloy.SnapshotT(t, err, opsLog.Level, opsLog.Message, string(opsLog.Fields))
	})

}
