package bindings

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	_ "github.com/mattn/go-sqlite3" // Import for registration side-effect.
	"github.com/stretchr/testify/require"
	"github.com/tecbot/gorocksdb"
	"go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

func TestDeriveBindings(t *testing.T) {
	var catalog, err = flow.NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	derivation, err := catalog.LoadDerivedCollection("testing/int-strings")
	require.NoError(t, err)
	bundle, err := catalog.LoadSchemaBundle()
	require.NoError(t, err)
	schemaIndex, err := NewSchemaIndex(bundle)
	require.NoError(t, err)

	localDir, err := ioutil.TempDir("", "derive-test")
	require.NoError(t, err)
	defer os.RemoveAll(localDir)

	jsWorker, err := flow.NewJSWorker(catalog, "")
	require.NoError(t, err)
	defer jsWorker.Stop()

	var rocksEnv = gorocksdb.NewDefaultEnv()

	// Tweak fixture so that the derive API produces partition fields.
	// These aren't actually valid partitions, as they're not required to exist.
	derivation.Collection.PartitionFields = []string{"sOne", "eye"}

	der, err := NewDerive(
		schemaIndex,
		derivation,
		rocksEnv,
		jsWorker,
		localDir,
	)
	require.NoError(t, err)
	defer der.Stop()

	_, err = der.RestoreCheckpoint()
	require.NoError(t, err)

	// Expect we can clear registers in between transactions.
	require.NoError(t, der.ClearRegisters())

	der.BeginTxn()

	var fixtures = []struct {
		key int
		doc string
	}{
		{32, `{"i":32, "s":"one"}`},
		{42, `{"i":42, "s":"two"}`},
		{42, `{"i":42, "s":"three"}`},
		{32, `{"i":32, "s":"four"}`},
	}
	for _, fixture := range fixtures {
		require.NoError(t, der.Add(
			pf.UUIDParts{ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
			tuple.Tuple{fixture.key}.Pack(),
			0,
			json.RawMessage(fixture.doc),
		))
	}

	// Drain transaction, and look for expected roll-ups.
	expectCombineFixture(t, der.Finish)

	require.NoError(t, der.PrepareCommit(protocol.Checkpoint{}))
}
