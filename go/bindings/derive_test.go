package bindings

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	_ "github.com/mattn/go-sqlite3" // Import for registration side-effect.
	"github.com/stretchr/testify/require"
	"github.com/tecbot/gorocksdb"
	"go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

func TestDeriveBindings(t *testing.T) {
	const dbPath = "../../catalog.db"
	const collection = "testing/int-strings"
	const transform = "appendStrings"

	var db, err = sql.Open("sqlite3", "file:"+dbPath+"?immutable=true&mode=ro")
	require.NoError(t, err)

	var transformID int32
	var row = db.QueryRow("SELECT transform_id FROM transform_details "+
		"WHERE derivation_name = ? and transform_name = ?", collection, transform)
	require.NoError(t, row.Scan(&transformID))

	tmpdir, err := ioutil.TempDir("", "derive-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	var dbEnv = gorocksdb.NewDefaultEnv()

	der, err := NewDerive(
		"../../catalog.db",
		"testing/int-strings",
		tmpdir,
		"/meta/_uuid",
		[]string{"/s/1", "/i"},
		dbEnv)
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
			transformID,
			json.RawMessage(fixture.doc),
		))
	}

	// Drain transaction, and look for expected roll-ups.
	expectCombineFixture(t, der.Finish)

	require.NoError(t, der.PrepareCommit(protocol.Checkpoint{}))
}
