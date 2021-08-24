package ingest

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"

	"go.gazette.dev/core/server"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/task"
)

func TestAPIs(t *testing.T) {
	var built, err = bindings.BuildCatalog(bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			Directory:   "testdata",
			Source:      "file:///flow.yaml",
			SourceType:  pf.ContentType_CATALOG_SPEC,
			CatalogPath: filepath.Join(t.TempDir(), "catalog.db"),
			ExtraJournalRules: &pf.JournalRules{
				Rules: []pf.JournalRules_Rule{
					{
						Rule:     "Override for single brokertest broker",
						Template: pb.JournalSpec{Replication: 1},
					},
				},
			},
		}})
	require.NoError(t, err)
	require.Empty(t, built.Errors)

	var ctx = context.Background()
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	_, _, err = flow.ApplyCatalogToEtcd(flow.ApplyArgs{
		Ctx:           ctx,
		Etcd:          etcd,
		Root:          "/flow/catalog",
		Build:         built,
		TypeScriptUDS: "/not/used",
	})
	require.NoError(t, err)
	catalog, err := flow.NewCatalog(ctx, etcd, "/flow/catalog")
	require.NoError(t, err)

	var broker = brokertest.NewBroker(t, etcd, "local", "broker")
	var tasks = task.NewGroup(ctx)

	journals, err := flow.NewJournalsKeySpace(tasks.Context(), etcd, "/broker.test")
	require.NoError(t, err)
	journals.WatchApplyDelay = 0
	go journals.Watch(tasks.Context(), etcd)

	var ingester = &Ingester{
		Catalog:                  catalog,
		Journals:                 journals,
		JournalClient:            broker.Client(),
		PublishClockDeltaForTest: 0,
	}
	ingester.QueueTasks(tasks, broker.Client())

	var srv = server.MustLoopback()
	var addr = srv.RawListener.Addr().String()
	srv.HTTPMux = http.NewServeMux() // Don't use default mux.
	RegisterAPIs(srv, ingester, journals)
	srv.QueueTasks(tasks)

	tasks.GoRun()

	// Actual sub-tests all go here:
	t.Run("httpSingleSimple", func(t *testing.T) { testHTTPSingleSimple(t, addr) })
	t.Run("httpSingleNotFound", func(t *testing.T) { testHTTPSingleNotFound(t, addr) })
	t.Run("httpSingleMalformed", func(t *testing.T) { testHTTPSingleMalformed(t, addr) })
	t.Run("httpMultiSimple", func(t *testing.T) { testHTTPMultiSimple(t, addr) })
	t.Run("httpMultiNotFound", func(t *testing.T) { testHTTPMultiNotFound(t, addr) })
	t.Run("httpMultiMalformed", func(t *testing.T) { testHTTPMultiMalformed(t, addr) })
	t.Run("csvSimple", func(t *testing.T) { testCSVSimple(t, addr) })
	t.Run("csvCollectionNotFound", func(t *testing.T) { testCSVCollectionNotFound(t, addr) })
	t.Run("csvMalformed", func(t *testing.T) { testCSVMalformed(t, addr) })
	t.Run("csvMissingRequired", func(t *testing.T) { testCSVMissingRequired(t, addr) })
	t.Run("csvConvertRequiredNullable", func(t *testing.T) { testCSVConvertRequiredNullable(t, addr) })
	t.Run("csvMissingMustExistNullable", func(t *testing.T) { testCSVMissingMustExistNullable(t, addr) })
	t.Run("csvValueFailsValidation", func(t *testing.T) { testCSVValueFailsValidation(t, addr) })
	t.Run("csvProjectionNotFound", func(t *testing.T) { testCSVProjectionNotFound(t, addr) })
	t.Run("csvHeaderMissingRequiredField", func(t *testing.T) { testCSVHeaderMissingRequiredField(t, addr) })
	t.Run("csvOptionalMultipleTypes", func(t *testing.T) { testCSVOptionalMultipleTypes(t, addr) })
	t.Run("csvNumOrIntOrNull", func(t *testing.T) { testCSVNumOrIntOrNull(t, addr) })
	t.Run("csvOptionalObjectsAndArrays", func(t *testing.T) { testCSVOptionalObjectsAndArrays(t, addr) })
	t.Run("csvUnsupportedObject", func(t *testing.T) { testCSVUnsupportedObject(t, addr) })
	t.Run("csvUnsupportedArray", func(t *testing.T) { testCSVUnsupportedArray(t, addr) })
	t.Run("csvConversionError", func(t *testing.T) { testCSVConversionError(t, addr) })
	t.Run("csvEmptyBody", func(t *testing.T) { testCSVEmptyBody(t, addr) })
	t.Run("csvTypeConversions", testCSVTypeConversions)
	t.Run("tsvSimple", func(t *testing.T) { testTSVSimple(t, addr) })
	t.Run("jsonSimple", func(t *testing.T) { testJSONSimple(t, addr) })
	t.Run("jsonMalformed", func(t *testing.T) { testJSONMalformed(t, addr) })
	t.Run("jsonInvalidSchema", func(t *testing.T) { testJSONInvalidSchema(t, addr) })
	t.Run("grpcSimple", func(t *testing.T) { testGRPCSimple(t, addr) })
	t.Run("grpcNotFound", func(t *testing.T) { testGRPCNotFound(t, addr) })

	tasks.Cancel()
	srv.BoundedGracefulStop()
	require.NoError(t, tasks.Wait())

	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
