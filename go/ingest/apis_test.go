package ingest

import (
	"context"
	"net/http"
	"testing"

	"go.gazette.dev/core/server"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/task"
)

func TestAPIs(t *testing.T) {
	var catalog, err = flow.NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	collections, err := catalog.LoadCapturedCollections()
	require.NoError(t, err)

	// Use JournalSpecs suitable for unit-tests.
	for _, collection := range collections {
		collection.JournalSpec = *brokertest.Journal(pb.JournalSpec{})
	}

	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	wh, err := flow.NewWorkerHost("combine", "--catalog", catalog.LocalPath())
	require.Nil(t, err)
	defer wh.Stop()

	var broker = brokertest.NewBroker(t, etcd, "local", "broker")
	var tasks = task.NewGroup(context.Background())

	journals, err := flow.NewJournalsKeySpace(tasks.Context(), etcd, "/broker.test")
	require.NoError(t, err)
	journals.WatchApplyDelay = 0
	go journals.Watch(tasks.Context(), etcd)

	var ingester = &flow.Ingester{
		Collections: collections,
		Combiner:    pf.NewCombineClient(wh.Conn),
		Mapper: &flow.Mapper{
			Ctx:           tasks.Context(),
			JournalClient: broker.Client(),
			Journals:      journals,
		},
		PublishClockDelta: 0,
	}
	ingester.QueueTasks(tasks, broker.Client())

	var srv = server.MustLoopback()
	var addr = srv.RawListener.Addr().String()
	srv.HTTPMux = http.NewServeMux() // Don't use default mux.
	RegisterAPIs(srv, ingester, journals)
	srv.QueueTasks(tasks)

	tasks.GoRun()

	// Actual sub-tests all go here:
	t.Run("httpSimple", func(t *testing.T) { testHTTPSimple(t, addr) })
	t.Run("httpNotFound", func(t *testing.T) { testHTTPNotFound(t, addr) })
	t.Run("httpMalformed", func(t *testing.T) { testHTTPMalformed(t, addr) })
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
