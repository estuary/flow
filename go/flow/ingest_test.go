package flow

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/task"
)

func TestIngesterLifecycle(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	collections, err := catalog.LoadCapturedCollections()
	require.NoError(t, err)

	// Use JournalSpecs suitable for unit-tests.
	for _, collection := range collections {
		collection.JournalSpec = *brokertest.Journal(pb.JournalSpec{})
	}

	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	wh, err := NewWorkerHost("combine", "--catalog", catalog.LocalPath())
	require.Nil(t, err)
	defer wh.Stop()

	var broker = brokertest.NewBroker(t, etcd, "local", "broker")
	var tasks = task.NewGroup(context.Background())

	journals, err := NewJournalsKeySpace(tasks.Context(), etcd, "/broker.test")
	require.NoError(t, err)
	journals.WatchApplyDelay = 0
	go journals.Watch(tasks.Context(), etcd)

	var ingester = Ingester{
		Collections: collections,
		Combiner:    pf.NewCombineClient(wh.Conn),
		Mapper: &Mapper{
			Ctx:           tasks.Context(),
			JournalClient: broker.Client(),
			Journals:      journals,
		},
		PublishClockDelta: 0,
	}
	ingester.QueueTasks(tasks, broker.Client())
	tasks.GoRun()

	var in1 = ingester.Start()
	var in2 = ingester.Start()

	require.NoError(t, in1.Add("testing/int-string", json.RawMessage(`{"i": 32, "s": "one"}`)))
	require.NoError(t, in2.Add("testing/int-string", json.RawMessage(`{"i": 42, "s": "two"}`)))
	require.NoError(t, in1.Add("testing/int-string", json.RawMessage(`{"i": 32, "s": "three"}`)))
	require.NoError(t, in2.Add("testing/int-string", json.RawMessage(`{"i": 42, "s": "four"}`)))

	// Race Ingestion Prepare(). These ingestions could
	// commit in any order, or in the same transaction.
	var wg sync.WaitGroup
	for _, i := range []*Ingestion{in1, in2} {
		wg.Add(1)

		go func(i *Ingestion) {
			var offsets, err = i.PrepareAndAwait()
			require.NoError(t, err)
			require.Len(t, offsets, 1)
			t.Logf("got offest: %v", offsets)
			wg.Done()
		}(i)
	}
	wg.Wait()

	// An empty Ingestion is allowed (and is useful, as a transaction barrier).
	offsets, err := ingester.Start().PrepareAndAwait()
	require.NoError(t, err)
	require.Empty(t, offsets)

	tasks.Cancel()
	require.NoError(t, tasks.Wait())

	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}
