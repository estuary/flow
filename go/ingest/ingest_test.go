package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/task"
)

func TestIngesterLifecycle(t *testing.T) {
	var built, err = bindings.BuildCatalog(bindings.BuildArgs{
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
	var tasks = task.NewGroup(context.Background())

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

	// After |tasks| exits, an attempt to prepare an ingestion fails.
	require.Equal(t, ErrIngesterExiting, ingester.Start().Prepare())

	// Case: if the Ingester's ingestPublish is failed, it aborts
	// the Ingester loop and bubbles up as a task failure & cancellation.
	tasks = task.NewGroup(context.Background())
	ingester = &Ingester{
		Catalog:                  catalog,
		Journals:                 journals,
		JournalClient:            broker.Client(),
		PublishClockDeltaForTest: 0,
	}
	ingester.QueueTasks(tasks, broker.Client())
	tasks.GoRun()

	var pub = <-ingester.pubCh
	pub.failed = fmt.Errorf("an error")
	ingester.pubCh <- pub

	// Concurrent prepares are notified of the failure.
	require.Equal(t, ErrIngesterExiting, ingester.Start().Prepare())
	// Error bubbles up to cancel the task group, which exits with an error.
	require.EqualError(t, tasks.Wait(), "ingesterCommitLoop: ingest publisher had terminal error: an error")

	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}
