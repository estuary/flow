package runtime

import (
	"context"
	"fmt"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/ingest"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// FlowIngesterConfig configures the flow-ingester application.
type FlowIngesterConfig struct {
	Ingest struct {
		mbp.ServiceConfig
	} `group:"Ingest" namespace:"ingest" env-namespace:"INGEST"`

	Flow        FlowConfig            `group:"flow" namespace:"flow" env-namespace:"FLOW"`
	Etcd        mbp.EtcdConfig        `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`
	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

// FlowIngesterArgs implements the Estuary Flow Ingester.
type FlowIngesterArgs struct {
	// Etcd prefix of the broker.
	BrokerRoot string
	// Etcd prefix of the catalog.
	CatalogRoot string
	// Server is a dual HTTP and gRPC Server. Applications may register
	// APIs they implement against the Server mux.
	Server *server.Server
	// Tasks are independent, cancelable goroutines having the lifetime of
	// the consumer, such as service loops and the like. Applications may
	// add additional tasks which should be started with the consumer.
	Tasks *task.Group
	// Journal client for use by consumer applications.
	Journals pb.RoutedJournalClient
	// Etcd client for use by consumer applications.
	Etcd *clientv3.Client
}

// StartIngesterService initializes the Ingester and wires up all API handlers.
func StartIngesterService(args FlowIngesterArgs) (*ingest.Ingester, error) {
	var ctx = context.Background()

	// Load catalog & journal keyspaces, and queue tasks that watch each for updates.
	catalog, err := flow.NewCatalog(ctx, args.Etcd, args.CatalogRoot)
	if err != nil {
		return nil, fmt.Errorf("loading catalog keyspace: %w", err)
	}
	journals, err := flow.NewJournalsKeySpace(ctx, args.Etcd, args.BrokerRoot)
	if err != nil {
		return nil, fmt.Errorf("loading journals keyspace: %w", err)
	}

	args.Tasks.Queue("catalog.Watch", func() error {
		if err := catalog.Watch(args.Tasks.Context(), args.Etcd); err != context.Canceled {
			return err
		}
		return nil
	})
	args.Tasks.Queue("journals.Watch", func() error {
		if err := journals.Watch(args.Tasks.Context(), args.Etcd); err != context.Canceled {
			return err
		}
		return nil
	})

	var ingester = &ingest.Ingester{
		Catalog:                  catalog,
		Journals:                 journals,
		JournalClient:            args.Journals,
		PublishClockDeltaForTest: 0,
	}
	ingester.QueueTasks(args.Tasks, args.Journals)
	ingest.RegisterAPIs(args.Server, ingester, journals)

	return ingester, nil
}
