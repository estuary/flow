package runtime

import (
	"context"
	"fmt"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/ingest"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// FlowIngesterConfig configures the flow-ingester application.
type FlowIngesterConfig struct {
	Ingest struct {
		mbp.ServiceConfig
		Catalog string `long:"catalog" required:"true" description:"Catalog URL or local path"`
	} `group:"Ingest" namespace:"ingest" env-namespace:"INGEST"`

	Flow struct {
		BrokerRoot string `long:"broker-root" env:"BROKER_ROOT" default:"/gazette/cluster" description:"Broker Etcd base prefix"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`

	Etcd        mbp.EtcdConfig        `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`
	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

// FlowIngesterArgs implements the Estuary Flow Ingester.
type FlowIngesterArgs struct {
	// Flow catalog served by the ingester.
	Catalog *flow.Catalog
	// Etcd prefix of the broker.
	BrokerRoot string
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
	collections, err := args.Catalog.LoadCapturedCollections()
	if err != nil {
		return nil, err
	}
	bundle, err := args.Catalog.LoadSchemaBundle()
	if err != nil {
		return nil, err
	}
	schemaIndex, err := bindings.NewSchemaIndex(bundle)
	if err != nil {
		return nil, err
	}
	journalRules, err := args.Catalog.LoadJournalRules()
	if err != nil {
		return nil, err
	}
	for _, collection := range collections {
		log.WithField("name", collection.Collection).Info("serving captured collection")
	}

	// Start watch of broker journal keyspace.
	journals, err := flow.NewJournalsKeySpace(context.Background(),
		args.Etcd, args.BrokerRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to build journals keyspace: %w", err)
	}

	args.Tasks.Queue("journals.Watch", func() error {
		if err := journals.Watch(args.Tasks.Context(), args.Etcd); err != context.Canceled {
			return err
		}
		return nil
	})

	var mapper = &flow.Mapper{
		Ctx:           args.Tasks.Context(),
		JournalClient: args.Journals,
		Journals:      journals,
		JournalRules:  journalRules.Rules,
	}
	var ingester = &ingest.Ingester{
		Collections:       collections,
		CombineBuilder:    bindings.NewCombineBuilder(schemaIndex),
		Mapper:            mapper,
		PublishClockDelta: 0,
	}
	ingester.QueueTasks(args.Tasks, args.Journals)
	ingest.RegisterAPIs(args.Server, ingester, journals)

	return ingester, nil
}
