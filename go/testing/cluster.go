package testing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/ingest"
	"github.com/estuary/flow/go/runtime"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker"
	"go.gazette.dev/core/broker/fragment"
	"go.gazette.dev/core/broker/http_gateway"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
	"google.golang.org/grpc"
)

// ClusterConfig configures a single-process Flow cluster member.
type ClusterConfig struct {
	mbp.ServiceConfig
	Context           context.Context
	Catalog           *flow.Catalog
	DisableClockTicks bool
	Etcd              *clientv3.Client
	LambdaJSUDS       string
}

// Cluster is an in-process Flow cluster environment.
type Cluster struct {
	Tasks    *task.Group
	Server   *server.Server
	Journals pb.RoutedJournalClient
	Shards   pc.ShardClient
	Ingester *ingest.Ingester
	Consumer *runtime.FlowConsumer

	// TODO remove me when we load specs into Etcd.
	SchemaIndex *bindings.SchemaIndex
}

// NewCluster builds and returns a new, running flow Cluster.
func NewCluster(c ClusterConfig) (*Cluster, error) {
	// Task group under which all cluster services are placed.
	var tasks = task.NewGroup(pb.WithDispatchDefault(c.Context))

	// Bind a joint server for gazette broker, flow-consumer, and flow-ingester.
	var server, err = server.New("", c.Port)
	if err != nil {
		return nil, fmt.Errorf("server.New: %w", err)
	}
	server.QueueTasks(tasks)

	var (
		processSpec = c.ServiceConfig.BuildProcessSpec(server)
		rjc         pb.RoutedJournalClient
	)

	log.WithFields(log.Fields{
		"zone":     processSpec.Id.Zone,
		"id":       processSpec.Id.Suffix,
		"endpoint": processSpec.Endpoint,
	}).Info("starting in-process Flow cluster")

	// Wire up Gazette broker service.
	{
		var (
			spec = &pb.BrokerSpec{
				JournalLimit: brokerLimit,
				ProcessSpec:  processSpec,
			}
			lo         = pb.NewJournalClient(server.GRPCLoopback)
			ks         = broker.NewKeySpace(etcdBrokerPrefix)
			allocState = allocator.NewObservedState(ks,
				allocator.MemberKey(ks, spec.Id.Zone, spec.Id.Suffix),
				broker.JournalIsConsistent)
			service = broker.NewService(allocState, lo, c.Etcd)
		)
		rjc = pb.NewRoutedJournalClient(lo, service)

		pb.RegisterJournalServer(server.GRPCServer, service)
		server.HTTPMux.Handle("/journal", http_gateway.NewGateway(rjc))

		var persister = fragment.NewPersister(ks)
		broker.SetSharedPersister(persister)

		tasks.Queue("persister.Serve", func() error {
			persister.Serve()
			return nil
		})
		service.QueueTasks(tasks, server, persister.Finish)

		err = allocator.StartSession(allocator.SessionArgs{
			Etcd:     c.Etcd,
			LeaseTTL: etcdLease,
			SignalCh: make(chan os.Signal),
			Spec:     spec,
			State:    allocState,
			Tasks:    tasks,
		})
		if err != nil {
			return nil, fmt.Errorf("starting broker session: %w", err)
		}
	}

	// Wire up Flow consumer service.
	var flowConsumer = new(runtime.FlowConsumer)
	{
		var appConfig = new(runtime.FlowConsumerConfig)
		appConfig.Flow.BrokerRoot = etcdBrokerPrefix
		appConfig.DisableClockTicks = c.DisableClockTicks
		appConfig.Flow.LambdaJS = c.LambdaJSUDS

		var (
			spec = &pc.ConsumerSpec{
				ShardLimit:  consumerLimit,
				ProcessSpec: processSpec,
			}
			ks    = consumer.NewKeySpace(etcdConsumerPrefix)
			state = allocator.NewObservedState(ks,
				allocator.MemberKey(ks, spec.Id.Zone, spec.Id.Suffix),
				consumer.ShardIsConsistent)
			service = consumer.NewService(flowConsumer, state, rjc,
				server.GRPCLoopback, c.Etcd)
		)

		pc.RegisterShardServer(server.GRPCServer, service)
		service.QueueTasks(tasks, server)

		err = flowConsumer.InitApplication(runconsumer.InitArgs{
			Context: tasks.Context(),
			Config:  appConfig,
			Server:  server,
			Service: service,
			Tasks:   tasks,
		})
		if err != nil {
			return nil, fmt.Errorf("flow consumer init: %w", err)
		}

		err = allocator.StartSession(allocator.SessionArgs{
			Etcd:     c.Etcd,
			LeaseTTL: etcdLease,
			SignalCh: make(chan os.Signal),
			Spec:     spec,
			State:    state,
			Tasks:    tasks,
		})
		if err != nil {
			return nil, fmt.Errorf("starting flow consumer session: %w", err)
		}
	}

	// Start Flow ingester.
	ingester, err := runtime.StartIngesterService(runtime.FlowIngesterArgs{
		Catalog:    c.Catalog,
		BrokerRoot: etcdBrokerPrefix,
		Server:     server,
		Tasks:      tasks,
		Journals:   rjc,
		Etcd:       c.Etcd,
	})
	if err != nil {
		return nil, fmt.Errorf("starting ingester: %w", err)
	}

	schemaBundle, err := c.Catalog.LoadSchemaBundle()
	if err != nil {
		return nil, fmt.Errorf("loading schema bundle: %w", err)
	}
	schemaIndex, err := bindings.NewSchemaIndex(schemaBundle)
	if err != nil {
		return nil, fmt.Errorf("building schema index: %w", err)
	}
	tasks.GoRun()

	return &Cluster{
		Tasks:       tasks,
		Server:      server,
		Journals:    rjc,
		Shards:      pc.NewShardClient(server.GRPCLoopback),
		Ingester:    ingester,
		Consumer:    flowConsumer,
		SchemaIndex: schemaIndex,
	}, nil
}

// Stop the Cluster, blocking until it's shut down.
func (c *Cluster) Stop() error {
	c.Tasks.Cancel()
	var err = c.Tasks.Wait()

	if errors.Is(err, grpc.ErrClientConnClosing) {
		// This error is expected, as both the broker and consumer
		// service will each close the gRPC client on tear-down.
		err = nil
	}
	return err
}

// "Reasonable defaults" for which we're not bothering to wire up configuration.
const etcdBrokerPrefix = "/gazette/cluster"
const etcdConsumerPrefix = "/gazette/consumers/runtime.Flow"
const brokerLimit = 1024
const consumerLimit = 1024
const etcdLease = time.Second * 20
