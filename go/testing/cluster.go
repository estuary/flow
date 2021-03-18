package testing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/estuary/flow/go/ingest"
	"github.com/estuary/flow/go/runtime"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker"
	"go.gazette.dev/core/broker/client"
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
	Context            context.Context
	DisableClockTicks  bool
	Etcd               *clientv3.Client
	EtcdCatalogPrefix  string
	EtcdBrokerPrefix   string
	EtcdConsumerPrefix string
}

// Cluster is an in-process Flow cluster environment.
type Cluster struct {
	Consumer *runtime.FlowConsumer
	Ingester *ingest.Ingester
	Journals pb.RoutedJournalClient
	Server   *server.Server
	Shards   pc.ShardClient
	Tasks    *task.Group
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
			ks         = broker.NewKeySpace(c.EtcdBrokerPrefix)
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

		ks.Observers = append(ks.Observers, func() {
			for _, item := range allocState.LocalItems {
				var name = item.Item.Decoded.(allocator.Item).ID
				go resetJournalHead(tasks.Context(), rjc, pb.Journal(name))
			}
		})

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
		appConfig.Flow.BrokerRoot = c.EtcdBrokerPrefix
		appConfig.Flow.CatalogRoot = c.EtcdCatalogPrefix
		appConfig.DisableClockTicks = c.DisableClockTicks

		var (
			spec = &pc.ConsumerSpec{
				ShardLimit:  consumerLimit,
				ProcessSpec: processSpec,
			}
			ks    = consumer.NewKeySpace(c.EtcdConsumerPrefix)
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
		BrokerRoot:  c.EtcdBrokerPrefix,
		CatalogRoot: c.EtcdCatalogPrefix,
		Server:      server,
		Tasks:       tasks,
		Journals:    rjc,
		Etcd:        c.Etcd,
	})
	if err != nil {
		return nil, fmt.Errorf("starting ingester: %w", err)
	}

	tasks.GoRun()

	return &Cluster{
		Consumer: flowConsumer,
		Ingester: ingester,
		Journals: rjc,
		Server:   server,
		Shards:   pc.NewShardClient(server.GRPCLoopback),
		Tasks:    tasks,
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
const brokerLimit = 1024
const consumerLimit = 1024
const etcdLease = time.Second * 10

// resetJournalHead queries the largest written offset of the journal,
// and issues an empty append with that explicit offset.
func resetJournalHead(ctx context.Context, rjc pb.RoutedJournalClient, name pb.Journal) error {
	var r = client.NewReader(ctx, rjc, pb.ReadRequest{
		Journal:      name,
		Offset:       -1,
		Block:        false,
		MetadataOnly: true,
	})
	if _, err := r.Read(nil); err != client.ErrOffsetNotYetAvailable {
		return fmt.Errorf("reading head of journal %q: %w", name, err)
	}
	// Issue a zero-byte write at the indexed head.
	var a = client.NewAppender(ctx, rjc, pb.AppendRequest{
		Journal: name,
		Offset:  r.Response.Offset,
	})
	var err = a.Close()

	if err == nil || err == client.ErrWrongAppendOffset {
		// Success, or raced write (indicating reset wasn't needed).
	} else {
		return fmt.Errorf("setting offset of %q: %w", name, err)
	}

	return nil
}
