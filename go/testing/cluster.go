package testing

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/estuary/flow/go/ingest"
	"github.com/estuary/flow/go/runtime"
	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/fragment"
	"go.gazette.dev/core/broker/http_gateway"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// ClusterConfig configures a single-process Flow cluster member.
type ClusterConfig struct {
	mbp.ServiceConfig
	Context            context.Context
	DisableClockTicks  bool
	Etcd               *clientv3.Client
	EtcdBrokerPrefix   string
	EtcdCatalogPrefix  string
	EtcdConsumerPrefix string
	Poll               bool
}

// Cluster is an in-process Flow cluster environment.
type Cluster struct {
	Config      ClusterConfig
	Consumer    *runtime.FlowConsumer
	Ingester    *ingest.Ingester
	Journals    pb.RoutedJournalClient
	Server      *server.Server
	Shards      pc.ShardClient
	Tasks       *task.Group
	ProcessSpec pb.ProcessSpec
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

		// Testing Clusters which are stopped and started (e.x. `flowctl develop`)
		// by definition have lost broker offset consistency: they start again at
		// offset zero, while having larger offset in the fragment index.
		// Automatically reset the write-heads of these journals.
		ks.Observers = append(ks.Observers, func() {
			for _, item := range allocState.LocalItems {
				var name = item.Item.Decoded.(allocator.Item).ID
				go resetJournalHead(tasks.Context(), rjc, pb.Journal(name))
			}
		})
		ks.WatchApplyDelay = 0 // Faster convergence.

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
		appConfig.Poll = c.Poll

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
		ks.WatchApplyDelay = 0 // Faster convergence.

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
		Config:      c,
		Consumer:    flowConsumer,
		Ingester:    ingester,
		Journals:    rjc,
		Server:      server,
		Shards:      pc.NewShardClient(server.GRPCLoopback),
		Tasks:       tasks,
		ProcessSpec: processSpec,
	}, nil
}

// Stop the Cluster, blocking until it's shut down.
func (c *Cluster) Stop() error {
	defer c.Tasks.Cancel()

	var memberKey = allocator.MemberKey(
		&keyspace.KeySpace{Root: c.Config.EtcdConsumerPrefix},
		c.ProcessSpec.Id.Zone,
		c.ProcessSpec.Id.Suffix)

	// We don't use c.Config.Context because it's presumably already done
	// (since we're stopping).
	var _, err = c.Config.Etcd.Delete(context.Background(), memberKey)
	if err != nil {
		return fmt.Errorf("attempting to delete member key: %w", err)
	}

	err = c.Tasks.Wait()
	if err != nil && strings.Contains(err.Error(), "member key not found in Etcd") {
		err = nil // Expected given our tear-down behavior.
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

// WaitForShardsToAssign blocks until all shards have reached PRIMARY.
func (c *Cluster) WaitForShardsToAssign() error {
	var state = c.Consumer.Service.State

	state.KS.Mu.RLock()
	defer state.KS.Mu.RUnlock()

	for {
		var wait bool

		// This is subtly wrong, in the general case but not the local-cluster
		// testing case, because assignments can also include non-primary replicas.
		// TODO(johnny): Should allocator.State better surface this?
		if state.ItemSlots != len(state.Assignments) {
			wait = true
		}

		for _, a := range state.Assignments {
			var (
				decoded = a.Decoded.(allocator.Assignment)
				status  = decoded.AssignmentValue.(*pc.ReplicaStatus)
			)
			if decoded.Slot == 0 && status.Code < pc.ReplicaStatus_PRIMARY {
				wait = true
			}
		}

		if !wait {
			return nil
		}

		// Block for the next KeySpace update.
		if err := state.KS.WaitForRevision(c.Config.Context, state.KS.Header.Revision+1); err != nil {
			return err
		}
	}
}
