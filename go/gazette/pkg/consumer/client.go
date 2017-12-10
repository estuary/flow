package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/keepalive"
)

// Client interacts with a Gazette Consumer to maintain an updated pool
// of GRPC client connections to live Consumer endpoints, along with a local
// snapshot of Consumer topology for selecting appropriate endpoints.
type Client struct {
	service *grpc.ClientConn

	// Drives periodic polls of the consumer's state.
	refreshTicker *time.Ticker
	// Signals the Client.serve goroutine.
	signalCh chan clientSignal

	// Once attached to a *Client instance, |state|, |conns|, and |index| are read
	// only, allowing for unlocked concurrent use of the maps. Modifications are
	// made by constructing new maps and swapping out the instances
	// (guarded by |mu|).
	state ConsumerState
	conns poolConns
	index partitionShards
	mu    sync.Mutex
}

// A collection of ClientConn instances indexed on Dialed endpoint address.
type poolConns map[string]*grpc.ClientConn

// A collection of ConsumerState_Shard instances indexed on shard partition.
type partitionShards map[journal.Name]ConsumerState_Shard

// Used to signal Client.serve.
type clientSignal bool

const (
	shardClientCacheErrBackoff   = time.Second * 5
	shardClientCachePollInterval = time.Second * 30

	// Signals to Client.serve that to begin a refresh.
	signalRefresh clientSignal = false
	// Signals to Client.serve to halt.
	signalStop clientSignal = true
)

var (
	ErrNoSuchConsumerPartition = errors.New("no such consumer partition")
	ErrNoReadyPartitionClient  = errors.New("no ready consumer partition replica client")
)

func NewClient(endpoint string) (*Client, error) {
	var conn, err = grpc.Dial(endpoint,
		grpc.WithBlock(),
		grpc.WithDialer(keepalive.DialerFunc),
		grpc.WithInsecure())

	if err != nil {
		return nil, err
	}

	var cc = &Client{
		service: conn,

		refreshTicker: time.NewTicker(shardClientCachePollInterval),
		signalCh:      make(chan clientSignal),

		conns: make(poolConns),
		index: make(partitionShards),
	}

	if err = cc.update(); err != nil {
		return nil, err
	}
	go cc.serve()

	return cc, nil
}

// PartitionClient maps |partition| to its live endpoint and ConsumerState_Shard.
func (c *Client) PartitionClient(partition journal.Name) (*grpc.ClientConn, ConsumerState_Shard, error) {
	c.mu.Lock()
	var index, conns = c.index, c.conns
	c.mu.Unlock()

	if shard, ok := index[partition]; !ok {
		return nil, shard, ErrNoSuchConsumerPartition
	} else if len(shard.Replicas) == 0 || shard.Replicas[0].Status != ConsumerState_Replica_PRIMARY {
		return nil, shard, ErrNoReadyPartitionClient
	} else if conn, ok := conns[shard.Replicas[0].Endpoint]; !ok {
		return nil, shard, ErrNoReadyPartitionClient
	} else {
		return conn, shard, nil
	}
}

func (c *Client) State() ConsumerState {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.state
}

// Invalidate triggers an immediate refresh of Client state.
func (c *Client) Invalidate() {
	select {
	case c.signalCh <- signalRefresh:
		// Pass.
	default:
		// An update is already in progress.
	}
}

// Close shuts down a Client.
func (c *Client) Close() {
	c.signalCh <- signalStop

	// Block until closed.
	for range c.signalCh {
	}
}

func (c *Client) serve() {
	for {
		var signal clientSignal

		select {
		case <-c.refreshTicker.C:
		case signal = <-c.signalCh:
		}

		if signal == signalStop {
			break
		}

		if err := c.update(); err != nil {
			log.WithField("err", err).Warn("failed to refresh Shard routes")

			time.Sleep(shardClientCacheErrBackoff)
		}
	}

	for ep, conn := range c.conns {
		if err := conn.Close(); err != nil {
			log.WithFields(log.Fields{"endpoint": ep, "err": err}).
				Warn("failed to close GRPC ClientConn")
		}
	}

	c.refreshTicker.Stop()
	close(c.signalCh)

	return
}

func (c *Client) update() error {
	var state, err = NewConsumerClient(c.service).
		CurrentConsumerState(context.Background(), &Empty{})
	if err != nil {
		return err
	}

	c.mu.Lock()
	var oldConns = c.conns
	c.mu.Unlock()

	var conns = make(map[string]*grpc.ClientConn)
	var index = make(partitionShards)

	// Ensure GRPC connections exist to each client.
	for _, ep := range state.Endpoints {
		if conn, ok := oldConns[ep]; ok {
			conns[ep] = conn
		} else if conn, err = grpc.Dial(ep,
			grpc.WithDialer(keepalive.DialerFunc),
			grpc.WithInsecure(),
		); err != nil {
			// Without grpc.WithBlock, Dial should return immediately, with
			// connections attempted in the background. We fatal rather than return
			// the error, because 1) this really should not happen, and 2) if it does,
			// returning would leak grpc.ClientConn instances (and TCP sockets).
			log.WithFields(log.Fields{"endpoint": ep, "err": err}).
				Fatal("non-blocking grpc.Dial returned error")
		} else {
			log.WithField("endpoint", ep).Debug("built new GRPC ClientConn")
			conns[ep] = conn
		}
	}

	for _, s := range state.Shards {
		index[s.Partition] = s
	}

	c.mu.Lock()
	c.state, c.conns, c.index = *state, conns, index
	c.mu.Unlock()

	// Tear down endpoint connections which are no longer needed.
	for ep, conn := range oldConns {
		if _, ok := conns[ep]; ok {
			continue // This endpoint is still in use.
		}
		if err := conn.Close(); err != nil {
			log.WithFields(log.Fields{"endpoint": ep, "err": err}).
				Warn("failed to close GRPC ClientConn")
		}
		log.WithField("endpoint", ep).Info("closed GRPC ClientConn")
	}

	return nil
}
