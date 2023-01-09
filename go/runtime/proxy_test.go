package runtime

import (
	"testing"

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	"google.golang.org/grpc"

	//pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestExposeCalledAfterUnexpose(t *testing.T) {
	var logger = ops.NewLocalPublisher(labels.ShardLabeling{
		Build:    "test-build",
		TaskName: "foo/bar",
		TaskType: "capture",
	})
	var server = &ProxyServer{
		containers: make(map[pc.ShardID]*runningContainer),
	}

	var shard = pc.ShardID("foo/bar/test/shard")
	var ports = map[uint16]*labels.PortConfig{
		1234: {},
	}

	var handle = networkConfigHandle{
		server:  server,
		shardID: shard,
		ports:   ports,
	}

	handle.Unexpose()

	handle.Expose(nil, logger)

	require.Equal(t, 0, len(server.containers))
}

func TestExposeCalledBeforePreviousInstanceIsUnexposed(t *testing.T) {
	var logger = ops.NewLocalPublisher(labels.ShardLabeling{
		Build:    "test-build",
		TaskName: "foo/bar",
		TaskType: "capture",
	})
	var server = &ProxyServer{
		containers: make(map[pc.ShardID]*runningContainer),
	}

	var shard = pc.ShardID("foo/bar/test/shard")
	var ports = map[uint16]*labels.PortConfig{
		1234: {},
	}

	var connA = &grpc.ClientConn{}

	var handleA = networkConfigHandle{
		server:  server,
		shardID: shard,
		ports:   ports,
	}

	handleA.Expose(connA, logger)
	require.Same(t, connA, server.containers[shard].connection)

	// Simulate a second instance of the container starting up before the first one was fully torn down
	var connB = &grpc.ClientConn{}
	var handleB = networkConfigHandle{
		server:  server,
		shardID: shard,
		ports:   ports,
	}
	handleB.Expose(connB, logger)

	// Expect that the _new_ connnection is now the one that's exposed
	require.Same(t, connB, server.containers[shard].connection)
	require.Equal(t, 1, len(server.containers))

	// Simulate a delayed call to unexpose the first handle, and ensure that it is ignored.
	handleA.Unexpose()
	require.Same(t, connB, server.containers[shard].connection)
	require.Equal(t, 1, len(server.containers))

	// Unexposing the second handle should still work
	handleB.Unexpose()
	require.Equal(t, 0, len(server.containers))
}
