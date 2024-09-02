package bindings

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math"
	"path"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/protocols/catalog"
	pd "github.com/estuary/flow/go/protocols/derive"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestSimpleDerive(t *testing.T) {
	pb.RegisterGRPCDispatcher("local") // Required (only) by sqlite.InProcessServer.

	var args = BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "9999999999999999",
			BuildDb:    path.Join(t.TempDir(), "build.db"),
			Source:     "file:///build.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, BuildCatalog(args))

	var collection *pf.CollectionSpec

	require.NoError(t, catalog.Extract(args.BuildDb, func(db *sql.DB) (err error) {
		if collection, err = catalog.LoadCollection(db, "a/derivation"); err != nil {
			return fmt.Errorf("loading collection: %w", err)
		}
		return nil
	}))

	var socket = path.Join(t.TempDir(), "sock")
	var publisher = testPublisher{t: t}

	var svc, err = NewTaskService(
		pr.TaskServiceConfig{
			TaskName: "hello-world",
			UdsPath:  socket,
		},
		publisher.PublishLog,
	)
	require.NoError(t, err)

	stream, err := pd.NewConnectorClient(svc.Conn()).Derive(context.Background())
	require.NoError(t, err)

	require.NoError(t, stream.Send(&pd.Request{
		Open: &pd.Request_Open{
			Collection: collection,
			Version:    "fixture",
			Range: &pf.RangeSpec{
				KeyEnd:    math.MaxUint32,
				RClockEnd: math.MaxUint32,
			},
			StateJson: []byte("{}"),
		},
		Internal: pr.ToInternal(&pr.DeriveRequestExt{
			LogLevel: ops.Log_debug,
		}),
	}))

	opened, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, opened.Opened)

	// Send some "read" documents.
	for _, doc := range []string{
		`{"a_key":"key","a_val":2}`,
		`{"a_key":"key","a_val":2}`, // Repeat.
		`{"a_key":"key","a_val":2}`, // Repeat.
		`{"a_key":"key","a_val":3}`,
		`{"a_key":"key","a_val":1}`,
		`{"a_key":"key","a_val":2}`, // Repeat.
	} {
		require.NoError(t, stream.Send(&pd.Request{
			Read: &pd.Request_Read{
				Transform: 0,
				DocJson:   []byte(doc),
			},
		}))
	}

	// Flush our pipeline.
	require.NoError(t, stream.Send(&pd.Request{Flush: &pd.Request_Flush{}}))

	// Expect to read Published documents.
	var published []string
	for {
		response, err := stream.Recv()
		require.NoError(t, err)

		if response.Published != nil {
			published = append(published, string(response.Published.DocJson))
		} else if response.Flushed != nil {
			break
		}
	}

	// Start to commit.
	require.NoError(t, stream.Send(&pd.Request{StartCommit: &pd.Request_StartCommit{
		RuntimeCheckpoint: &pc.Checkpoint{
			Sources: map[pb.Journal]pc.Checkpoint_Source{
				"a/journal": {ReadThrough: 123},
			},
		},
	}}))
	startedCommit, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, startedCommit.StartedCommit)

	// Send Close, and expect to read EOF.
	stream.CloseSend()
	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)

	svc.Drop()

	cupaloy.SnapshotT(t, published)
	require.NotEmpty(t, publisher.logs)
}

type testPublisher struct {
	t    *testing.T
	logs int
}

var _ ops.Publisher = &testPublisher{}

func (p *testPublisher) PublishLog(log ops.Log) {
	logStr, err := (&jsonpb.Marshaler{}).MarshalToString(&log)
	require.NoError(p.t, err)
	p.t.Log(logStr)
	p.logs += 1
}

func (p *testPublisher) Labels() ops.ShardLabeling {
	return ops.ShardLabeling{
		TaskName: "some/task",
	}
}
