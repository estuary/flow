package materialize

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

//go:generate flowctl raw build --build-id test-build --db-path testdata/temp.db --source testdata/flow.yaml
//go:generate sqlite3 file:testdata/temp.db "SELECT WRITEFILE('testdata/materialization.proto', spec) FROM built_materializations WHERE materialization = 'test/sqlite';"

func TestStreamLifecycle(t *testing.T) {
	var specBytes, err = os.ReadFile("testdata/materialization.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	var stream = new(stream)
	var srvRPC = &srvStream{stream: stream}
	var cliRPC = &clientStream{stream: stream}

	txRequest, err := WriteOpen(cliRPC, &Request_Open{
		Materialization: &spec,
		Version:         "someVersion",
	})
	require.NoError(t, err)

	var rxRequest Request
	require.NoError(t, srvRPC.RecvMsg(&rxRequest))
	require.NotNil(t, rxRequest.Open)

	txResponse, err := WriteOpened(srvRPC, &Response_Opened{
		RuntimeCheckpoint: &pc.Checkpoint{
			Sources: map[pb.Journal]pc.Checkpoint_Source{"a/journal": {ReadThrough: 111}},
		},
	})
	require.NoError(t, err)

	rxResponse, err := ReadOpened(cliRPC)
	require.NoError(t, err)

	// Write Acknowledge and read Acknowledged.
	require.NoError(t, WriteAcknowledge(cliRPC, &txRequest))
	require.NoError(t, ReadAcknowledge(srvRPC, &rxRequest))

	require.NoError(t, WriteAcknowledged(srvRPC, &txResponse))
	require.NoError(t, ReadAcknowledged(cliRPC, &rxResponse))

	// Runtime sends multiple Loads, then Flush.
	require.NoError(t, WriteLoad(cliRPC, &txRequest, 0, tuple.Tuple{"key-1"}.Pack(), []byte("[1]")))
	require.NoError(t, WriteLoad(cliRPC, &txRequest, 1, tuple.Tuple{2}.Pack(), []byte("[2]")))
	require.NoError(t, WriteLoad(cliRPC, &txRequest, 1, tuple.Tuple{-3}.Pack(), []byte("[3]")))
	require.NoError(t, WriteLoad(cliRPC, &txRequest, 1, tuple.Tuple{"four"}.Pack(), []byte("[4]")))
	require.NoError(t, WriteLoad(cliRPC, &txRequest, 0, tuple.Tuple{[]byte("five")}.Pack(), []byte("[5]")))
	require.NoError(t, WriteFlush(cliRPC, &txRequest))

	// Driver reads Loads.
	var awaitDoneCh = make(chan struct{})
	close(awaitDoneCh)
	var it = &LoadIterator{stream: srvRPC, request: &rxRequest, awaitDoneCh: awaitDoneCh}
	require.True(t, it.Next())
	require.Equal(t, tuple.Tuple{"key-1"}, it.Key)
	require.True(t, it.Next())
	require.Equal(t, tuple.Tuple{int64(2)}, it.Key)
	require.True(t, it.Next())
	require.Equal(t, tuple.Tuple{int64(-3)}, it.Key)
	require.True(t, it.Next())
	require.Equal(t, tuple.Tuple{"four"}, it.Key)
	require.True(t, it.Next())
	require.Equal(t, tuple.Tuple{[]byte("five")}, it.Key)
	require.False(t, it.Next())
	require.Nil(t, it.Err())

	// Driver reads Flush, and responds with Loaded and then Flushed.
	require.NoError(t, ReadFlush(&rxRequest))
	require.NoError(t, WriteLoaded(srvRPC, &txResponse, 0, []byte(`loaded-1`)))
	require.NoError(t, WriteLoaded(srvRPC, &txResponse, 0, []byte(`loaded-2`)))
	require.NoError(t, WriteLoaded(srvRPC, &txResponse, 2, []byte(`loaded-3`)))
	require.NoError(t, WriteFlushed(srvRPC, &txResponse))

	// Runtime reads Loaded.
	loaded, err := ReadLoaded(cliRPC, &rxResponse)
	require.NoError(t, err)
	require.Equal(t, 0, int(loaded.Binding))
	require.Equal(t, "loaded-1", string(loaded.DocJson))
	loaded, err = ReadLoaded(cliRPC, &rxResponse)
	require.NoError(t, err)
	require.Equal(t, 0, int(loaded.Binding))
	require.Equal(t, "loaded-2", string(loaded.DocJson))
	loaded, err = ReadLoaded(cliRPC, &rxResponse)
	require.NoError(t, err)
	require.Equal(t, 2, int(loaded.Binding))
	require.Equal(t, "loaded-3", string(loaded.DocJson))
	loaded, err = ReadLoaded(cliRPC, &rxResponse)
	require.NoError(t, err)
	require.Nil(t, loaded) // Indicates end of Loaded responses.

	// Runtime reads Flushed.
	require.NoError(t, ReadFlushed(&rxResponse))

	// Runtime sends Store, then StartCommit with runtime checkpoint.
	require.NoError(t, WriteStore(cliRPC, &txRequest,
		0, tuple.Tuple{"key-1"}.Pack(), []byte("[1]"), tuple.Tuple{false}.Pack(), []byte("[11]"), []byte(`doc-1`), true))
	require.NoError(t, WriteStore(cliRPC, &txRequest,
		0, tuple.Tuple{"key", 2}.Pack(), []byte("[2]"), tuple.Tuple{"two"}.Pack(), []byte("[22]"), []byte(`doc-2`), false))
	require.NoError(t, WriteStore(cliRPC, &txRequest,
		1, tuple.Tuple{"three"}.Pack(), []byte("[3]"), tuple.Tuple{true}.Pack(), []byte("[33]"), []byte(`doc-3`), true))
	require.NoError(t, WriteStartCommit(cliRPC, &txRequest, &pc.Checkpoint{
		Sources: map[pb.Journal]pc.Checkpoint_Source{"a/journal": {ReadThrough: 222}},
	}))

	// Driver reads stores.
	var sit = &StoreIterator{stream: srvRPC, request: &rxRequest}
	require.True(t, sit.Next())
	require.Equal(t, 0, sit.Binding)
	require.Equal(t, tuple.Tuple{"key-1"}, sit.Key)
	require.Equal(t, tuple.Tuple{false}, sit.Values)
	require.Equal(t, []byte(`doc-1`), []byte(sit.RawJSON))
	require.Equal(t, true, sit.Exists)

	require.True(t, sit.Next())
	require.Equal(t, 0, sit.Binding)
	require.Equal(t, tuple.Tuple{"key", int64(2)}, sit.Key)
	require.Equal(t, tuple.Tuple{"two"}, sit.Values)
	require.Equal(t, []byte(`doc-2`), []byte(sit.RawJSON))
	require.Equal(t, false, sit.Exists)

	require.True(t, sit.Next())
	require.Equal(t, 1, sit.Binding)
	require.Equal(t, tuple.Tuple{"three"}, sit.Key)
	require.Equal(t, tuple.Tuple{true}, sit.Values)
	require.Equal(t, []byte(`doc-3`), []byte(sit.RawJSON))
	require.Equal(t, true, sit.Exists)

	require.False(t, sit.Next())
	require.Nil(t, sit.Err())

	// Driver reads StartCommit.
	runtimeCP, err := ReadStartCommit(&rxRequest)
	require.NoError(t, err)
	require.NotEmpty(t, runtimeCP)

	// Driver sends StartedCommit.
	require.NoError(t, WriteStartedCommit(srvRPC, &txResponse,
		&pf.ConnectorState{UpdatedJson: []byte(`checkpoint`)}))

	// Runtime reads StartedCommit.
	driverCP, err := ReadStartedCommit(cliRPC, &rxResponse)
	require.NoError(t, err)
	require.Equal(t, "checkpoint", string(driverCP.UpdatedJson))

	// Write Acknowledge and read Acknowledged.
	require.NoError(t, WriteAcknowledge(cliRPC, &txRequest))
	require.NoError(t, ReadAcknowledge(srvRPC, &rxRequest))

	require.NoError(t, WriteAcknowledged(srvRPC, &txResponse))
	require.NoError(t, ReadAcknowledged(cliRPC, &rxResponse))

	// Snapshot to verify driver responses.
	cupaloy.SnapshotT(t, stream.req, stream.resp)
}

type stream struct {
	reqInd  int
	req     []Request
	respInd int
	resp    []Response
}

func (s stream) Context() context.Context { return context.Background() }

type clientStream struct{ *stream }
type srvStream struct{ *stream }

func (s *clientStream) Send(r *Request) error {
	s.req = append(s.req, *r)
	return nil
}

func (s *srvStream) Send(r *Response) error {
	s.resp = append(s.resp, *r)
	return nil
}

func (s *clientStream) RecvMsg(out interface{}) error {
	if len(s.resp) == s.respInd {
		return io.EOF
	}

	*out.(*Response) = s.resp[s.respInd]
	s.respInd += 1
	return nil
}

func (s *srvStream) RecvMsg(out interface{}) error {
	if len(s.req) == s.reqInd {
		return io.EOF
	}

	*out.(*Request) = s.req[s.reqInd]
	s.reqInd += 1
	return nil
}
