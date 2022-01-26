package capture

import (
	"io"
	"testing"

	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestPullWrites(t *testing.T) {
	var stream = new(pullStream)
	var srv = &pullSrvStream{pullStream: stream}

	var staged *PullResponse
	require.NoError(t, StagePullDocuments(srv, &staged, 0, []byte(`"doc-1"`)))
	require.NoError(t, StagePullDocuments(srv, &staged, 1, []byte(`"doc-2"`)))
	require.NoError(t, StagePullDocuments(srv, &staged, 1, []byte(`"doc-3"`)))
	require.NoError(t, StagePullDocuments(srv, &staged, 2, []byte(`"doc-4"`)))
	require.NoError(t, WritePullCheckpoint(srv, &staged, makeCheckpoint(map[string]int{"a": 1})))
	require.NoError(t, WritePullCheckpoint(srv, &staged, makeCheckpoint(map[string]int{"b": 2})))

	require.Equal(t, []*PullResponse{
		{Documents: makeDocs(0, "doc-1")},
		{Documents: makeDocs(1, "doc-2", "doc-3")},
		{Documents: makeDocs(2, "doc-4")},
		{Checkpoint: makeCheckpoint(map[string]int{"a": 1})},
		{Checkpoint: makeCheckpoint(map[string]int{"b": 2})},
	}, stream.resp)
	require.Nil(t, staged)
}

func TestPushRoundTrip(t *testing.T) {
	var stream = new(pushStream)
	var cli = &pushClientStream{pushStream: stream}
	var srv = &pushSrvStream{pushStream: stream}

	// Write a sequence of documents with varying bindings, and checkpoints.
	var staged *PushRequest
	require.NoError(t, StagePushDocuments(cli, &staged, 0, []byte(`"doc-1"`)))
	require.NoError(t, StagePushDocuments(cli, &staged, 0, []byte(`"doc-2"`)))
	require.NoError(t, StagePushDocuments(cli, &staged, 1, []byte(`"doc-3"`)))
	require.NoError(t, WritePushCheckpoint(cli, &staged, makeCheckpoint(map[string]int{"a": 1})))
	require.NoError(t, StagePushDocuments(cli, &staged, 1, []byte(`"doc-4"`)))
	require.NoError(t, WritePushCheckpoint(cli, &staged, makeCheckpoint(map[string]int{"b": 2})))

	// Expect to read two separate checkpointed chunks.
	var docs, checkpoint, err = ReadPushCheckpoint(srv, 1024)
	require.NoError(t, err)

	require.Equal(t, []Documents{
		*makeDocs(0, "doc-1", "doc-2"), // Merged.
		*makeDocs(1, "doc-3"),
	}, docs)
	require.Equal(t, pf.DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"a":1}`),
		Rfc7396MergePatch:    true,
	}, checkpoint)

	docs, checkpoint, err = ReadPushCheckpoint(srv, 1024)
	require.NoError(t, err)

	require.Equal(t, []Documents{
		*makeDocs(1, "doc-4"),
	}, docs)
	require.Equal(t, pf.DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"b":2}`),
		Rfc7396MergePatch:    true,
	}, checkpoint)

	// Followed by EOF.
	_, _, err = ReadPushCheckpoint(srv, 1024)
	require.Equal(t, io.EOF, err)

	// Case: checkpoint is too large.
	require.NoError(t, StagePushDocuments(cli, &staged, 0, []byte(`"doc-5"`)))
	require.NoError(t, StagePushDocuments(cli, &staged, 0, []byte(`"doc-6"`)))
	cli.Send(staged)

	_, _, err = ReadPushCheckpoint(srv, 8)
	require.EqualError(t, err, "too many documents without a checkpoint (14 bytes vs max of 8)")

	// Case: Documents, then without a checkpoint.
	*stream, staged = pushStream{}, nil // Reset.
	require.NoError(t, StagePushDocuments(cli, &staged, 0, []byte(`"doc-7"`)))
	require.NoError(t, StagePushDocuments(cli, &staged, 0, []byte(`"doc-8"`)))
	cli.Send(staged)

	_, _, err = ReadPushCheckpoint(srv, 1024)
	require.Equal(t, io.ErrUnexpectedEOF, err)
}

type pullStream struct {
	req  []*PullRequest
	resp []*PullResponse
}
type pushStream struct {
	req  []*PushRequest
	resp []*PushResponse
}

type pullClientStream struct{ *pullStream }
type pullSrvStream struct{ *pullStream }

type pushClientStream struct{ *pushStream }
type pushSrvStream struct{ *pushStream }

func (s *pullClientStream) Send(r *PullRequest) error {
	s.req = append(s.req, r)
	return nil
}
func (s *pushClientStream) Send(r *PushRequest) error {
	s.req = append(s.req, r)
	return nil
}
func (s *pullSrvStream) Send(r *PullResponse) error {
	s.resp = append(s.resp, r)
	return nil
}
func (s *pushSrvStream) Send(r *PushResponse) error {
	s.resp = append(s.resp, r)
	return nil
}

func (s *pushSrvStream) Recv() (*PushRequest, error) {
	if len(s.req) == 0 {
		return nil, io.EOF
	}

	var r = s.req[0]
	s.req = s.req[1:]
	return r, nil
}
