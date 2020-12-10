package ingest

import (
	"context"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func testGRPCSimple(t *testing.T, addr string) {
	// It should correctly handle extra newlines between messages.
	var valid = `
{"i": 32, "s": "hello"}

{"i": 42, "s": "world"}

`
	var conn, err = grpc.Dial(addr, grpc.WithInsecure())
	require.NoError(t, err)

	resp, err := pf.NewIngesterClient(conn).Ingest(context.Background(),
		&pf.IngestRequest{
			Collections: []pf.IngestRequest_Collection{
				{
					Name:          "testing/int-string",
					DocsJsonLines: []byte(valid),
				},
			}})
	require.NoError(t, err)

	require.NotEmpty(t, resp.JournalWriteHeads)
	require.NotZero(t, resp.JournalEtcd.Revision)
}

func testGRPCNotFound(t *testing.T, addr string) {
	// It should correctly handle extra newlines between messages.
	var valid = `
{"i": 32, "s": "hello"}
{"i": 42, "s": "world"}`
	var conn, err = grpc.Dial(addr, grpc.WithInsecure())
	require.NoError(t, err)

	_, err = pf.NewIngesterClient(conn).Ingest(context.Background(),
		&pf.IngestRequest{
			Collections: []pf.IngestRequest_Collection{
				{
					Name:          "not/found",
					DocsJsonLines: []byte(valid),
				},
			}})
	require.EqualError(t, err, `rpc error: code = Unknown desc = "not/found" is not an ingestable collection`)
}
