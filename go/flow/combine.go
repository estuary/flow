package flow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	pf "github.com/estuary/flow/go/protocol"
	"google.golang.org/grpc"
)

// Combine manages the lifecycle of a combine RPC.
type Combine struct {
	rpc interface {
		Send(*pf.CombineRequest) error
		CloseSend() error
		Recv() (*pf.CombineResponse, error)
	}
	next pf.CombineRequest_Continue
	spec *pf.CollectionSpec
}

// NewCombine begins a new Combine RPC.
func NewCombine(ctx context.Context, conn *grpc.ClientConn, spec *pf.CollectionSpec) (*Combine, error) {
	var stream, err = pf.NewCombineClient(conn).Combine(ctx)
	if err != nil {
		return nil, fmt.Errorf("staring Combine RPC: %w", err)
	}
	return &Combine{
		rpc: stream,
		next: pf.CombineRequest_Continue{
			Arena:    make(pf.Arena, 0, 4096),
			DocsJson: make([]pf.Slice, 0, 16),
		},
		spec: spec,
	}, nil
}

// Open the RPC with an request Open message. Must be called before Add.
func (c *Combine) Open(extractPtrs []string) error {
	if err := c.rpc.Send(&pf.CombineRequest{
		Kind: &pf.CombineRequest_Open_{Open: &pf.CombineRequest_Open{
			SchemaUri:          c.spec.SchemaUri,
			KeyPtr:             c.spec.KeyPtrs,
			FieldPtrs:          extractPtrs,
			UuidPlaceholderPtr: c.spec.UuidPtr,
		}},
	}); err != nil {
		return fmt.Errorf("sending CombineRequest_Open: %w", err)
	}
	return nil
}

// Add |doc| to be Combined.
func (c *Combine) Add(doc json.RawMessage) error {
	c.next.DocsJson = append(c.next.DocsJson, c.next.Arena.Add(doc))

	if len(c.next.Arena) > combineArenaThreshold {
		return c.Flush()
	}
	return nil
}

// Flush queued documents which have yet to be submitted to the RPC.
func (c *Combine) Flush() error {
	if len(c.next.DocsJson) == 0 {
		return nil // No-op.
	}
	var msg = &pf.CombineRequest{
		Kind: &pf.CombineRequest_Continue_{Continue: &c.next},
	}
	if err := c.rpc.Send(msg); err != nil {
		// On stream breaks gRPC returns io.EOF as the Send error,
		// and a far more informative Recv error.
		if _, recvErr := c.rpc.Recv(); recvErr != nil {
			err = recvErr
		}
		return fmt.Errorf("sending CombineRequest_Continue: %w", err)
	}
	// Clear for re-use.
	c.next = pf.CombineRequest_Continue{
		Arena:    c.next.Arena[:0],
		DocsJson: c.next.DocsJson[:0],
	}
	return nil
}

// Finish the ingestion.
func (c *Combine) Finish(cb func(pf.IndexedCombineResponse) error) error {
	if err := c.rpc.CloseSend(); err != nil {
		if _, recvErr := c.rpc.Recv(); recvErr != nil {
			err = recvErr
		}
		return fmt.Errorf("closing Combine RPC: %w", err)
	}

	for {
		var combined, err = c.rpc.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("reading CombineResponse: %w", err)
		}

		var icr = pf.IndexedCombineResponse{
			CombineResponse: combined,
			Index:           0,
			Collection:      c.spec,
		}
		for ; icr.Index != len(icr.DocsJson); icr.Index++ {
			if err := cb(icr); err != nil {
				return err
			}
		}
	}
}

var (
	combineArenaThreshold = 1 << 18 // 256K.
)
