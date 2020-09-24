package flow

import (
	"context"
	"fmt"
	"io"

	pf "github.com/estuary/flow/go/protocol"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
	"google.golang.org/grpc"
)

// Transaction manages the consumer life-cycle of a Derive RPC.
type Transaction struct {
	derivation *pf.CollectionSpec
	// Mapping function used to publish derived documents.
	mapFn message.MappingFunc

	// Tx is the subset of grpc.ClientStream which we require,
	// described as an interface for easy mocking.
	tx interface {
		Send(*pf.DeriveRequest) error
		CloseSend() error
	}
	// Rx is a channel of reads from the grpc.ClientStream.
	rx <-chan deriveResponseOrError
	// Next request Continue we'll send, which is in the process of being built.
	next pf.DeriveRequest_Continue
	// Number of request Continues we've sent that remain un-acked.
	flighted int
}

type deriveResponseOrError struct {
	pf.DeriveResponse
	err error
}

// NewTransaction starts a Derive RPC of |derivation| on the ClientConn, and returns a Transaction.
func NewTransaction(
	ctx context.Context,
	conn *grpc.ClientConn,
	derivation *pf.CollectionSpec,
	mapFn message.MappingFunc,
) (*Transaction, error) {
	var stream, err = pf.NewDeriveClient(conn).Derive(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start a derive-worker transaction: %w", err)
	}
	var readCh = make(chan deriveResponseOrError, 1)

	go func(rx pf.Derive_DeriveClient, ch chan<- deriveResponseOrError) {
		for {
			var out deriveResponseOrError
			out.err = rx.RecvMsg(&out.DeriveResponse)
			if ch <- out; out.err != nil {
				return
			}
		}
	}(stream, readCh)

	return &Transaction{
		derivation: derivation,
		mapFn:      mapFn,
		tx:         stream,
		rx:         readCh,
		next:       pf.DeriveRequest_Continue{},
		flighted:   0,
	}, nil
}

// Open the RPC with an request Open message. Must be called before ConsumeMessage.
func (txn *Transaction) Open() error {
	if err := txn.tx.Send(&pf.DeriveRequest{
		Kind: &pf.DeriveRequest_Open_{Open: &pf.DeriveRequest_Open{
			Collection: txn.derivation.Name,
		}},
	}); err != nil {
		return fmt.Errorf("sending DeriveRequest_Open: %w", err)
	}
	return nil
}

// ConsumeMessage is delegated to from the application's ConsumeMessage callback.
func (txn *Transaction) ConsumeMessage(_ consumer.Shard, env message.Envelope, _ *message.Publisher) error {
	var doc = env.Message.(pf.IndexedShuffleResponse)
	var flags = message.GetFlags(doc.GetUUID())

	if flags != message.Flag_ACK_TXN {
		queueContinue(doc, &txn.next)
	}

	// If |pending| is under-threshold and this message is a continuation of an append
	// transaction (e.g., another message or ACK is forthcoming), then don't attempt to
	// send a derive continuation just yet.
	if overThreshold := len(txn.next.Arena) > txnArenaThreshold; overThreshold || flags != message.Flag_CONTINUE_TXN {
		return txn.pollContinue(overThreshold) // Must block if |overThreshold|.
	}
	return nil
}

func (txn *Transaction) pollContinue(mustSend bool) error {
	var respOk bool
	var resp deriveResponseOrError

	if mustSend && txn.flighted == txnMaxFlighted {
		resp, respOk = <-txn.rx, true // Block for next DeriveResponse_Continue.
	} else {
		select {
		case resp = <-txn.rx:
			respOk = true
		default: // Don't block.
		}
	}

	if respOk {
		if _, err := unwrapContinue(resp); err != nil {
			return err
		}
		txn.flighted--
	}

	if len(txn.next.DocsJson) != 0 && txn.flighted != txnMaxFlighted {
		if err := txn.tx.Send(&pf.DeriveRequest{
			Kind: &pf.DeriveRequest_Continue_{
				Continue: &txn.next,
			},
		}); err != nil {
			return fmt.Errorf("sending DeriveRequest_Continue: %w", err)
		}
		// Clear |pending| for re-use.
		txn.next = pf.DeriveRequest_Continue{
			Arena:       txn.next.Arena[:0],
			DocsJson:    txn.next.DocsJson[:0],
			UuidParts:   txn.next.UuidParts[:0],
			PackedKey:   txn.next.PackedKey[:0],
			TransformId: txn.next.TransformId[:0],
		}
		txn.flighted++
	}
	return nil
}

// FinalizeTxn is delegated to from the application's FinalizeTxn callback.
func (txn *Transaction) FinalizeTxn(_ consumer.Shard, pub *message.Publisher) error {
	// Flush final Continue message remainder.
	if err := txn.pollContinue(true); err != nil {
		return fmt.Errorf("sending final DeriveRequest_Continue: %w", err)
	}

	// Send Flush to signal all documents of the transaction have been sent,
	// and the worker should send back combined responses.
	if err := txn.tx.Send(&pf.DeriveRequest{
		Kind: &pf.DeriveRequest_Flush_{Flush: &pf.DeriveRequest_Flush{
			UuidPlaceholderPtr: txn.derivation.UuidPtr,
			FieldPtrs:          FieldPointersForMapper(txn.derivation),
		}},
	}); err != nil {
		return fmt.Errorf("sending DeriveRequest_Flush: %w", err)
	}

	for ; txn.flighted != 0; txn.flighted-- {
		if _, err := unwrapContinue(<-txn.rx); err != nil {
			return fmt.Errorf("draining remaining continuations: %w", err)
		}
	}

	for {
		var combined, err = unwrapFlush(<-txn.rx)
		if err != nil {
			return fmt.Errorf("finalize: %w", err)
		}

		var icr = pf.IndexedCombineResponse{
			CombineResponse: combined,
			Index:           0,
			Collection:      txn.derivation,
		}
		for ; icr.Index != len(icr.DocsJson); icr.Index++ {
			if _, err = pub.PublishUncommitted(txn.mapFn, icr); err != nil {
				return fmt.Errorf("publishing CombineResponse: %w", err)
			}
		}

		if icr.Index == 0 {
			break // CombineResponse with no documents signals end-of-flush.
		}
	}
	return nil
}

// StartCommit is delegated to from the store's StartCommit callback.
func (txn *Transaction) StartCommit(_ consumer.Shard, checkpoint pc.Checkpoint, waitFor client.OpFutures) consumer.OpFuture {
	if err := txn.tx.Send(&pf.DeriveRequest{
		Kind: &pf.DeriveRequest_Prepare_{
			Prepare: &pf.DeriveRequest_Prepare{
				Checkpoint: checkpoint,
			},
		},
	}); err != nil {
		return client.FinishedOperation(
			fmt.Errorf("sending DeriveRequest_Prepare: %w", err))
	}

	// Build a future to return now, that we'll resolve later
	// when the commit has finished (or failed).
	var future = client.NewAsyncOperation()

	// Asynchronously:
	// - Wait for |waitFor| to resolve.
	// - Close our side, which signals derive-worker to un-gate it's prepared recovery log write, committing the transaction.
	// - Notify |future| of failure or success of the commit.
	go func() {
		var err error
		for op := range waitFor {
			if err = op.Err(); err != nil {
				future.Resolve(err)
				return
			}
		}

		if err = txn.tx.CloseSend(); err != nil {
			future.Resolve(fmt.Errorf("failed to close prepared transaction: %w", err))
		} else if resp := <-txn.rx; resp.err != io.EOF {
			future.Resolve(fmt.Errorf("expected derive-worker EOF, not: %s, %w", resp.DeriveResponse.String(), resp.err))
		} else {
			future.Resolve(nil)
		}
	}()

	return future
}

func queueContinue(from pf.IndexedShuffleResponse, into *pf.DeriveRequest_Continue) {
	into.DocsJson = append(into.DocsJson, into.Arena.Add(
		from.Arena.Bytes(from.DocsJson[from.Index])))
	into.UuidParts = append(into.UuidParts, from.UuidParts[from.Index])
	into.PackedKey = append(into.PackedKey, into.Arena.Add(
		from.Arena.Bytes(from.PackedKey[from.Index])))
	into.TransformId = append(into.TransformId, from.Transform.CatalogDbId)
}

func unwrapContinue(resp deriveResponseOrError) (*pf.DeriveResponse_Continue, error) {
	if resp.err != nil {
		return nil, fmt.Errorf("reading DeriveResponse_Continue: %w", resp.err)
	} else if cont, ok := resp.DeriveResponse.Kind.(*pf.DeriveResponse_Continue_); ok {
		return cont.Continue, nil
	}
	return nil, fmt.Errorf("expected DeriveResponse_Continue, but received: %s", resp.DeriveResponse.String())
}

func unwrapFlush(resp deriveResponseOrError) (*pf.CombineResponse, error) {
	if resp.err != nil {
		return nil, fmt.Errorf("reading DeriveResponse_Flush: %w", resp.err)
	} else if flush, ok := resp.DeriveResponse.Kind.(*pf.DeriveResponse_Flush); ok {
		return flush.Flush, nil
	}
	return nil, fmt.Errorf("expected DeriveResponse_Flush, but received: %s", resp.DeriveResponse.String())
}

var (
	txnArenaThreshold = 1 << 19 // 524K.
	txnMaxFlighted    = 3
)
