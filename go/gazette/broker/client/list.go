package client

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
)

// WatchedList drives ongoing List RPCs with a given ListRequest, making
// its most recent result available via List. It's a building block for
// applications which interact with dynamic journal sets and wish to react
// to changes in their set membership over time.
//
//	var partitions, _ = protocol.ParseLabelSelector("logs=clicks, source=mobile")
//	var pl, err = NewPolledList(ctx, client, protocol.ListRequest{
//	    Selector: partitions,
//	})
type WatchedList struct {
	ctx      context.Context
	client   pb.JournalClient
	req      pb.ListRequest
	resp     atomic.Value
	updateCh chan error
}

// NewWatchedList returns a WatchedList of the ListRequest which will keep
// up-to-date with watched changes from brokers.
//
// Upon return, the WatchedList is not yet ready, but the caller may await
// UpdateCh() to be notified when the very first listing (or error) is available.
//
// If `updateCh` is non-nil, then it's used as UpdateCh() instead of allocating a
// new channel. This is recommended if you have a dynamic set of WatchedList
// instances and wish to take action if any one of them update. Otherwise,
// a new channel with a single buffered item is allocated.
func NewWatchedList(ctx context.Context, client pb.JournalClient, req pb.ListRequest, updateCh chan error) *WatchedList {
	if updateCh == nil {
		updateCh = make(chan error, 1)
	}

	req = pb.ListRequest{
		Selector: req.Selector,
		Watch:    true,
	}

	var pl = &WatchedList{
		ctx:      ctx,
		client:   client,
		req:      req,
		updateCh: updateCh,
	}

	go pl.watch()
	return pl
}

// List returns the most recent ListResponse snapshot, or nil if a snapshot has not been received yet.
func (pl *WatchedList) List() (out *pb.ListResponse) {
	out, _ = pl.resp.Load().(*pb.ListResponse)
	return
}

// UpdateCh returns a channel which is signaled with each update or error of the
// PolledList. Errors are informational only: WatchedList will retry on all errors,
// but the caller may wish to examine errors and cancel its context.
// Only one signal is sent per-update, so if multiple goroutines
// select from UpdateCh only one will wake.
func (pl *WatchedList) UpdateCh() <-chan error { return pl.updateCh }

func (pl *WatchedList) watch() {
	var (
		attempt int
		err     error
		resp    *pb.ListResponse
		stream  pb.Journal_ListClient
	)

	for {
		if stream == nil {
			stream, err = pl.client.List(pb.WithDispatchDefault(pl.ctx), &pl.req, grpc.WaitForReady(true))
		}
		if err == nil {
			resp, err = ReadListResponse(stream, pl.req)
		}

		if err == nil {
			pl.resp.Store(resp)
			pl.req.WatchResume = &resp.Header
			attempt = 0
		} else {
			stream = nil // Must restart.

			if err == io.EOF {
				attempt = 0 // Clean server-side close.
			}
			// Wait for back-off timer or context cancellation.
			select {
			case <-pl.ctx.Done():
				return
			case <-time.After(backoff(attempt)):
			}

			if attempt != 0 {
				log.WithFields(log.Fields{"err": err, "attempt": attempt, "req": pl.req.String()}).
					Warn("watched journal listing failed (will retry)")
			}
			attempt += 1
		}

		select {
		case pl.updateCh <- err:
		default: // Don't block if nobody's reading.
		}
	}
}

// ListAllJournals performs a unary broker journal listing.
// Any encountered error is returned.
func ListAllJournals(ctx context.Context, client pb.JournalClient, req pb.ListRequest) (*pb.ListResponse, error) {
	if req.Watch {
		panic("ListAllJournals cannot be used to Watch a listing")
	}
	var stream, err = client.List(pb.WithDispatchDefault(ctx), &req, grpc.WaitForReady(true))
	if err != nil {
		return nil, mapGRPCCtxErr(ctx, err)
	}

	if resp, err := ReadListResponse(stream, req); err != nil {
		return nil, err
	} else {
		if dr, ok := client.(pb.DispatchRouter); ok {
			for _, j := range resp.Journals {
				dr.UpdateRoute(j.Spec.Name.String(), &j.Route)
			}
		}
		return resp, nil
	}
}

// ReadListResponse reads a complete ListResponse snapshot from the stream.
func ReadListResponse(stream pb.Journal_ListClient, req pb.ListRequest) (*pb.ListResponse, error) {
	var resp, err = stream.Recv()

	if err != nil {
		return nil, mapGRPCCtxErr(stream.Context(), err)
	} else if err = resp.Validate(); err != nil {
		return resp, err
	} else if resp.Status != pb.Status_OK {
		return resp, errors.New(resp.Status.String())
	} else if req.WatchResume != nil && req.WatchResume.Etcd.Revision >= resp.Header.Etcd.Revision {
		return nil, fmt.Errorf("server sent unexpected ListResponse revision (%d; expected > %d)",
			resp.Header.Etcd.Revision, req.WatchResume.Etcd.Revision)
	}

	for {
		if next, err := stream.Recv(); err == io.EOF {
			if req.Watch {
				return nil, io.ErrUnexpectedEOF // Unexpected EOF of long-lived watch.
			} else {
				return resp, nil // Expected EOF of unary listing.
			}
		} else if err != nil {
			return nil, mapGRPCCtxErr(stream.Context(), err)
		} else if len(next.Journals) == 0 {
			if req.Watch {
				return resp, nil
			} else {
				return nil, fmt.Errorf("unexpected empty ListResponse.Journals in unary listing")
			}
		} else {
			resp.Journals = append(resp.Journals, next.Journals...)
		}
	}
}

// GetJournal retrieves the JournalSpec of the named Journal, or returns an error.
func GetJournal(ctx context.Context, jc pb.JournalClient, journal pb.Journal) (*pb.JournalSpec, error) {
	var lr, err = ListAllJournals(ctx, jc, pb.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.LabelSet{Labels: []pb.Label{{Name: "name", Value: journal.StripMeta().String()}}},
		},
	})
	if err == nil && len(lr.Journals) == 0 {
		err = errors.Errorf("named journal does not exist (%s)", journal)
	}
	if err != nil {
		return nil, err
	}
	return &lr.Journals[0].Spec, nil
}

// ApplyJournals applies journal changes detailed in the ApplyRequest via the broker Apply RPC.
// Changes are applied as a single Etcd transaction. If the change list is larger than an
// Etcd transaction can accommodate, ApplyJournalsInBatches should be used instead.
// ApplyResponse statuses other than OK are mapped to an error.
func ApplyJournals(ctx context.Context, jc pb.JournalClient, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
	return ApplyJournalsInBatches(ctx, jc, req, 0)
}

// ApplyJournalsInBatches is like ApplyJournals, but chunks the ApplyRequest
// into batches of the given size, which should be less than Etcd's maximum
// configured transaction size (usually 128). If size is 0 all changes will
// be attempted in a single transaction. Be aware that ApplyJournalsInBatches
// may only partially succeed, with some batches having applied and others not.
// The final ApplyResponse is returned, unless an error occurs.
// ApplyResponse statuses other than OK are mapped to an error.
func ApplyJournalsInBatches(ctx context.Context, jc pb.JournalClient, req *pb.ApplyRequest, size int) (*pb.ApplyResponse, error) {
	if size == 0 {
		size = len(req.Changes)
	}
	var offset = 0

	for {
		var r *pb.ApplyRequest
		if len(req.Changes[offset:]) > size {
			r = &pb.ApplyRequest{Changes: req.Changes[offset : offset+size]}
		} else {
			r = &pb.ApplyRequest{Changes: req.Changes[offset:]}
		}

		var resp, err = jc.Apply(pb.WithDispatchDefault(ctx), r, grpc.WaitForReady(true))
		if err != nil {
			return resp, err
		} else if err = resp.Validate(); err != nil {
			return resp, err
		} else if resp.Status != pb.Status_OK {
			return resp, errors.New(resp.Status.String())
		}

		if offset += len(r.Changes); offset == len(req.Changes) {
			return resp, nil
		}
	}
}

// ListAllFragments performs multiple Fragments RPCs, as required to join across multiple
// FragmentsResponse pages, and returns the completed FragmentResponse.
// Any encountered error is returned.
func ListAllFragments(ctx context.Context, client pb.RoutedJournalClient, req pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
	var resp *pb.FragmentsResponse
	var routedCtx = pb.WithDispatchItemRoute(ctx, client, req.Journal.String(), false)

	for {
		if r, err := client.ListFragments(routedCtx, &req); err != nil {
			return resp, mapGRPCCtxErr(ctx, err)
		} else if err = r.Validate(); err != nil {
			return resp, err
		} else if r.Status != pb.Status_OK {
			return resp, errors.New(r.Status.String())
		} else {
			req.NextPageToken, r.NextPageToken = r.NextPageToken, 0

			if resp == nil {
				resp = r
			} else {
				resp.Fragments = append(resp.Fragments, r.Fragments...)
			}
			if req.NextPageToken == 0 {
				break // All done.
			}
		}
	}
	return resp, nil
}
