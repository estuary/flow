package broker

import (
	"context"
	"sort"
	"time"

	"github.com/gazette/gazette/v2/fragment"
	pb "github.com/gazette/gazette/v2/protocol"
)

var defaultPageLimit = int32(1000)

// ListFragments dispatches the JournalServer.ListFragments API.
func (svc *Service) ListFragments(ctx context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
	var err error
	defer instrumentJournalServerOp("list_fragments", &err, time.Now())

	if err = req.Validate(); err != nil {
		return nil, err
	}

	var res resolution
	res, err = svc.resolver.resolve(resolveArgs{
		ctx:                   ctx,
		journal:               req.Journal,
		mayProxy:              !req.DoNotProxy,
		requirePrimary:        false,
		requireFullAssignment: false,
		proxyHeader:           req.Header,
	})

	if err != nil {
		return nil, err
	} else if res.status != pb.Status_OK {
		return &pb.FragmentsResponse{Status: res.status, Header: res.Header}, err
	} else if !res.journalSpec.Flags.MayRead() {
		return &pb.FragmentsResponse{Status: pb.Status_NOT_ALLOWED, Header: res.Header}, err
	} else if res.replica == nil {
		req.Header = &res.Header // Attach resolved Header to |req|, which we'll forward.
		ctx = pb.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId)

		var resp *pb.FragmentsResponse
		resp, err = svc.jc.ListFragments(ctx, req)
		return resp, err
	}

	if req.PageLimit == 0 {
		req.PageLimit = int32(defaultPageLimit)
	}

	if err = res.replica.index.WaitForFirstRemoteRefresh(ctx); err != nil {
		err = pb.ExtendContext(err, "error waiting for index")
		return nil, err
	}

	var resp = &pb.FragmentsResponse{
		Status: pb.Status_OK,
		Header: res.Header,
	}
	if err = res.replica.index.Inspect(func(fragmentSet fragment.CoverSet) error {
		resp.Fragments, resp.NextPageToken, err = listFragments(req, fragmentSet)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return resp, nil
}

// List FragmentsResponse__Fragment matching the query, and return the
// NextPageToken to be used for subsequent requests. If NextPageToken is nil
// there are no further Fragments to enumerate.
func listFragments(req *pb.FragmentsRequest, set fragment.CoverSet) ([]pb.FragmentsResponse__Fragment, int64, error) {
	// Determine |next| offset within |set| at which we begin or continue enumeration.
	var next = sort.Search(len(set), func(i int) bool {
		return set[i].Begin >= req.NextPageToken
	})

	// TODO(johnny): Sanity check size before allocating?
	var out = make([]pb.FragmentsResponse__Fragment, 0, req.PageLimit)

	for ; next != len(set) && len(out) != cap(out); next++ {
		var f = set[next]

		// ModTime may be zero on the Fragment if it's local-only, and not yet
		// persisted to any store. We included these in the response iff
		// EndModTime is zero.
		if (f.ModTime != 0 && f.ModTime < req.BeginModTime) ||
			(req.EndModTime != 0 && (f.ModTime == 0 || f.ModTime > req.EndModTime)) {
			continue // Fragment is outside of the allowed time range.
		}

		var err error
		var frag = pb.FragmentsResponse__Fragment{Spec: f.Fragment}

		if req.SignatureTTL != nil && f.BackingStore != "" {
			if frag.SignedUrl, err = fragment.SignGetURL(frag.Spec, *req.SignatureTTL); err != nil {
				return nil, 0, err
			}
		}
		out = append(out, frag)
	}

	if next != len(set) {
		return out, set[next].Begin, nil
	}
	return out, 0, nil
}
