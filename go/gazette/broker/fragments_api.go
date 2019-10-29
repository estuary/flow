package broker

import (
	"context"
	"net"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/peer"
)

var defaultPageLimit = int32(1000)

// ListFragments dispatches the JournalServer.ListFragments API.
func (svc *Service) ListFragments(ctx context.Context, req *pb.FragmentsRequest) (resp *pb.FragmentsResponse, err error) {
	var res *resolution
	defer instrumentJournalServerOp("ListFragments", &err, &res, time.Now())

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(ctx); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served ListFragments RPC failed")
		}
	}()

	if err = req.Validate(); err != nil {
		return nil, err
	}
	if req.PageLimit == 0 {
		req.PageLimit = defaultPageLimit
	}

	res, err = svc.resolver.resolve(resolveArgs{
		ctx:            ctx,
		journal:        req.Journal,
		mayProxy:       !req.DoNotProxy,
		requirePrimary: false,
		proxyHeader:    req.Header,
	})

	if err != nil {
		return nil, err
	} else if res.status != pb.Status_OK {
		return &pb.FragmentsResponse{Status: res.status, Header: res.Header}, nil
	} else if !res.journalSpec.Flags.MayRead() {
		return &pb.FragmentsResponse{Status: pb.Status_NOT_ALLOWED, Header: res.Header}, nil
	} else if res.replica == nil {
		req.Header = &res.Header // Attach resolved Header to |req|, which we'll forward.
		ctx = pb.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId)
		return svc.jc.ListFragments(ctx, req)
	}

	resp = &pb.FragmentsResponse{
		Status: pb.Status_OK,
		Header: res.Header,
	}
	err = res.replica.index.Inspect(ctx, func(fragmentSet fragment.CoverSet) error {
		resp.Fragments, resp.NextPageToken, err = listFragments(req, fragmentSet)
		return err
	})
	return resp, err
}

// List FragmentsResponse__Fragment matching the query, and return the
// NextPageToken to be used for subsequent requests. If NextPageToken is nil
// there are no further Fragments to enumerate.
func listFragments(req *pb.FragmentsRequest, set fragment.CoverSet) ([]pb.FragmentsResponse__Fragment, int64, error) {
	// Determine |next| offset within |set| at which we begin or continue enumeration.
	var next = sort.Search(len(set), func(i int) bool {
		return set[i].Begin >= req.NextPageToken
	})
	// Request validation sanity-checked that PageLimit isn't absurdly large.
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
