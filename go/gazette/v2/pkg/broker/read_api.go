package broker

import (
	"context"
	"io"
	"io/ioutil"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Read dispatches the JournalServer.Read API.
func (svc *Service) Read(req *pb.ReadRequest, stream pb.Journal_ReadServer) error {
	if err := req.Validate(); err != nil {
		return err
	}

	var res, err = svc.resolver.resolve(resolveArgs{
		ctx:                   stream.Context(),
		journal:               req.Journal,
		mayProxy:              !req.DoNotProxy,
		requirePrimary:        false,
		requireFullAssignment: false,
		proxyHeader:           req.Header,
	})

	if err != nil {
		return err
	} else if res.status != pb.Status_OK {
		return stream.Send(&pb.ReadResponse{Status: res.status, Header: &res.Header})
	} else if !res.journalSpec.Flags.MayRead() {
		return stream.Send(&pb.ReadResponse{Status: pb.Status_NOT_ALLOWED, Header: &res.Header})
	} else if res.replica == nil {
		req.Header = &res.Header // Attach resolved Header to |req|, which we'll forward.
		return proxyRead(stream, req, svc.jc)
	}

	if err = serveRead(stream, req, &res.Header, res.replica.index); err == context.Canceled {
		err = nil // Gracefully terminate RPC.
	} else if err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Read")
	}
	return err
}

// proxyRead forwards a ReadRequest to a resolved peer broker.
func proxyRead(stream grpc.ServerStream, req *pb.ReadRequest, jc pb.JournalClient) error {
	var ctx = pb.WithDispatchRoute(stream.Context(), req.Header.Route, req.Header.ProcessId)

	var client, err = jc.Read(ctx, req)
	if err != nil {
		return err
	} else if err = client.CloseSend(); err != nil {
		return err
	}

	var resp = new(pb.ReadResponse)

	for {
		if err = client.RecvMsg(resp); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if err = stream.SendMsg(resp); err != nil {
			return err
		}
	}
}

// serveRead evaluates a client's Read RPC against the local replica index.
func serveRead(stream grpc.ServerStream, req *pb.ReadRequest, hdr *pb.Header, index *fragment.Index) error {
	var buffer = make([]byte, chunkSize)
	var reader io.ReadCloser

	for i := 0; true; i++ {
		var resp, file, err = index.Query(stream.Context(), req)
		if err != nil {
			return err
		}

		// Send the Header with the first response message (only).
		if i == 0 {
			resp.Header = hdr
		}
		if err = stream.SendMsg(resp); err != nil {
			return err
		}

		// Return after sending Metadata if the Fragment query failed,
		// or we were only asked to send metadata, or the Fragment is
		// remote and we're instructed to not proxy.
		if resp.Status != pb.Status_OK || req.MetadataOnly || file == nil && req.DoNotProxy {
			return nil
		}
		// Note Query may have resolved or updated req.Offset. For the remainder of
		// this iteration, we update |req.Offset| to reference the next byte to read.
		req.Offset = resp.Offset

		if file != nil {
			reader = ioutil.NopCloser(io.NewSectionReader(
				file, req.Offset-resp.Fragment.Begin, resp.Fragment.End-req.Offset))
		} else {
			if reader, err = fragment.Open(stream.Context(), *resp.Fragment); err != nil {
				return err
			} else if reader, err = client.NewFragmentReader(reader, *resp.Fragment, req.Offset); err != nil {
				return err
			}
		}

		// Loop over chunks read from |reader|, sending each to the client.
		var n int
		var readErr error

		for readErr == nil {
			if n, readErr = reader.Read(buffer); n == 0 {
				continue
			}

			if err = stream.SendMsg(&pb.ReadResponse{
				Offset:  req.Offset,
				Content: buffer[:n],
			}); err != nil {
				return err
			}
			req.Offset += int64(n)
		}

		if readErr != io.EOF {
			return readErr
		} else if err = reader.Close(); err != nil {
			return err
		}

		// Loop to query and read the next Fragment.
	}
	return nil
}

var chunkSize = 1 << 17 // 128K.
