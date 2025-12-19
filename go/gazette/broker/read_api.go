package broker

import (
	"context"
	"io"
	"net"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Read dispatches the JournalServer.Read API.
func (svc *Service) Read(claims pb.Claims, req *pb.ReadRequest, stream pb.Journal_ReadServer) (err error) {
	var resolved *resolution
	defer instrumentJournalServerRPC("Read", &err, &resolved)()

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(stream.Context()); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served Read RPC failed")
		}
	}()

	if err = req.Validate(); err != nil {
		return err
	}

	resolved, err = svc.resolver.resolve(stream.Context(), claims, req.Journal, resolveOpts{
		mayProxy:       !req.DoNotProxy,
		requirePrimary: false,
		proxyHeader:    req.Header,
	})

	if err != nil {
		return err
	} else if resolved.status != pb.Status_OK {
		return stream.Send(&pb.ReadResponse{Status: resolved.status, Header: &resolved.Header})
	} else if !resolved.journalSpec.Flags.MayRead() {
		return stream.Send(&pb.ReadResponse{Status: pb.Status_NOT_ALLOWED, Header: &resolved.Header})
	} else if resolved.ProcessId != resolved.localID {
		req.Header = &resolved.Header // Attach resolved Header to |req|, which we'll forward.
		return proxyRead(stream, req, svc.jc, svc.stopProxyReadsCh)
	}

	return pb.SuppressCancellationError(serveRead(stream, req, &resolved.Header, resolved.replica.index))
}

// proxyRead forwards a ReadRequest to a resolved peer broker.
func proxyRead(stream grpc.ServerStream, req *pb.ReadRequest, jc pb.JournalClient, stopCh <-chan struct{}) error {
	// We verified the client's authorization and are running under its context.
	// pb.AuthJournalClient will self-sign claims to proxy this journal on the client's behalf.
	var ctx, cancel = context.WithCancel(stream.Context())
	ctx = pb.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId)

	defer cancel()

	// We use the |stream| context for this RPC, which means a cancellation from
	// our client automatically propagates to the proxy |client| stream.
	var client, err = jc.Read(ctx, req)
	if err != nil {
		return err
	}
	var chunkCh = make(chan proxyChunk, 8)

	// Start a "pump" of |client| reads that we'll select from.
	go func() {
		for {
			var resp, err = client.Recv()
			chunkCh <- proxyChunk{resp: resp, err: err}

			if err != nil {
				return
			}
		}
	}()

	// Read and proxy chunks from `client`.
	// Cancel the proxy RPC when `stopCh` is signaled.
	var chunk proxyChunk
	for {
		select {
		case chunk = <-chunkCh:
			if chunk.err == nil {
				_ = stream.SendMsg(chunk.resp)
			} else if chunk.err == io.EOF || ctx.Err() != nil {
				return nil
			} else if chunk.err != nil {
				return chunk.err
			}
		case <-stopCh:
			stopCh = nil
			cancel()
		}
	}
}

type proxyChunk struct {
	resp *pb.ReadResponse
	err  error
}

// serveRead evaluates a client's Read RPC against the local replica index.
func serveRead(stream grpc.ServerStream, req *pb.ReadRequest, hdr *pb.Header, index *fragment.Index) error {
	var (
		buffer []byte // allocated on first use
		ctx    = stream.Context()
		reader io.ReadCloser
	)

	for i := 0; true; i++ {
		var resp, file, err = index.Query(ctx, req)
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

		// Return after sending Metadata if the Fragment query failed, or we
		// were only asked to send metadata, or the Fragment is remote and we're
		// instructed to not proxy, or if we resolved to an offset beyond the
		// requested EndOffset.
		if resp.Status != pb.Status_OK ||
			req.MetadataOnly ||
			file == nil && req.DoNotProxy ||
			req.EndOffset != 0 && resp.Offset >= req.EndOffset {
			return nil
		}
		// Note Query may have resolved or updated req.Offset. For the remainder of
		// this iteration, we update |req.Offset| to reference the next byte to read.
		req.Offset = resp.Offset

		if file != nil {
			reader = io.NopCloser(io.NewSectionReader(
				file, req.Offset-resp.Fragment.Begin, resp.Fragment.End-req.Offset))
		} else {
			if reader, err = fragment.Open(ctx, *resp.Fragment); err != nil {
				return err
			} else if reader, err = client.NewFragmentReader(reader, *resp.Fragment, req.Offset); err != nil {
				return err
			}
		}

		// Loop over chunks read from |reader|, sending each to the client.
		var n int
		var readErr error
		if buffer == nil {
			buffer = make([]byte, chunkSize)
		}

		for readErr == nil {
			if n, readErr = reader.Read(buffer); n == 0 {
				continue
			}
			if req.EndOffset != 0 && req.EndOffset-req.Offset <= int64(n) {
				// Send final chunk to EndOffset, then stop.
				n = int(req.EndOffset - req.Offset)
				readErr = io.EOF
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
		} else if req.EndOffset != 0 && req.Offset == req.EndOffset {
			return nil
		}

		// Loop to query and read the next Fragment.
	}
	return nil
}

var chunkSize = 1 << 17 // 128K.
