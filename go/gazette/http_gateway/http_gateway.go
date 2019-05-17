package http_gateway

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/gazette/gazette/v2/client"
	pb "github.com/gazette/gazette/v2/protocol"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
)

// Gateway presents an HTTP gateway to Gazette brokers, by mapping GET, HEAD,
// and PUT requests into equivalent Read RPCs and Append RPCs.
type Gateway struct {
	decoder *schema.Decoder
	client  pb.RoutedJournalClient
}

// NewGateway returns a Gateway using the BrokerClient.
func NewGateway(client pb.RoutedJournalClient) *Gateway {
	var decoder = schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)

	return &Gateway{
		decoder: decoder,
		client:  client,
	}
}

func (h *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET", "HEAD":
		h.serveRead(w, r)
	case "PUT":
		h.serveWrite(w, r)
	default:
		http.Error(w, fmt.Sprintf("unknown method: %s", r.Method), http.StatusBadRequest)
	}
}

func (h *Gateway) serveRead(w http.ResponseWriter, r *http.Request) {
	var req, err = h.parseReadRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var reader = client.NewReader(r.Context(), h.client, req)
	if _, err = reader.Read(nil); err == client.ErrOffsetJump {
		// Swallow this error, as the client is notified via the Content-Range
		// header and we can continue the read. Any future jump after this one
		// will necessarily terminate the stream, forcing the client to retry.
	} else if reader.Response.Status != pb.Status_OK {
		// Fallthrough to return this status error as an HTTP status code.
	} else if r.Context().Err() != nil {
		// Request was aborted by client.
		http.Error(w, err.Error(), http.StatusRequestTimeout)
		return
	} else if err != nil {
		log.WithField("err", err).Warn("http_gateway: failed to proxy Read request")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeReadResponse(w, r, reader.Response)

	if reader.Response.Status != pb.Status_OK {
		return
	}
	if _, err = io.Copy(flushWriter{w}, reader); err == nil {
		err = errBrokerTerminated
	}
	w.Header().Set(CloseErrorHeader, err.Error())

	if r.Context().Err() != nil ||
		err == context.Canceled ||
		err == client.ErrOffsetJump ||
		err == client.ErrOffsetNotYetAvailable ||
		err == errBrokerTerminated {
		// Common & expected errors. Don't log.
	} else {
		log.WithField("err", err).Warn("http_gateway: failed to proxy Read response")
	}
}

func (h *Gateway) serveWrite(w http.ResponseWriter, r *http.Request) {
	var req, err = h.parseAppendRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var appender = client.NewAppender(r.Context(), h.client, req)
	if _, err = io.Copy(appender, r.Body); err == nil {
		err = appender.Close()
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		appender.Abort()
		return
	}
	writeAppendResponse(w, r, appender.Response)
}

func (h *Gateway) parseReadRequest(r *http.Request) (pb.ReadRequest, error) {
	var schema struct {
		Offset int64
		Block  bool
	}
	var q url.Values
	var err error

	if q, err = url.ParseQuery(r.URL.RawQuery); err == nil {
		err = h.decoder.Decode(&schema, q)
	}
	var req = pb.ReadRequest{
		Journal:      pb.Journal(r.URL.Path[1:]),
		Offset:       schema.Offset,
		Block:        schema.Block,
		MetadataOnly: r.Method == "HEAD",
	}
	if err == nil {
		err = req.Validate()
	}
	return req, err
}

func (h *Gateway) parseAppendRequest(r *http.Request) (pb.AppendRequest, error) {
	var schema struct{}
	var q url.Values
	var err error

	if q, err = url.ParseQuery(r.URL.RawQuery); err == nil {
		err = h.decoder.Decode(&schema, q)
	}
	var req = pb.AppendRequest{
		Journal: pb.Journal(r.URL.Path[1:]),
	}
	if err == nil {
		err = req.Validate()
	}
	return req, err
}

func writeReadResponse(w http.ResponseWriter, r *http.Request, resp pb.ReadResponse) {
	if resp.Header != nil {
		writeHeader(w, r, resp.Header)
	}
	if resp.Fragment != nil {
		w.Header().Add(FragmentNameHeader, resp.Fragment.ContentName())

		if resp.Fragment.ModTime != 0 {
			w.Header().Add(FragmentLastModifiedHeader, time.Unix(resp.Fragment.ModTime, 0).UTC().Format(http.TimeFormat))
		}
		if resp.FragmentUrl != "" {
			w.Header().Add(FragmentLocationHeader, resp.FragmentUrl)
		}
		w.Header().Add("Content-Range", fmt.Sprintf("bytes %v-%v/%v", resp.Offset,
			math.MaxInt64, math.MaxInt64))
	}
	if resp.WriteHead != 0 {
		w.Header().Add(WriteHeadHeader, strconv.FormatInt(resp.WriteHead, 10))
	}

	switch resp.Status {
	case pb.Status_OK:
		if r.Method == "GET" {
			// We must pre-declare our intent to send an X-Close-Error trailer.
			w.Header().Set("Trailer", CloseErrorHeader)
		}
		w.WriteHeader(http.StatusPartialContent) // 206.
	case pb.Status_JOURNAL_NOT_FOUND:
		http.Error(w, resp.Status.String(), http.StatusNotFound) // 404.
	case pb.Status_INSUFFICIENT_JOURNAL_BROKERS:
		http.Error(w, resp.Status.String(), http.StatusServiceUnavailable) // 503.
	case pb.Status_OFFSET_NOT_YET_AVAILABLE:
		http.Error(w, resp.Status.String(), http.StatusRequestedRangeNotSatisfiable) // 416.
	default:
		http.Error(w, resp.Status.String(), http.StatusInternalServerError) // 500.
	}
}

func writeAppendResponse(w http.ResponseWriter, r *http.Request, resp pb.AppendResponse) {
	writeHeader(w, r, &resp.Header)

	if resp.Commit != nil {
		w.Header().Add(CommitBeginHeader, strconv.FormatInt(resp.Commit.Begin, 10))
		w.Header().Add(CommitEndHeader, strconv.FormatInt(resp.Commit.End, 10))
		w.Header().Add(WriteHeadHeader, strconv.FormatInt(resp.Commit.End, 10))

		var digest = resp.Commit.Sum.ToDigest()
		w.Header().Add(CommitSumHeader, hex.EncodeToString(digest[:]))
	}

	switch resp.Status {
	case pb.Status_OK:
		w.WriteHeader(http.StatusNoContent) // 204.
	case pb.Status_JOURNAL_NOT_FOUND:
		w.WriteHeader(http.StatusNotFound) // 404.
	default:
		http.Error(w, resp.Status.String(), http.StatusInternalServerError) // 500.
	}
}

func writeHeader(w http.ResponseWriter, r *http.Request, h *pb.Header) {
	if len(h.Route.Members) != 0 {
		w.Header().Set(RouteTokenHeader, proto.CompactTextString(&h.Route))
	}
	// If there is a primary broker, add a Location header which would direct
	// further requests of this journal to the primary.
	if len(h.Route.Members) != 0 && h.Route.Primary != -1 {
		var loc = h.Route.Endpoints[h.Route.Primary].URL()

		loc.Path = path.Join(loc.Path, r.URL.Path)
		w.Header().Add("Location", loc.String())
	}
}

type flushWriter struct{ io.Writer }

func (fw flushWriter) Write(p []byte) (n int, err error) {
	n, err = fw.Writer.Write(p)

	if flusher, ok := fw.Writer.(http.Flusher); ok {
		flusher.Flush()
	}
	return
}

const (
	FragmentLastModifiedHeader = "X-Fragment-Last-Modified"
	FragmentLocationHeader     = "X-Fragment-Location"
	FragmentNameHeader         = "X-Fragment-Name"
	RouteTokenHeader           = "X-Route-Token"
	CloseErrorHeader           = "X-Close-Error"

	WriteHeadHeader   = "X-Write-Head"
	CommitBeginHeader = "X-Commit-Begin"
	CommitEndHeader   = "X-Commit-End"
	CommitSumHeader   = "X-Commit-SHA1-Sum"
)

var errBrokerTerminated = errors.New("broker terminated RPC")
