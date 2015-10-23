package gazette

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"

	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/gazette/journal"
)

type ReadAPI struct {
	cfs     cloudstore.FileSystem
	decoder *schema.Decoder
	handler ReadOpHandler
}

func NewReadAPI(handler ReadOpHandler, cfs cloudstore.FileSystem) *ReadAPI {
	decoder := schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)
	decoder.SetAliasTag("json")

	return &ReadAPI{handler: handler, cfs: cfs, decoder: decoder}
}

func (h *ReadAPI) Register(router *mux.Router) {
	router.NewRoute().Methods("HEAD").HandlerFunc(h.Head)
	router.NewRoute().Methods("GET").HandlerFunc(h.Read)
}

func (h *ReadAPI) Head(w http.ResponseWriter, r *http.Request) {
	op, result := h.initialRead(w, r)

	if result.Error != nil && result.Error != journal.ErrNotYetAvailable {
		log.WithFields(log.Fields{"err": result.Error, "ReadOp": op}).Warn("head failed")
	}
}

func (h *ReadAPI) Read(w http.ResponseWriter, r *http.Request) {
	op, result := h.initialRead(w, r)

	// Loop performing incremental reads and copying to the client. If we fail
	// here, we log and just drop the connection (since we've already written
	// response headers).
	for {
		if result.Error != nil {
			log.WithFields(log.Fields{"err": result.Error, "ReadOp": op}).
				Warn("read failed")
			break
		}

		var reader io.Reader
		reader, err := result.Fragment.ReaderFromOffset(result.Offset, h.cfs)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "ReadOp": op}).
				Warn("failed to get a fragment reader")
			break
		}

		delta, err := io.Copy(w, reader)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "ReadOp": op}).
				Warn("failed to copy to client")
			break
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		op.Offset = result.Offset + delta

		// Next incremental read.
		h.handler.Read(op)
		result = <-op.Result
	}
}

func (h *ReadAPI) initialRead(w http.ResponseWriter, r *http.Request) (journal.ReadOp,
	journal.ReadResult) {

	var schema struct {
		Offset int64 // Required.
		Block  bool
	}
	var op journal.ReadOp
	var result journal.ReadResult

	if result.Error = r.ParseForm(); result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusBadRequest)
		return op, result
	} else if result.Error = h.decoder.Decode(&schema, r.Form); result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusBadRequest)
		return op, result
	}

	op = journal.ReadOp{
		ReadArgs: journal.ReadArgs{
			Journal:  journal.Name(r.URL.Path[1:]),
			Offset:   schema.Offset,
			Blocking: false,
		},
		Result: make(chan journal.ReadResult, 1),
	}
	// Perform an initial non-blocking read to test for request legality.
	h.handler.Read(op)
	result = <-op.Result

	if result.Error != nil {
		// Return a 302 redirect on a routing error.
		if routeError, ok := result.Error.(RouteError); ok {
			http.Redirect(w, r, routeError.RerouteURL(r.URL).String(), http.StatusTemporaryRedirect)
			return op, result
		}
		// Fail now if we encountered an error other than ErrNotYetAvailable,
		// or we saw ErrNotYetAvailable for a non-blocking read.
		if schema.Block == false || result.Error != journal.ErrNotYetAvailable {
			http.Error(w, result.Error.Error(), ResponseCodeForError(result.Error))
			return op, result
		}
	}
	op.Blocking = schema.Block // Switch to requested blocking mode.

	// Respond via HTTP 206 (Partial Content), as an effectively infinite-length
	// bytestream beginning at |result.Offset|.
	w.Header().Add("Content-Range", fmt.Sprintf("bytes %v-%v/%v", result.Offset,
		math.MaxInt64, math.MaxInt64))
	// Informational: Add the current write head.
	w.Header().Add(WriteHeadHeader, strconv.FormatInt(result.WriteHead, 10))

	if result.Error == nil {
		// Include the fragment's content-name (begin offset, end, and sha-sum).
		w.Header().Add(FragmentNameHeader, result.Fragment.ContentName())
		// If this is a remote fragment, also include a signed URL for direct access.
		// This allows the client to abort this request (or better: use HEAD first),
		// and then directly fetch content from cloud storage.
		if !result.Fragment.IsLocal() {
			url, err := result.Fragment.AsDirectURL(h.cfs)

			if err == nil {
				w.Header().Add(FragmentLocationHeader, url.String())
			} else {
				log.WithFields(log.Fields{"err": err, "fragment": result.Fragment}).
					Warn("failed to generate remote URL")
			}
		}
	}
	w.WriteHeader(http.StatusPartialContent)

	if result.Error == journal.ErrNotYetAvailable {
		// We must wait for a blocking read to complete. Flush headers,
		// as it may be a while before data is otherwise available.
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Retry, actually blocking this time.
		h.handler.Read(op)
		result = <-op.Result
	}
	return op, result
}
