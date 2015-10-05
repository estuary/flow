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
)

type ReadAPI struct {
	dispatcher headAndReadDispatcher
	cfs        cloudstore.FileSystem
	decoder    *schema.Decoder
}

func NewReadAPI(dispatcher headAndReadDispatcher, cfs cloudstore.FileSystem) *ReadAPI {
	decoder := schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)
	decoder.SetAliasTag("json")

	return &ReadAPI{dispatcher: dispatcher, cfs: cfs, decoder: decoder}
}

func (h *ReadAPI) Register(router *mux.Router) {
	router.NewRoute().Methods("HEAD").HandlerFunc(h.Head)
	router.NewRoute().Methods("GET").HandlerFunc(h.Read)
}

func (h *ReadAPI) Head(w http.ResponseWriter, r *http.Request) {
	journal := r.URL.Path[1:]

	var schema struct {
		Offset int64 // Required.
		Block  bool
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else if err = h.decoder.Decode(&schema, r.Form); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeHead := h.dispatcher.WriteHead(journal)
	if writeHead == -1 {
		http.Error(w, ErrNotReplica.Error(), ResponseCodeForError(ErrNotReplica))
		return
	}
	if (schema.Offset == -1 || schema.Offset > writeHead) && !schema.Block {
		http.Error(w, ErrNotYetAvailable.Error(), ResponseCodeForError(ErrNotYetAvailable))
		return
	}
	if schema.Offset == -1 {
		schema.Offset = writeHead
	}

	w.Header().Add("Content-Range",
		fmt.Sprintf("bytes %v-%v/%v", schema.Offset, math.MaxInt64, math.MaxInt64))
	w.Header().Add(WriteHeadHeader, strconv.FormatInt(writeHead, 10))
	w.WriteHeader(http.StatusPartialContent)
}

func (h *ReadAPI) Read(w http.ResponseWriter, r *http.Request) {
	journal := r.URL.Path[1:]

	var schema struct {
		Offset int64 // Required.
		Block  bool
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else if err = h.decoder.Decode(&schema, r.Form); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var writeOffsetStr string
	if schema.Offset == -1 && schema.Block {
		schema.Offset = h.dispatcher.WriteHead(journal)
		writeOffsetStr = strconv.FormatInt(schema.Offset, 10)
	} else {
		offset := h.dispatcher.WriteHead(journal)
		writeOffsetStr = strconv.FormatInt(offset, 10)
	}

	var op = ReadOp{
		Journal:  journal,
		Offset:   schema.Offset,
		Blocking: false,
		Result:   make(chan ReadResult, 1),
	}
	// Perform an initial non-blocking read to test for request legality.
	h.dispatcher.DispatchRead(op)
	result := <-op.Result

	if result.Error != nil &&
		(schema.Block == false || result.Error != ErrNotYetAvailable) {
		http.Error(w, result.Error.Error(), ResponseCodeForError(result.Error))
		log.WithFields(log.Fields{"err": result.Error, "ReadOp": op}).
			Warn("initial read failed")
		return
	}
	op.Blocking = schema.Block // Switch to requested blocking mode.

	w.Header().Add("Content-Range",
		fmt.Sprintf("bytes %v-%v/%v", op.Offset, math.MaxInt64, math.MaxInt64))
	w.Header().Add(WriteHeadHeader, writeOffsetStr)
	w.WriteHeader(http.StatusPartialContent)

	if result.Error == ErrNotYetAvailable {
		// Flush headers, as it may be a while before data is otherwise available.
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Retry, actually blocking this time.
		h.dispatcher.DispatchRead(op)
		result = <-op.Result
	}

	// Loop performing incremental reads and copying to the client. If we fail
	// here, we log and just drop the connection (since we've already written
	// response headers).
	for {
		if result.Error != nil {
			log.WithFields(log.Fields{"err": result.Error, "ReadOp": op}).
				Warn("incremental read failed")
			break
		}

		var reader io.Reader
		reader, err := result.Fragment.ReaderFromOffset(op.Offset, h.cfs)
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
		op.Offset += delta

		// Next incremental read.
		h.dispatcher.DispatchRead(op)
		result = <-op.Result
	}
}

type headAndReadDispatcher interface {
	WriteHead(journal string) int64
	DispatchRead(op ReadOp)
}
