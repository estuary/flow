package gazette

import (
	"io"
	"net/http"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"

	"github.com/pippio/gazette/journal"
)

type ReplicateAPI struct {
	handler ReplicateOpHandler
	decoder *schema.Decoder
}

func NewReplicateAPI(handler ReplicateOpHandler) *ReplicateAPI {
	decoder := schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)
	decoder.SetAliasTag("json")

	return &ReplicateAPI{handler: handler, decoder: decoder}
}

func (h *ReplicateAPI) Register(router *mux.Router) {
	router.NewRoute().Methods("REPLICATE").HandlerFunc(h.Replicate)
}

func (h *ReplicateAPI) Replicate(w http.ResponseWriter, r *http.Request) {
	var schema struct {
		WriteHead  int64
		RouteToken string
		NewSpool   bool
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else if err = h.decoder.Decode(&schema, r.Form); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var op = journal.ReplicateOp{
		ReplicateArgs: journal.ReplicateArgs{
			Journal:    journal.Name(r.URL.Path[1:]),
			RouteToken: schema.RouteToken,
			WriteHead:  schema.WriteHead,
			NewSpool:   schema.NewSpool,
		},
		Result: make(chan journal.ReplicateResult, 1),
	}

	h.handler.Replicate(op)
	result := <-op.Result

	if result.Error != nil {
		if result.ErrorWriteHead != 0 {
			w.Header().Add(WriteHeadHeader,
				strconv.FormatInt(result.ErrorWriteHead, 16))
		}
		http.Error(w, result.Error.Error(), http.StatusBadRequest)
		return
	}
	var err error
	var commitDelta int64

	if _, err = io.Copy(result.Writer, r.Body); err != nil {
		result.Writer.Commit(0) // Abort.
	} else if commitDelta, err = strconv.ParseInt(
		r.Trailer.Get(CommitDeltaHeader), 16, 64); err != nil {
		result.Writer.Commit(0) // Abort.
	} else if err = result.Writer.Commit(commitDelta); err != nil {
	} else {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	log.WithField("err", err).Error("failed to commit transaction")
	http.Error(w, err.Error(), http.StatusBadRequest)
	return
}
