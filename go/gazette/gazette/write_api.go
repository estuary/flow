package gazette

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"github.com/LiveRamp/gazette/journal"
)

type WriteAPI struct {
	handler AppendOpHandler
}

func NewWriteAPI(handler AppendOpHandler) *WriteAPI {
	return &WriteAPI{handler: handler}
}

func (h *WriteAPI) Register(router *mux.Router) {
	router.NewRoute().Methods("PUT").HandlerFunc(h.Write)
}

func (h *WriteAPI) Write(w http.ResponseWriter, r *http.Request) {
	var op = journal.AppendOp{
		AppendArgs: journal.AppendArgs{
			Journal: journal.Name(r.URL.Path[1:]),
			Content: r.Body,
		},
		Result: make(chan journal.AppendResult, 1),
	}
	h.handler.Append(op)
	result := <-op.Result

	if result.WriteHead != 0 {
		w.Header().Set(WriteHeadHeader, strconv.FormatInt(result.WriteHead, 10))
	}
	if result.RouteToken != "" {
		w.Header().Set(RouteTokenHeader, string(result.RouteToken))
	}
	r.Body.Close()

	if result.Error == journal.ErrNotBroker {
		// Return a Location header with the broker location.
		brokerRedirect(w, r, result.RouteToken, journal.StatusCodeForError(result.Error))
	} else if result.Error != nil {
		http.Error(w, result.Error.Error(), journal.StatusCodeForError(result.Error))
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}
