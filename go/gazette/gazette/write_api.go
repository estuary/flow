package gazette

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pippio/gazette/journal"
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
		Result: make(chan error, 1),
	}
	h.handler.Append(op)
	err := <-op.Result

	if err != nil {
		// Return a 404 Not Found with Location header on a routing error.
		if routeError, ok := err.(RouteError); ok {
			http.Redirect(w, r, routeError.RerouteURL(r.URL).String(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		r.Body.Close()
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}
