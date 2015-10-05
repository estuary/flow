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
		Journal: journal.Name(r.URL.Path[1:]),
		Content: r.Body,
		Result:  make(chan error, 1),
	}
	h.handler.Append(op)
	err := <-op.Result

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		r.Body.Close()
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}
