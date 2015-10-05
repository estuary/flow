package gazette

import (
	"github.com/gorilla/mux"
	"net/http"
)

type WriteAPI struct {
	dispatcher *dispatcher
}

func NewWriteAPI(dispatcher *dispatcher) *WriteAPI {
	return &WriteAPI{dispatcher: dispatcher}
}

func (h *WriteAPI) Register(router *mux.Router) {
	router.NewRoute().Methods("PUT").HandlerFunc(h.Write)
}

func (h *WriteAPI) Write(w http.ResponseWriter, r *http.Request) {
	journal := r.URL.Path[1:]

	var op = AppendOp{
		Journal: journal,
		Content: r.Body,
		Result:  make(chan error, 1),
	}
	h.dispatcher.DispatchAppend(op)
	err := <-op.Result

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		r.Body.Close()
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}
