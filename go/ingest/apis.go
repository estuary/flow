package ingest

import (
	"net/http"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gorilla/mux"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/server"
)

type args struct {
	ingester *flow.Ingester
	journals *keyspace.KeySpace
}

// RegisterAPIs registers all ingestion APIs with the *Server instance.
func RegisterAPIs(srv *server.Server, ingester *flow.Ingester, journals *keyspace.KeySpace) {
	var args = args{ingester, journals}

	var router = mux.NewRouter()
	srv.HTTPMux.Handle("/", router)

	router.
		Path("/ingest").
		Methods("POST", "PUT").
		Headers("Content-Type", "application/json").
		HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serveHTTPJSON(args, w, r) })
	router.
		PathPrefix("/ingest/").
		Methods("GET").
		Headers("Sec-WebSocket-Protocol", wsCSVProtocol).
		HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serveWebsocketCSV(args, ',', w, r) })
	router.
		PathPrefix("/ingest/").
		Methods("GET").
		Headers("Sec-WebSocket-Protocol", wsTSVProtocol).
		HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serveWebsocketCSV(args, '\t', w, r) })
	router.
		PathPrefix("/ingest/").
		Methods("GET").
		Headers("Sec-WebSocket-Protocol", wsJSONProtocol).
		HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serveWebsocketJSON(args, w, r) })

	pf.RegisterIngesterServer(srv.GRPCServer, &grpcAPI{args})
}
