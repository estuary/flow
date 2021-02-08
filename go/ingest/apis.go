package ingest

import (
	"net/http"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gorilla/mux"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/server"
)

type args struct {
	ingester *Ingester
	journals *keyspace.KeySpace
}

// RegisterAPIs registers all ingestion APIs with the *Server instance.
func RegisterAPIs(srv *server.Server, ingester *Ingester, journals *keyspace.KeySpace) {
	var args = args{ingester, journals}

	var router = mux.NewRouter()
	srv.HTTPMux.Handle("/", router)

	// Allows transactional ingestion of multiple documents across multiple collections.
	// Expects a JSON object body where the keys are collection names and the values are arrays of
	// documents to ingest.
	router.
		Path("/ingest").
		Methods("POST", "PUT").
		Headers("Content-Type", "application/json").
		HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serveHTTPTransactionJSON(args, w, r) })

		// Ingests a single document to a collection named in the URL path (e.g. /ingest/my-collection).
		// The request body is a JSON document that will be appended to the collection.
	router.
		PathPrefix("/ingest/").
		Methods("POST", "PUT").
		Headers("Content-Type", "application/json").
		HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serveHTTPDocumentJSON(args, w, r) })

		// These allow ingestion of multiple documents to a single collection over a websocket.
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
