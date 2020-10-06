package ingest

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocol"
	pb "go.gazette.dev/core/broker/protocol"
)

// HTTPAPI presents a JSON HTTP endpoint for ingestion.
type HTTPAPI struct {
	Ingester *flow.Ingester
}

// ServeHTTP implements the http.Handler interface.
func (a *HTTPAPI) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if ct := req.Header.Get("Content-Type"); ct != "application/json" {
		http.Error(w,
			fmt.Sprintf("unsupported Content-Type %q (expected 'application/json'')", ct), http.StatusBadRequest)
		return
	}

	var body map[pf.Collection][]json.RawMessage
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var ingest = a.Ingester.Start()
	defer ingest.Done()

	for collection, docs := range body {
		for _, doc := range docs {
			if err := ingest.Add(collection, doc); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
	}

	var offsets, err = ingest.PrepareAndAwait()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Add("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(struct {
		Offsets pb.Offsets
	}{offsets})
}

var _ http.Handler = (*HTTPAPI)(nil)
