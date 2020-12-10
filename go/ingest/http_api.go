package ingest

import (
	"encoding/json"
	"net/http"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/protocol/ext"
)

func serveHTTPJSON(a args, w http.ResponseWriter, r *http.Request) (err error) {
	defer func() {
		if err != nil {
			log.WithFields(log.Fields{"err": err, "url": r.URL.String(), "client": r.RemoteAddr}).
				Warn("ingest via http transaction failed")
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}()

	var body map[pf.Collection][]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return err
	}

	var ingest = a.ingester.Start()
	defer ingest.Done()

	for collection, docs := range body {
		for _, doc := range docs {
			if err := ingest.Add(collection, doc); err != nil {
				return err
			}
		}
	}

	offsets, err := ingest.PrepareAndAwait()
	if err != nil {
		return err
	}

	a.journals.Mu.RLock()
	var etcd = ext.FromEtcdResponseHeader(a.journals.Header)
	a.journals.Mu.RUnlock()

	w.Header().Add("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(struct {
		Offsets pb.Offsets
		Etcd    pb.Header_Etcd
	}{offsets, etcd})
}
