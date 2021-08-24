package ingest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/protocol/ext"
)

func serveHTTPTransactionJSON(a args, w http.ResponseWriter, r *http.Request) (err error) {
	return doServeHTTPJSON(a, w, r, func(ingest *Ingestion) error {
		var body map[pf.Collection][]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			return err
		}
		for collection, docs := range body {
			for _, doc := range docs {
				if err := ingest.Add(collection, doc); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func serveHTTPDocumentJSON(a args, w http.ResponseWriter, r *http.Request) (err error) {
	return doServeHTTPJSON(a, w, r, func(ingestion *Ingestion) error {
		var name = strings.Join(strings.Split(r.URL.Path, "/")[2:], "/")

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("failed to read request body: %w", err)
		}

		return ingestion.Add(pf.Collection(name), body)
	})
}

func doServeHTTPJSON(a args, w http.ResponseWriter, r *http.Request, addIngests func(*Ingestion) error) (err error) {
	var ingest = a.ingester.Start()
	defer func() {
		if err != nil {
			log.WithFields(log.Fields{"err": err, "url": r.URL.String(), "client": r.RemoteAddr}).
				Warn("ingest via http body failed")
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}()

	err = addIngests(ingest)
	if err != nil {
		return err
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
