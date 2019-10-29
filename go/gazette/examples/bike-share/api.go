package bike_share

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	store_sqlite "go.gazette.dev/core/consumer/store-sqlite"
	"go.gazette.dev/core/message"
)

// ServeBikeHistory is an http.HandlerFunc which returns the most recent
// rides of a bike ID provided via URL parameter. Invoke as:
//
//      /api/bikes?id=12345
//
// If the bike ID is served by a non-local shard, ServeBikeHistory will
// proxy to the appropriate peer.
func (app *Application) ServeBikeHistory(w http.ResponseWriter, r *http.Request) {
	var err error
	defer func() {
		if err != nil {
			http.Error(w, err.Error(), http.StatusTeapot)
		}
	}()

	// Parse bike ID from the request.
	var bike = r.URL.Query().Get("id")
	if bike == "" {
		err = errors.New("expected 'bike' parameter")
		return
	}

	// Parse a header attached by a proxy-ing peer. The header is used during
	// resolution for sanity checks, and to ensure the peer isn't aware of a
	// future Etcd revision we haven't yet read (and should wait for).
	var proxy *pb.Header
	if p := r.Header.Get("X-ProxyHeader"); p == "" {
		// Pass.
	} else if err = json.Unmarshal([]byte(p), &proxy); err != nil {
		return
	}

	var shard pc.ShardID
	if shard, err = app.mapBikeToOwningShard(bike); err != nil {
		return
	}
	w.Header().Add("X-Shard-ID", shard.String())

	// Resolve the shard to determine its current assignment. If we're the
	// primary for this shard, Resolve also provides us with its Store.
	var res consumer.Resolution
	if res, err = app.svc.Resolver.Resolve(consumer.ResolveArgs{
		Context:     r.Context(),
		ShardID:     shard,
		MayProxy:    proxy == nil, // MayProxy if the request hasn't already been.
		ProxyHeader: proxy,
	}); err != nil {
		return
	} else if res.Status != pc.Status_OK {
		err = fmt.Errorf(res.Status.String())
		return
	} else if res.Store == nil {
		// Shard is assigned to peer.
		err = proxyRequest(w, r, res.Header)
		return
	}
	defer res.Done()

	// Add resolution header to the response.
	var b []byte
	if b, err = json.Marshal(res.Header); err != nil {
		return
	} else {
		w.Header().Add("X-Header", string(b))
	}

	// Query previous rides for this bike, and write each to the response.
	var rows *sql.Rows

	switch s := res.Store.(type) {
	case *consumer.SQLStore:
		rows, err = app.queryHistory.Query(bike)
	case *store_sqlite.Store:
		rows, err = s.Stmts[3].Query(bike)
	default:
		panic("not reached")
	}
	if err != nil {
		return
	}

	var enc = json.NewEncoder(w)
	for rows.Next() {
		var o struct {
			UUID                     message.UUID
			StartTime, EndTime       time.Time
			StartStation, EndStation string
		}
		if err = rows.Scan(&o.UUID, &o.StartTime, &o.EndTime, &o.StartStation, &o.EndStation); err != nil {
			return
		} else if err = enc.Encode(o); err != nil {
			return
		}
	}
	err = rows.Err()
	return
}

// mapBikeToOwningShard maps the |bike| ID to its associated journal partition,
// and then maps that journal to the shard which consumes it (we assume
// there's only one).
func (app *Application) mapBikeToOwningShard(bike string) (pc.ShardID, error) {
	if journal, _, err := app.rideMapping([]byte(bike)); err != nil {
		return "", err
	} else if specs := app.svc.Resolver.ShardsWithSource(journal); len(specs) == 0 {
		return "", fmt.Errorf("no ShardSpec is consuming mapped journal %s", journal)
	} else {
		return specs[0].Id, nil
	}
}

// proxyRequest proxies an http.Request to the primary of the Header's Route.
func proxyRequest(w http.ResponseWriter, orig *http.Request, hdr pb.Header) error {
	var url = *orig.URL
	var peer = hdr.Route.Endpoints[hdr.Route.Primary].URL()
	url.Scheme, url.Host = peer.Scheme, peer.Host

	var req, err = http.NewRequestWithContext(orig.Context(), "GET", url.String(), nil)
	if err != nil {
		return err
	} else if b, err := json.Marshal(hdr); err != nil {
		return err
	} else {
		req.Header.Add("X-ProxyHeader", string(b))
	}

	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return err
	}
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	_ = resp.Body.Close()
	return err
}
