package main

import (
	"context"
	//"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v4"
)

var ingesterAddr = flag.String("ingester-address", "ws://localhost:8080", "Address where flow-ingester is listening")
var postgresURI = flag.String("postgres-uri", "postgresql://postgres:postgres@localhost:5432/postgres", "URI for connecting to postgres")
var verifyTable = flag.String("verify-table", "sets_verify", "Name of the table that verifications are materialized into")
var verifyPeriod = flag.String("verify-period", "30s", "Time to wait in between verifying results")

// TestSetOps continuously ingests operations and periodically checks the materialized results in
// postgres to ensure that none have failed. This test will never exit normally.
// You may want to use the `-timeout=0` argument, since the default is to timeout after 10 minutes.
func TestSetOps(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	var ctx, cancelFunc = context.WithCancel(context.Background())
	var generatorConfig = newGeneratorConfig()
	var period, err = time.ParseDuration(*verifyPeriod)
	if err != nil {
		t.Fatalf("failed to parse verify-period: %v", err)
	}

	// Ensure we can connect to postgres and the ingester before starting up the background
	// processes.
	pgConn, err := pgx.Connect(ctx, *postgresURI)
	if err != nil {
		t.Fatalf("Failed to connect to postgres: %v", err)
	}

	var dialer = websocket.Dialer{
		HandshakeTimeout: time.Second * 10,
		Subprotocols:     []string{"json/v1"},
	}
	var ingesterURI = *ingesterAddr + "/ingest/soak/set-ops/operations"
	wsConn, errResp, err := dialer.DialContext(ctx, ingesterURI, http.Header{})
	if err != nil {
		t.Fatalf("Failed to dial ingester: %v, errResp: %v", err, errResp)
	}

	// Start up goroutines to ingest and verify
	var ingestErrCh = doIngest(ctx, generatorConfig, wsConn)
	var verifyErrCh = doVerify(ctx, pgConn, generatorConfig.Author, generatorConfig.Concurrent, period)

	// Wait for whichever goroutine finishes first, then cancel the other.
	select {
	case err = <-ingestErrCh:
	case err = <-verifyErrCh:
	}
	cancelFunc()
	if err != nil {
		t.Fatal(err)
	}
}

func doVerify(ctx context.Context, pgConn *pgx.Conn, author int, nStreams int, period time.Duration) <-chan error {
	var errCh = make(chan error)
	go func() {
		var err = verify(ctx, pgConn, author, nStreams, period)
		errCh <- err
	}()
	return errCh
}

func verify(ctx context.Context, pgConn *pgx.Conn, author int, nStreams int, period time.Duration) error {
	// We only want to verify results of operations that we created.
	// The timestamp comparison is textual, but that's ok because we consistently format them as
	// RFC3339, which works with lexicographic comparisons.
	// The derived/expect check casts to text because postgres doesn't support comparison of json
	// columns, only jsonb.
	var template = "select flow_document from %s where author = $1 AND (timestamp < $2 OR derivedvalues::text != expectvalues::text)"
	var queries = map[string]string{
		"sets":          fmt.Sprintf(template, "sets"),
		"sets_register": fmt.Sprintf(template, "sets_register"),
	}

	var now = time.Now()
	var lastVerify = now.UTC().Format(time.RFC3339)
	for {
		select {
		case <-ctx.Done():
			return nil
		case now = <-time.After(period):
		}

		for k, query := range queries {
			var rows, err = pgConn.Query(ctx, query, author, lastVerify)
			if err != nil {
				return fmt.Errorf("querying verification results for %s: %w", k, err)
			}

			for rows.Next() {
				var naughtyDoc string
				err = rows.Scan(&naughtyDoc)
				if err != nil {
					return fmt.Errorf("reading an error result for %s: %w", k, err)
				}
				return fmt.Errorf("Failed: collection: %s, lastVerify: %s: %s", k, lastVerify, naughtyDoc)
			}
			rows.Close()
		}

		lastVerify = now.UTC().Format(time.RFC3339)
		fmt.Fprintf(os.Stderr, "Successfully verified %d sets for author %d, as of: %s\n", nStreams, author, lastVerify)
	}
}

func doIngest(ctx context.Context, conf generatorConfig, wsConn *websocket.Conn) <-chan error {
	var errCh = make(chan error)
	go func() {
		var err = ingestOps(ctx, conf, wsConn)
		errCh <- err
	}()
	return errCh
}

func ingestOps(ctx context.Context, conf generatorConfig, wsConn *websocket.Conn) error {
	defer wsConn.Close()

	var opsCh = make(chan json.RawMessage)
	go generateOps(ctx, conf, opsCh)

	for {
		var writer, err = wsConn.NextWriter(websocket.TextMessage)
		if err != nil {
			return err
		}

		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				return nil
			case next := <-opsCh:
				_, err = writer.Write(next)
				if err != nil {
					return fmt.Errorf("writing to websocket: %w", err)
				}
			}
		}
		if err = writer.Close(); err != nil {
			return fmt.Errorf("closing websocket writer: %w", err)
		}
	}
}
