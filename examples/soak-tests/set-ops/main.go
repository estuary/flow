package main

import (
	"context"
	//"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v4"
	"github.com/jessevdk/go-flags"
)

type cmdTest struct {
	cmdGenerate
	IngesterAddr string `long:"ingester-address" env:"INGESTER_ADDR" default:"ws://localhost:8080" description:"Address where flow-ingester is listening"`
	PostgresURI  string `long:"postgres-uri" env:"POSTGRES_URI" default:"postgresql://postgres:postgres@localhost:5432/postgres" description:"URI for connecting to postgres"`
	VerifyPeriod string `long:"verify-period" env:"VERIFY_PERIOD" default:"30s" description:"Time to wait in between verifying results"`
}

func (cmd cmdTest) Execute(_ []string) error {
	cmd.cmdGenerate.resolveAuthor()
	var period, err = time.ParseDuration(cmd.VerifyPeriod)
	if err != nil {
		return fmt.Errorf("parsing verify-period: %w", err)
	}

	// Ensure we can connect to postgres and the ingester before starting up the background
	// processes.
	var ctx, cancelFunc = context.WithCancel(context.Background())
	defer cancelFunc()
	pgConn, err := pgx.Connect(ctx, cmd.PostgresURI)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}

	var dialer = websocket.Dialer{
		HandshakeTimeout: time.Second * 10,
		Subprotocols:     []string{"json/v1"},
	}
	var ingesterURI = cmd.IngesterAddr + "/ingest/soak/set-ops/operations"
	wsConn, errResp, err := dialer.DialContext(ctx, ingesterURI, http.Header{})
	if err != nil {
		return fmt.Errorf("dialing ingester: %w, errResp: %v", err, errResp)
	}

	// Start up goroutines to ingest and verify
	var ingestErrCh = doIngest(ctx, cmd.cmdGenerate, wsConn)
	var verifyErrCh = doVerify(ctx, pgConn, cmd.cmdGenerate.Author, cmd.cmdGenerate.Streams, period)

	// Wait for whichever goroutine finishes first, then cancel the other.
	select {
	case err = <-ingestErrCh:
	case err = <-verifyErrCh:
	}
	return err
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var parser = flags.NewParser(nil, flags.HelpFlag|flags.PassDoubleDash)

	_, _ = parser.AddCommand("test", "Run the set-ops soak test", `
Ingest set operations and verify the results continuously until an error is encountered.
`, &cmdTest{})
	_, _ = parser.AddCommand("generate", "Generate operations and print them to stdout", `
Generates set operations and just prints them to stdout, without ingesting or verifying anything.
`, &cmdGenerate{})

	_, err := parser.ParseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
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
	// We only want to verify results of operations that we created. Verifies that expected matches
	// actual. The derived/expect check casts to text because postgres doesn't support comparison of
	// json columns, only jsonb.
	var template = "select flow_document from %s where author = $1 AND derivedvalues::text != expectvalues::text"
	var queries = map[string]string{
		"sets":          fmt.Sprintf(template, "sets"),
		"sets_register": fmt.Sprintf(template, "sets_register"),
	}

	// Verifies that the expected number of rows have been updated since the last time we checked.
	// The timestamp comparison is textual, but that's ok because we consistently format them as
	// RFC3339, which works with lexicographic comparisons.
	var countTemplate = "select count(*) from %s where author = $1 AND timestamp > $2"
	var countQueries = map[string]string{
		"sets":          fmt.Sprintf(countTemplate, "sets"),
		"sets_register": fmt.Sprintf(countTemplate, "sets_register"),
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
			var rows, err = pgConn.Query(ctx, query, author)
			if err != nil {
				return fmt.Errorf("querying verification results for %s: %w", k, err)
			}

			for rows.Next() {
				var naughtyDoc string
				err = rows.Scan(&naughtyDoc)
				if err != nil {
					return fmt.Errorf("reading an error result for %s: %w", k, err)
				}
				return fmt.Errorf("collection %s: lastVerify: %s: %s", k, lastVerify, naughtyDoc)
			}
			rows.Close()
		}

		for k, query := range countQueries {
			var row = pgConn.QueryRow(ctx, query, author, lastVerify)
			var count int
			var err = row.Scan(&count)
			if err != nil {
				return fmt.Errorf("querying row count for %s: %w", k, err)
			}
			if count != nStreams {
				return fmt.Errorf("collection %s: expected %d sets updated since %s, but was: %d", k, nStreams, lastVerify, count)
			}
		}

		lastVerify = now.UTC().Format(time.RFC3339)
		fmt.Fprintf(os.Stderr, "Successfully verified %d sets for author %d, as of: %s\n", nStreams, author, lastVerify)
	}
}

func doIngest(ctx context.Context, conf cmdGenerate, wsConn *websocket.Conn) <-chan error {
	var errCh = make(chan error)
	go func() {
		var err = ingestOps(ctx, conf, wsConn)
		errCh <- err
	}()
	// We must read progress updates or the sender will time
	// us out. Write them to stdout.
	go func() {
		for {
			var out json.RawMessage
			if err := wsConn.ReadJSON(&out); err != nil {
				errCh <- err
				return
			}
		}
	}()
	return errCh
}

func ingestOps(ctx context.Context, conf cmdGenerate, wsConn *websocket.Conn) error {
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
