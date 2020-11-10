package ingest

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/protocol/ext"
)

const wsCSVProtocol = "csv/v1"
const wsTSVProtocol = "tsv/v1"
const wsJSONProtocol = "json/v1"

type wsIngester interface {
	onFrame(collection *pf.CollectionSpec, addCh chan<- ingestAdd) error
}

// Maximum time we'll wait for a write we initiate to complete.
// We don't use websocket's ping-pong mechanism, instead relying on TCP keep-alive.
const wsWriteTimeout = 10 * time.Second

func serveWebsocketCSV(a args, comma rune, w http.ResponseWriter, r *http.Request) {
	var buffer bytes.Buffer
	var csvReader = csv.NewReader(&buffer)
	csvReader.Comma = comma
	csvReader.ReuseRecord = true

	// Columns of the CSV header row are mapped to determine all possible projections.
	// Thereafter, rows may omit trailing columns having projections which are not
	// required to exist.
	csvReader.FieldsPerRecord = -1

	var csvIngester = wsCsvIngester{
		buffer:    &buffer,
		csvReader: csvReader,
	}

	_ = serveWebsocket(a, w, r, &buffer, &csvIngester)
}

type wsJsonIngester struct {
	buffer *bytes.Buffer
}

func (self *wsJsonIngester) onFrame(collection *pf.CollectionSpec, addCh chan<- ingestAdd) error {
	var decoder = json.NewDecoder(self.buffer)
	for {
		var doc json.RawMessage

		if err := decoder.Decode(&doc); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		addCh <- ingestAdd{
			collection: collection.Name,
			doc:        doc,
		}
	}
}

func serveWebsocketJSON(a args, w http.ResponseWriter, r *http.Request) {
	var buffer bytes.Buffer
	var ingester = wsJsonIngester{
		buffer: &buffer,
	}
	_ = serveWebsocket(a, w, r, &buffer, &ingester)
}

func serveWebsocket(
	a args,
	w http.ResponseWriter,
	r *http.Request,
	buffer *bytes.Buffer,
	ingester wsIngester,
) (err error) {

	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		Subprotocols:    []string{wsCSVProtocol, wsTSVProtocol},
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// A response has already been sent to client by |upgrader|.
		log.WithFields(log.Fields{"err": err, "url": r.URL.String(), "client": r.RemoteAddr}).
			Warn("failed to upgrade ingest request to websocket")
	}

	var frames int // Number of processed frame.

	// Defer a closure which ensures the peer connection is closed (gracefully, if possible).
	defer func() {
		var closeMessage []byte
		var deadline = time.Now().Add(wsWriteTimeout)
		var delayedClose = false

		// When using a tool like `websocat` in a Unix pipe, a failure of an
		// earlier portion of the pipe (eg, because a file doesn't exist) results
		// in no data being sent. Make it clear this isn't expected by erroring.
		if err == nil && frames == 0 {
			err = fmt.Errorf("client closed the connection without sending any documents")
		}

		if err != nil {
			log.WithFields(log.Fields{"err": err, "url": r.URL.String(), "client": r.RemoteAddr}).
				Warn("ingest over websocket failed")

			// Send a best-effort closing message with the terminating error.
			conn.SetWriteDeadline(deadline)
			if err = conn.WriteJSON(struct {
				Error            string
				ApproximateFrame int
			}{err.Error(), frames}); err != nil {
				log.WithFields(log.Fields{"err": err, "url": r.URL.String(), "client": r.RemoteAddr}).
					Warn("failed to send closing error")
			}

			closeMessage = websocket.FormatCloseMessage(websocket.CloseProtocolError, "error")
			delayedClose = true
		} else {
			closeMessage = websocket.FormatCloseMessage(websocket.CloseNormalClosure, "success")
		}

		// Write close to the peer.
		if err = conn.WriteControl(websocket.CloseMessage, closeMessage, deadline); err != nil {
			log.WithFields(log.Fields{"err": err, "url": r.URL.String(), "client": r.RemoteAddr}).
				Warn("failed to write websocket close")
		}

		if delayedClose {
			// Sleep a short while before actually closing the underlying connection.
			// The reason we do this is that the peer is probably still trying to send data.
			// If we close right now, we're likely to send a reset immediately thereafter,
			// and poorly written clients may hit the reset on attempting to send and never
			// bother to read out the lovely error message we just put so much work into sending.
			time.Sleep(100 * time.Millisecond)
		}

		if err := conn.Close(); err != nil {
			log.WithFields(log.Fields{"err": err, "url": r.URL.String(), "client": r.RemoteAddr}).
				Warn("failed to close websocket")
		}
	}()

	// Disable the default handler, which sends an immediate close.
	// We'll manual close on ingester drain.
	conn.SetCloseHandler(func(int, string) error { return nil })

	var name = strings.Join(strings.Split(r.URL.Path, "/")[2:], "/")
	var collection = a.ingester.Collections[pf.Collection(name)]
	if collection == nil {
		return fmt.Errorf("'%v' is not an ingestable collection", name)
	}

	var ingestCh, progressCh = newIngestPump(r.Context(), a.ingester)
	var pollCh, frameCh = newWSReadPump(r.Context(), conn, buffer)

	pollCh <- struct{}{} // Start first read.

	// Loop until |progressCh| closes (the ingestion has drained)
	for {
		select {
		case err := <-frameCh:
			// Did we receive a clean EOF?
			if err == io.EOF {
				close(ingestCh) // Drain ingestion pump.
				continue        // Note that we don't poll a next frame.
			} else if err != nil {
				return fmt.Errorf("while receiving: %w", err)
			} else if err = ingester.onFrame(collection, ingestCh); err != nil {
				return fmt.Errorf("processing frame: %w", err)
			}
			frames++
			pollCh <- struct{}{} // Read next frame.

		case progress, ok := <-progressCh:

			// Did we drain all ingestions?
			if !ok {
				return nil
			} else if progress.Err != nil {
				return progress.Err
			}

			// Send progress notification.
			a.journals.Mu.RLock()
			var etcd = ext.FromEtcdResponseHeader(a.journals.Header)
			a.journals.Mu.RUnlock()

			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err = conn.WriteJSON(struct {
				Offsets   pb.Offsets
				Etcd      pb.Header_Etcd
				Processed int
			}{progress.Offsets, etcd, progress.Processed}); err != nil {
				return fmt.Errorf("while sending progress: %w", err)
			}
		}
	}
}

func newWSReadPump(ctx context.Context, conn *websocket.Conn, buffer *bytes.Buffer) (chan<- struct{}, <-chan error) {
	var chIn = make(chan struct{}, 1)
	var chOut = make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return // Cancelled.
			case _ = <-chIn:
			}
			buffer.Reset()

			var mt, r, err = conn.NextReader()
			if err == nil {
				_, err = buffer.ReadFrom(r)
			}

			if err == nil {
				// If this message didn't end in a newline, add one.
				if l := len(buffer.Bytes()); l == 0 || buffer.Bytes()[l-1] != '\n' {
					_ = buffer.WriteByte('\n')
				}

				switch mt {
				case websocket.TextMessage: // Pass.
				case websocket.BinaryMessage:
					err = fmt.Errorf("unexpected binary message (expected text)")
				default:
					err = fmt.Errorf("unexpected message type %d", mt)
				}
			}

			if websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseNoStatusReceived) {
				err = io.EOF
			}
			chOut <- err
		}
	}()
	return chIn, chOut
}

type ingestAdd struct {
	collection pf.Collection
	doc        json.RawMessage
}

type ingestProgress struct {
	Processed int
	Offsets   pb.Offsets
	Err       error
}

func newIngestPump(ctx context.Context, ingester *flow.Ingester) (chan<- ingestAdd, <-chan ingestProgress) {
	var chIn = make(chan ingestAdd, 1024)
	var chOut = make(chan ingestProgress, 1)

	go func() {
		defer close(chOut)
		var processed int

		for {
			var in ingestAdd
			var ok bool

			select {
			case <-ctx.Done():
				return // Cancelled.
			case in, ok = <-chIn:
				if !ok {
					return // Clean EOF.
				}
			}

			var ingestion = ingester.Start()

		EXTEND:
			if err := ingestion.Add(in.collection, in.doc); err != nil {
				select {
				case chOut <- ingestProgress{Err: err}:
				case <-ctx.Done():
				}
			}
			processed++

			// Continue pulling ready documents into the ingestion.
			for {
				select {
				case in, ok = <-chIn:
					if ok {
						goto EXTEND
					} else {
						goto COMMIT
					}
				default:
					goto COMMIT
				}
			}

		COMMIT:
			var offsets, err = ingestion.PrepareAndAwait()
			select {
			case chOut <- ingestProgress{Offsets: offsets, Processed: processed, Err: err}:
			case <-ctx.Done():
			}
		}
	}()
	return chIn, chOut
}
