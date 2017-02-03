package gazette

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/httpdump"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/keepalive"
	"net/http/httputil"
)

const (
	CommitDeltaHeader          = "X-Commit-Delta"
	FragmentLastModifiedHeader = "X-Fragment-Last-Modified"
	FragmentLocationHeader     = "X-Fragment-Location"
	FragmentNameHeader         = "X-Fragment-Name"
	RouteTokenHeader           = "X-Route-Token"
	WriteHeadHeader            = "X-Write-Head"

	ReplicateClientIdlePoolSize = 6
)

type ReplicateClient struct {
	endpoint *CachedURL
	idlePool chan replicaClientConn
}

type replicaClientConn struct {
	raw net.Conn
	buf *bufio.ReadWriter
}

type replicaClientTransaction struct {
	client ReplicateClient

	chunker io.WriteCloser
	conn    replicaClientConn
	request *http.Request
}

func NewReplicateClient(ep *CachedURL) ReplicateClient {
	// Use a global map of pools keyed on |ep.Base| to facilitate connection
	// re-use.
	idlePoolsMu.Lock()
	idlePool, ok := idlePools[ep.Base]
	if !ok {
		idlePool = make(chan replicaClientConn, ReplicateClientIdlePoolSize)
		idlePools[ep.Base] = idlePool
	}
	idlePoolsMu.Unlock()

	return ReplicateClient{
		endpoint: ep,
		idlePool: idlePool,
	}
}

func (c ReplicateClient) Replicate(op journal.ReplicateOp) {
	transaction := replicaClientTransaction{client: c}
	go transaction.start(op)
}

func (t *replicaClientTransaction) start(op journal.ReplicateOp) {
	conn, err := t.takeConn()
	if err != nil {
		op.Result <- journal.ReplicateResult{Error: err}
		return
	}
	req, _ := t.client.endpoint.NewHTTPRequest("REPLICATE", "/"+op.Journal.String(), nil)
	queryArgs := url.Values{
		"newSpool":   {strconv.FormatBool(op.NewSpool)},
		"writeHead":  {strconv.FormatInt(op.WriteHead, 10)},
		"routeToken": {string(op.RouteToken)},
	}
	req.URL.RawQuery = queryArgs.Encode()
	req.Header.Add("Expect", "100-continue")
	req.Header.Add("Trailer", CommitDeltaHeader)
	req.TransferEncoding = []string{"chunked"}

	reqBytes, err := httpdump.DumpRequest(req, false)
	if err != nil {
		op.Result <- journal.ReplicateResult{Error: err}
		return
	}
	// Flush the replication request to the peer.
	conn.buf.Write(reqBytes)
	if err = conn.buf.Flush(); err != nil {
		op.Result <- journal.ReplicateResult{Error: err}
		return
	}
	// Wait up to a minute for a 100-continue response.
	// TODO(johnny): HTTP/2 to peers would remove the need for this timeout.
	conn.raw.SetReadDeadline(time.Now().Add(time.Minute))
	resp, err := http.ReadResponse(conn.buf.Reader, req)
	if err != nil {
		op.Result <- journal.ReplicateResult{Error: err}
		return
	} else if resp.StatusCode != http.StatusContinue {
		var body bytes.Buffer
		io.Copy(&body, resp.Body)

		var remoteWriteHead int64
		if s := resp.Header.Get(WriteHeadHeader); s != "" {
			remoteWriteHead, err = strconv.ParseInt(s, 16, 64)
			if err != nil {
				log.WithFields(log.Fields{"err": err, "arg": s}).
					Error("failed to parse replica head")
			}
		}
		// Finish the request by writing an empty chunk and trailing headers.
		conn.buf.WriteString("0\r\n\r\n")
		if err := conn.buf.Flush(); !resp.Close && err == nil {
			// Connection is still okay. Retain for the next round.
			t.putConn(conn)
		}
		op.Result <- journal.ReplicateResult{Error: errors.New(body.String()),
			ErrorWriteHead: remoteWriteHead}
		return
	}
	// We've now opened a transaction stream.
	conn.raw.SetReadDeadline(time.Time{}) // Clear timeout.
	t.chunker = httputil.NewChunkedWriter(conn.buf)
	t.conn = conn
	t.request = req

	op.Result <- journal.ReplicateResult{Writer: t}
	return
}

func (t *replicaClientTransaction) takeConn() (replicaClientConn, error) {
	var conn replicaClientConn
	select {
	case conn = <-t.client.idlePool:
		return conn, nil
	default:
	}

	url, err := t.client.endpoint.ResolveURL()
	if err != nil {
		return replicaClientConn{}, err
	}
	raw, err := keepalive.Dialer.Dial("tcp", url.Host)
	if err != nil {
		t.client.endpoint.InvalidateResolution()
		return replicaClientConn{}, err
	}
	return replicaClientConn{raw,
		bufio.NewReadWriter(bufio.NewReader(raw), bufio.NewWriter(raw))}, nil
}

func (t *replicaClientTransaction) putConn(conn replicaClientConn) {
	conn.raw.SetReadDeadline(time.Time{}) // Clear timeout.
	select {
	case t.client.idlePool <- conn:
	default:
	}
}

func (t *replicaClientTransaction) Write(p []byte) (n int, err error) {
	return t.chunker.Write(p)
}

func (t *replicaClientTransaction) Commit(delta int64) error {
	// Close the chunker and write the commit delta as a trailing header.
	t.chunker.Close()
	fmt.Fprintf(t.conn.buf, "%s: %x\r\n\r\n", CommitDeltaHeader, delta)

	if err := t.conn.buf.Flush(); err != nil {
		return err
	}
	// Wait up to a minute for a commit response.
	// TODO(johnny): HTTP/2 to peers would remove the need for this timeout.
	t.conn.raw.SetReadDeadline(time.Now().Add(time.Minute))
	resp, err := http.ReadResponse(t.conn.buf.Reader, t.request)
	if err != nil {
		return err
	}
	// Success is indicated by 204 No Content.
	if resp.StatusCode != http.StatusNoContent {
		var body bytes.Buffer
		io.Copy(&body, resp.Body)
		err = errors.New(body.String())
	} else {
		io.Copy(ioutil.Discard, resp.Body)
	}
	if !resp.Close {
		t.putConn(t.conn)
	}
	return err
}

var (
	// Pool idle connections keyed on Base of an endpoint.
	idlePools   map[string]chan replicaClientConn
	idlePoolsMu sync.Mutex
)

func init() {
	idlePools = make(map[string]chan replicaClientConn)
}
