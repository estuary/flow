package network

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	lru "github.com/hashicorp/golang-lru/v2"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"golang.org/x/net/http2"
)

// Frontend accepts connections over its configured Listener,
// matches on the TLS ServerName (SNI), and either:
//
// - Passes unmatched connections on to the tapped Listener
// - Attaches the connection to a connector via TCP proxy
// - Serves the connection as HTTP/2 using an authorizing reverse proxy
// - Or, returns an HTTP/1.1 descriptive error about what went wrong
//
// If Frontend is not running in a TLS context, all connections are
// trivially passed to the tapped Listener.
type Frontend struct {
	controlAPI    *url.URL
	dashboard     *url.URL
	domains       []string
	networkClient pf.NetworkProxyClient
	shardClient   pc.ShardClient
	verifier      pb.Verifier

	listener  net.Listener // Tapped listener.
	tlsConfig *tls.Config  // TLS config for accepted connections.

	// Forwarding channel for connections that are passed on.
	fwdCh  chan<- net.Conn
	fwdErr *error

	// Cache of mappings from parsed to resolved SNIs.
	sniCache *lru.Cache[parsedSNI, resolvedSNI]

	// Map of frontend connections currently undergoing TLS handshake.
	handshake   map[uintptr]*frontendConn
	handshakeMu sync.Mutex
}

// frontendConn is the state of a connection initiated
// by a user into the Frontend.
type frontendConn struct {
	id uintptr

	// Raw and TLS-wrapped connections to the user.
	raw net.Conn
	tls *tls.Conn

	pass     bool
	parsed   parsedSNI
	resolved resolvedSNI

	// Error while resolving SNI to a mapped task:
	// the SNI is invalid with respect to current task config.
	sniErr error
	// proxyClient dialed during TLS handshake.
	// If set, we are acting as a TCP proxy.
	dialed *proxyClient
	// Error while dialing a shard for TCP proxy:
	// this is an internal and usually-temporary error.
	dialErr error
}

func NewFrontend(
	tap *Tap,
	fqdn string,
	controlAPI *url.URL,
	dashboard *url.URL,
	networkClient pf.NetworkProxyClient,
	shardClient pc.ShardClient,
	verifier pb.Verifier,
) (*Frontend, error) {
	if tap.raw == nil {
		return nil, fmt.Errorf("Tap has not tapped a raw net.Listener")
	}
	if strings.ToLower(fqdn) != fqdn {
		return nil, fmt.Errorf("fqdn must be lowercase, because DNS names are not case sensitive")
	}

	// Generate all subdomains of `fqdn`, including itself.
	// Also allow `localhost` to enable pass-through when port-forwarding.
	var domains, fqdnParts = []string{"localhost"}, strings.Split(fqdn, ".")
	for i := range fqdnParts {
		domains = append(domains, strings.Join(fqdnParts[i:], "."))
	}
	var sniCache, err = lru.New[parsedSNI, resolvedSNI](1024)
	if err != nil {
		panic(err)
	}

	var proxy = &Frontend{
		controlAPI:    controlAPI,
		dashboard:     dashboard,
		domains:       domains,
		networkClient: networkClient,
		shardClient:   shardClient,
		verifier:      verifier,
		listener:      tap.raw,
		tlsConfig:     tap.config,
		fwdCh:         tap.fwdCh,
		fwdErr:        &tap.fwdErr,
		sniCache:      sniCache,
		handshake:     make(map[uintptr]*frontendConn),
	}
	if proxy.tlsConfig != nil {
		proxy.tlsConfig.GetConfigForClient = proxy.getTLSConfigForClient
	}

	return proxy, nil
}

func (p *Frontend) Serve(ctx context.Context) (_err error) {
	defer func() {
		// Forward terminal error to callers of Tap.Accept().
		*p.fwdErr = _err
		close(p.fwdCh)
	}()

	for {
		var raw, err = p.listener.Accept()
		if err != nil {
			return err
		}
		if p.tlsConfig == nil {
			p.fwdCh <- raw // Not serving TLS.
			continue
		}
		go p.serveConn(ctx, raw)
	}
}

func (p *Frontend) serveConn(ctx context.Context, raw net.Conn) {
	var conn = &frontendConn{
		id:  reflect.ValueOf(raw).Pointer(),
		raw: raw,
		tls: tls.Server(raw, p.tlsConfig),
	}

	// Push `conn` onto the map of current handshakes.
	p.handshakeMu.Lock()
	p.handshake[conn.id] = conn
	p.handshakeMu.Unlock()

	// The TLS handshake machinery will next call into getTLSConfigForClient().
	var err = conn.tls.HandshakeContext(ctx)

	// Clear `conn` from the map of current handshakes.
	p.handshakeMu.Lock()
	delete(p.handshake, conn.id)
	p.handshakeMu.Unlock()

	if err != nil {
		if conn.dialed != nil {
			_ = conn.dialed.Close() // Handshake failed after we dialed the shard.
		}
		handshakeCounter.WithLabelValues("ErrHandshake").Inc()
		p.serveConnErr(conn.raw, 421, "This service may only be accessed using TLS, such as through an https:// URL.\n")
		return
	}
	if conn.pass {
		handshakeCounter.WithLabelValues("OKPass").Inc()
		p.fwdCh <- conn.tls // Connection is not for us.
		return
	}

	if conn.sniErr != nil {
		handshakeCounter.WithLabelValues("ErrSNI").Inc()
		p.serveConnErr(conn.tls, 404, fmt.Sprintf("Failed to match the connection to a task:\n\t%s\n", conn.sniErr))
	} else if conn.dialErr != nil {
		handshakeCounter.WithLabelValues("ErrDial").Inc()
		p.serveConnErr(conn.tls, 503, fmt.Sprintf("Failed to connect to a task shard:\n\t%s\n", conn.dialErr))
	} else if conn.dialed != nil {
		handshakeCounter.WithLabelValues("OkTCP").Inc()
		p.serveConnTCP(conn)
	} else {
		handshakeCounter.WithLabelValues("OkHTTP").Inc()
		p.serveConnHTTP(conn)
	}
}

func (p *Frontend) getTLSConfigForClient(hello *tls.ClientHelloInfo) (*tls.Config, error) {
	p.handshakeMu.Lock()
	var conn = p.handshake[reflect.ValueOf(hello.Conn).Pointer()]
	p.handshakeMu.Unlock()

	// Exact match of the FQDN or a parent domain means it's not for us.
	if slices.Contains(p.domains, hello.ServerName) {
		conn.pass = true
		return nil, nil
	}

	var ok bool
	var target, service, _ = strings.Cut(hello.ServerName, ".")

	// This block parses the SNI `target` and matches it to shard configuration.
	if !slices.Contains(p.domains, service) {
		conn.sniErr = fmt.Errorf("TLS ServerName %s is an invalid domain", hello.ServerName)
	} else if conn.parsed, conn.sniErr = parseSNI(target); conn.sniErr != nil {
		// No need to wrap error.
	} else if conn.resolved, ok = p.sniCache.Get(conn.parsed); !ok {
		// We didn't hit cache while resolving the parsed SNI.
		// We must fetch matching shard specs to inspect their shard ID prefix and port config.
		var shards []pc.ListResponse_Shard
		shards, conn.sniErr = listShards(hello.Context(), p.shardClient, conn.parsed, "")

		if conn.sniErr != nil {
			conn.sniErr = fmt.Errorf("fetching shards: %w", conn.sniErr)
		} else if len(shards) == 0 {
			conn.sniErr = errors.New("the requested subdomain does not match a known task and port combination")
		} else {
			conn.resolved = newResolvedSNI(conn.parsed, &shards[0].Spec)
			p.sniCache.Add(conn.parsed, conn.resolved)
		}
	}

	if conn.sniErr == nil && conn.resolved.portProtocol != "" {
		// We intend to TCP proxy to the connector. Dial the shard now so that
		// we fail-fast during TLS handshake, instead of letting the client
		// think it has a good connection.
		var addr = conn.raw.RemoteAddr().String()
		conn.dialed, conn.dialErr = dialShard(
			hello.Context(), p.networkClient, p.shardClient, conn.parsed, conn.resolved, addr)
	}

	var nextProtos []string
	if conn.sniErr != nil || conn.dialErr != nil {
		nextProtos = []string{"http/1.1"} // We'll send a descriptive HTTP/1.1 error.
	} else if conn.dialed == nil {
		nextProtos = []string{"h2"} // We'll reverse-proxy. The user MUST speak HTTP/2.
	} else {
		nextProtos = []string{conn.resolved.portProtocol} // We'll TCP proxy.
	}

	return &tls.Config{
		Certificates: p.tlsConfig.Certificates,
		NextProtos:   nextProtos,
	}, nil
}

func (p *Frontend) serveConnTCP(user *frontendConn) {
	var task, port, proto = user.resolved.taskName, user.parsed.port, user.resolved.portProtocol
	userStartedCounter.WithLabelValues(task, port, proto).Inc()

	// Enable TCP keep-alive to ensure broken user connections are closed.
	if tcpConn, ok := user.raw.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(time.Minute)
	}

	var (
		done    = make(chan struct{})
		errBack error
		errFwd  error
		shard   = user.dialed
	)

	// Backward loop that reads from `shard` and writes to `user`.
	// This may be sitting in a call to shard.Read() which races shard.Close().
	go func() {
		_, errBack = io.Copy(user.tls, shard)
		_ = user.tls.CloseWrite()
		close(done)
	}()

	// Forward loop that reads from `user` and writes to `shard`.
	if _, errFwd = io.Copy(shard, user.tls); errFwd == nil {
		_ = shard.rpc.CloseSend() // Allow reads to drain.
	} else {
		// `shard` write RST or `user` read error.
		// Either way, we want to abort reads from `shard` => `user`.
		_ = shard.Close()
	}
	<-done

	// If errBack is:
	// - nil, then we read a clean EOF from shard and wrote it all to the user.
	// - A user write RST, then errFwd MUST be an error read from the user and shard.Close() was already called.
	// - A shard read error, then the shard RPC is already done.
	_ = user.tls.Close()

	var status string
	if errFwd != nil && errBack != nil {
		status = "Err"
	} else if errFwd != nil {
		status = "ErrUser"
	} else if errBack != nil {
		status = "ErrShard"
	} else {
		status = "OK"
	}
	userHandledCounter.WithLabelValues(task, port, proto, status).Inc()
}

func (p *Frontend) serveConnHTTP(user *frontendConn) {
	var task, port, proto = user.resolved.taskName, user.parsed.port, user.resolved.portProtocol
	userStartedCounter.WithLabelValues(task, port, proto).Inc()

	var transport = &http.Transport{
		DialTLSContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			return dialShard(ctx, p.networkClient, p.shardClient, user.parsed, user.resolved, user.raw.RemoteAddr().String())
		},
		// Connections are fairly cheap because they're "dialed" over an
		// established gRPC HTTP/2 transport, but they do require a
		// Open / Opened round trip and we'd like to re-use them.
		// Note also that the maximum number of connections is implicitly
		// bounded by http2.Server's MaxConcurrentStreams (default: 100),
		// and the gRPC transport doesn't bound the number of streams.
		IdleConnTimeout:     5 * time.Second,
		MaxConnsPerHost:     0, // No limit.
		MaxIdleConns:        0, // No limit.
		MaxIdleConnsPerHost: math.MaxInt,
	}

	var reverse = httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "https"
			req.URL.Host = user.parsed.hostname
			scrubProxyRequest(req, user.resolved.portIsPublic)
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			var body = fmt.Sprintf("Service temporarily unavailable: %s\nPlease retry in a moment.", err)
			http.Error(w, body, http.StatusServiceUnavailable)
			httpHandledCounter.WithLabelValues(task, port, "ErrProxy").Inc()
		},
		ModifyResponse: func(r *http.Response) error {
			httpHandledCounter.WithLabelValues(task, port, r.Status).Inc()
			return nil
		},
		Transport: transport,
	}

	var handle = func(w http.ResponseWriter, req *http.Request) {
		httpStartedCounter.WithLabelValues(task, port, req.Method).Inc()

		if user.resolved.portIsPublic {
			reverse.ServeHTTP(w, req)
		} else if req.URL.Path == "/auth-redirect" {
			completeAuthRedirect(w, req)
			httpHandledCounter.WithLabelValues(task, port, "CompleteAuth").Inc()
		} else if err := verifyAuthorization(req, p.verifier, user.resolved.taskName); err == nil {
			reverse.ServeHTTP(w, req)
		} else if req.Method == "GET" && strings.Contains(req.Header.Get("accept"), "html") {
			// Presence of "html" in Accept means this is probably a browser.
			// Start a redirect chain to obtain an authorization cookie.
			startAuthRedirect(w, req, err, p.dashboard, user.resolved.taskName)
			httpHandledCounter.WithLabelValues(task, port, "StartAuth").Inc()
		} else {
			http.Error(w, err.Error(), http.StatusForbidden)
			httpHandledCounter.WithLabelValues(task, port, "MissingAuth").Inc()
		}
	}

	(&http2.Server{
		// IdleTimeout can be generous: it's intended to catch broken TCP transports.
		// MaxConcurrentStreams is an important setting left as the default (100).
		IdleTimeout: time.Minute,
	}).ServeConn(user.tls, &http2.ServeConnOpts{
		Handler: http.HandlerFunc(handle),
	})

	userHandledCounter.WithLabelValues(task, port, proto, "OK").Inc()
}

func (f *Frontend) serveConnErr(conn net.Conn, status int, body string) {
	// We're terminating this connection and sending a best-effort error.
	// We don't know what the client is, or even if they speak HTTP,
	// but we do only offer `http/1.1` during ALPN under an error condition.
	var resp, _ = httputil.DumpResponse(&http.Response{
		ProtoMajor: 1, ProtoMinor: 1,
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Close:      true,
	}, true)

	_, _ = conn.Write(resp)
	_ = conn.Close()
}
