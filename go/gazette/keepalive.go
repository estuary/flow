package keepalive

import (
	"context"
	"net"
	"time"
)

// NOTE(joshk): Copied from the invocation in http.DefaultTransport
var Dialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

// DialerFunc dials |addr| with |timeout|. It's designed to be easily used
// as a grpc.DialOption, eg:
//   option.WithGRPCDialOption(grpc.WithDialer(keepalive.DialerFunc))
func DialerFunc(addr string, timeout time.Duration) (net.Conn, error) {
	var ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return Dialer.DialContext(ctx, "tcp", addr)
}

// NOTE(joshk): Copied, renamed and exported from:
// https://golang.org/src/net/http/server.go?s=65528:65583#L2418
// so we can use net.Listen + http.Serve.

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type TCPListener struct {
	*net.TCPListener
}

func (ln TCPListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
