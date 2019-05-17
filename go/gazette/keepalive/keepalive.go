package keepalive

import (
	"context"
	"net"
	"time"
)

// Dialer is copied from the invocation in http.DefaultTransport:
// https://github.com/golang/go/blob/859cab099c5a9a9b4939960b630b78e468c8c39e/src/net/http/transport.go#L40-L44
var Dialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

// DialerFunc dials |addr| with |ctx|. It's designed to be easily used
// as a grpc.DialOption, eg:
//   option.WithGRPCDialOption(grpc.WithContextDialer(keepalive.DialerFunc))
func DialerFunc(ctx context.Context, addr string) (net.Conn, error) {
	return Dialer.DialContext(ctx, "tcp", addr)
}

// TCPListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
//
// Copied, renamed and exported from:
// https://github.com/golang/go/blob/d6bce32a3607222075734bf4363ca3fea02ea1e5/src/pkg/net/http/server.go#L1840-L1856
// so we can use net.Listen + http.Serve.
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
