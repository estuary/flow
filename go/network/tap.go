package network

import (
	"crypto/tls"
	"net"
)

// Tap is an adapter which retains a tapped net.Listener and itself acts as a
// net.Listener, with an Accept() that communicates over a forwarding channel.
// It's used for late binding of a Proxy to a pre-created Listener,
// and to hand off connections which are not intended for Proxy.
type Tap struct {
	raw    net.Listener
	config *tls.Config
	fwdCh  chan net.Conn
	fwdErr error
}

func NewTap() *Tap {
	return &Tap{
		raw:    nil, // Set by Tap().
		config: nil, // Set by Tap().
		fwdCh:  make(chan net.Conn, 4),
		fwdErr: nil,
	}
}

func (tap *Tap) Wrap(tapped net.Listener, config *tls.Config) (net.Listener, error) {
	tap.raw = tapped
	tap.config = config
	return tap, nil
}

func (tap *Tap) Accept() (net.Conn, error) {
	if conn, ok := <-tap.fwdCh; ok {
		return conn, nil
	} else {
		return nil, tap.fwdErr
	}
}

func (tap *Tap) Close() error {
	return tap.raw.Close()
}

func (tap *Tap) Addr() net.Addr {
	return tap.raw.Addr()
}

var _ net.Listener = &Tap{}
