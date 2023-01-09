package runtime

import (
	"context"
	"sync"
	//pf "github.com/estuary/flow/go/protocols/flow"
	//"go.gazette.dev/core/allocator"
)

type proxyOp struct {
	cancel        context.CancelFunc
	bytesInbound  uint64
	bytesOutbound uint64
}

type ProxyServer struct {
	mu      *sync.RWMutex
	ongoing map[string]*proxyOp
}

//func (ps *ProxyServer) Proxy()
