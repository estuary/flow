package gazette

import (
	"github.com/coreos/go-etcd/etcd"
	"github.com/pippio/etcd-client"
	"net/url"
	"sync"
)

type Client struct {
	topics map[string]*Topic

	ring   []*url.URL
	ringMu sync.Mutex
}

func NewClient(etcdPath string, etcdService etcdClient.EtcdService) *Client {
	client := &Client{}

	etcdService.Subscribe(etcdPath, client)
	return client
}

func (c *Client) routeToEndpoint(journal string) *url.URL {
	c.ringMu.Lock()
	defer c.ringMu.Unlock()

	return ring[0]
}

func (c *Client) OnEtcdResponse(response *etcd.Response, tree *etcd.Node) {
	c.ringMu.Lock()
	defer c.ringMu.Unlock()

	tree.Nodes
}
