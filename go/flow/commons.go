package flow

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/estuary/flow/go/bindings"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/net/http2"
)

// Commons embeds a pf.CommonsSpec, and extends it with mechanisms
// for instantiating runtime instances of shared resources.
type Commons struct {
	pf.CatalogCommons

	tsWorker   *JSWorker
	tsClient   *http.Client
	tsInitErr  error
	tsInitOnce sync.Once

	schemaIndex     *bindings.SchemaIndex
	schemaIndexErr  error
	schemaIndexOnce sync.Once
}

// TypeScriptLocalSocket returns the TypeScript Unix Domain Socket of this Commons.
// If a TypeScript worker isn't running, one is started.
func (c *Commons) TypeScriptClient(etcd *clientv3.Client) (*http.Client, error) {
	c.tsInitOnce.Do(func() { c.initTypeScript(etcd) })
	return c.tsClient, c.tsInitErr
}

func (c *Commons) SchemaIndex() (*bindings.SchemaIndex, error) {
	c.schemaIndexOnce.Do(func() {
		c.schemaIndex, c.schemaIndexErr = bindings.NewSchemaIndex(&c.Schemas)
	})
	return c.schemaIndex, c.schemaIndexErr
}

func (c *Commons) initTypeScript(etcd *clientv3.Client) (err error) {
	defer func() { c.tsInitErr = err }()

	if c.TypescriptLocalSocket == "" {
		url, err := url.Parse(c.TypescriptPackageUrl)
		if err != nil {
			return fmt.Errorf("parsing package URL: %w", err)
		}

		if url.Scheme != "etcd" {
			return fmt.Errorf("only etcd:// scheme is supported at present")
		}

		resp, err := etcd.Get(context.Background(), url.Path)
		if err != nil {
			return fmt.Errorf("fetching Etcd key %q: %w", url.Path, err)
		} else if resp.Count != 1 {
			return fmt.Errorf("etcd key %q not found", url.Path)
		}

		c.tsWorker, err = NewJSWorker(resp.Kvs[0].Value)
		if err != nil {
			return fmt.Errorf("starting worker: %w", err)
		}
		c.TypescriptLocalSocket = c.tsWorker.socketPath
	}

	// TypeScript h2c client bound to unix socket TypescriptLocalSocket.
	// See: https://www.mailgun.com/blog/http-2-cleartext-h2c-client-example-go/
	c.tsClient = &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(_, _ string, _ *tls.Config) (net.Conn, error) {
				return net.Dial("unix", c.TypescriptLocalSocket)
			},
		},
	}

	return nil
}

// Destory the Commons, releasing associated state and stopping associated workers.
func (c *Commons) Destroy() {
	if c.tsClient != nil {
		c.tsClient.CloseIdleConnections()
	}
	if c.tsWorker != nil {
		if err := c.tsWorker.Stop(); err != nil {
			logrus.WithField("err", err).Error("failed to stop worker")
		}
	}
	if c.schemaIndex != nil {
		_ = true // TODO destroy schema index.
	}
}
