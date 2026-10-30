package mainboilerplate

import (
	"context"
	"crypto/tls"
	"time"

	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/server"
	"google.golang.org/grpc"
)

// EtcdConfig configures the application Etcd session.
type EtcdConfig struct {
	Address       protocol.Endpoint `long:"address" env:"ADDRESS" default:"http://localhost:2379" description:"Etcd service address endpoint"`
	CertFile      string            `long:"cert-file" env:"CERT_FILE" default:"" description:"Path to the client TLS certificate"`
	CertKeyFile   string            `long:"cert-key-file" env:"CERT_KEY_FILE" default:"" description:"Path to the client TLS private key"`
	TrustedCAFile string            `long:"trusted-ca-file" env:"TRUSTED_CA_FILE" default:"" description:"Path to the trusted CA for client verification of server certificates"`
	LeaseTTL      time.Duration     `long:"lease" env:"LEASE_TTL" default:"20s" description:"Time-to-live of Etcd lease"`
}

// MustDial builds an Etcd client connection.
func (c *EtcdConfig) MustDial() *clientv3.Client {
	var addr = c.Address.URL()
	var tlsConfig *tls.Config

	switch addr.Scheme {
	case "https":
		var err error
		tlsConfig, err = server.BuildTLSConfig(c.CertFile, c.CertKeyFile, c.TrustedCAFile)
		Must(err, "failed to build TLS config")
	case "unix":
		// The Etcd client requires hostname is stripped from unix:// URLs.
		addr.Host = ""
	}

	// Use a blocking dial to build a trial connection to Etcd. If we're actively
	// partitioned or mis-configured this avoids a K8s CrashLoopBackoff, and
	// there's nothing actionable to do anyway aside from wait (or be SIGTERM'd).
	var timer = time.AfterFunc(time.Second, func() {
		log.WithField("addr", addr.String()).Warn("dialing Etcd is taking a while (is network okay?)")
	})
	trialEtcd, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr.String()},
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
		TLS:         tlsConfig,
	})
	Must(err, "failed to build trial Etcd client")

	_ = trialEtcd.Close()
	timer.Stop()

	// Build our actual |etcd| connection, with much tighter timeout bounds.
	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: []string{addr.String()},
		// Automatically and periodically sync the set of Etcd servers.
		// If a network split occurs, this allows for attempting different
		// members until a connectable one is found on our "side" of the network
		// partition.
		AutoSyncInterval: time.Minute,
		// Use aggressive timeouts to quickly cycle through member endpoints,
		// prior to our lease TTL expiring.
		DialTimeout:          c.LeaseTTL / 20,
		DialKeepAliveTime:    c.LeaseTTL / 4,
		DialKeepAliveTimeout: c.LeaseTTL / 4,
		// Require a reasonably recent server cluster.
		RejectOldCluster: true,
		TLS:              tlsConfig,
	})
	Must(err, "failed to build Etcd client")

	Must(etcd.Sync(context.Background()), "initial Etcd endpoint sync failed")
	return etcd
}
