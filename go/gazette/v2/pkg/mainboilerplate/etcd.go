package mainboilerplate

import (
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
)

// EtcdConfig configures the application Etcd session.
type EtcdConfig struct {
	Address  protocol.Endpoint `long:"address" env:"ADDRESS" default:"http://localhost:2379" description:"Etcd service address endpoint"`
	LeaseTTL time.Duration     `long:"lease" env:"LEASE_TTL" default:"20s" description:"Time-to-live of Etcd lease"`
}

// MustDial builds an Etcd client connection.
func (c *EtcdConfig) MustDial() *clientv3.Client {
	var etcd, err = clientv3.NewFromURL(string(c.Address))
	Must(err, "failed to build Etcd client")
	return etcd
}
