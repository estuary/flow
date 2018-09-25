package mainboilerplate

import (
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

// EtcdConfig configures the application Etcd session.
type EtcdConfig struct {
	Address  protocol.Endpoint `long:"address" env:"ADDRESS" default:"http://localhost:2379" description:"Etcd service address endpoint"`
	LeaseTTL time.Duration     `long:"lease" env:"LEASE_TTL" default:"20s" description:"Time-to-live of Etcd lease"`
}

// EtcdContext composes an Etcd client and TTL session lease.
type EtcdContext struct {
	Etcd    *clientv3.Client
	Session *concurrency.Session
}

// MustEtcdContext builds an EtcdContext from an EtcdConfig.
func MustEtcdContext(cfg EtcdConfig) EtcdContext {
	var etcd, err = clientv3.NewFromURL(string(cfg.Address))
	Must(err, "failed to build Etcd client")

	session, err := concurrency.NewSession(etcd,
		concurrency.WithTTL(int(cfg.LeaseTTL.Seconds())))
	Must(err, "failed to establish Etcd lease session")

	return EtcdContext{Etcd: etcd, Session: session}
}
