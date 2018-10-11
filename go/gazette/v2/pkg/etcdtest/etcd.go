// Package etcdtest provides test support for obtaining a client to an Etcd server.
//
package etcdtest

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver/api/v3client"
)

// TestClient returns a client of the embeded Etcd test server. It asserts that
// the Etcd keyspace is empty (eg, that the prior test cleaned up after itself).
func TestClient() *clientv3.Client {
	var resp, err = etcdClient.Get(context.Background(), "", clientv3.WithPrefix(), clientv3.WithLimit(5))
	if err != nil {
		log.Fatal(err)
	} else if len(resp.Kvs) != 0 {
		log.Fatalf("etcd not empty; did a previous test not clean up?\n%+v", resp)
	}
	return etcdClient
}

func Cleanup() {
	if _, err := v3client.New(embedEtcd.Server).Delete(context.Background(), "", clientv3.WithPrefix()); err != nil {
		log.Fatal(err)
	}
}

func init() {
	var cfg = embed.NewConfig()
	cfg.Dir = filepath.Join(os.TempDir(), "etcdtest.etcd", filepath.Base(os.Args[0]))
	cfg.LPUrls = nil
	cfg.LCUrls = nil

	_ = os.RemoveAll(cfg.Dir)

	// Squelch non-error logging from etcdserver, to not pollute test outputs.
	cfg.LogPkgLevels = strings.Join([]string{
		"auth=ERROR",
		"embed=ERROR",
		"etcdserver/api=ERROR",
		"etcdserver/membership=ERROR",
		"etcdserver=ERROR",
		"raft=ERROR",
		"wal=ERROR",
	}, ",")
	cfg.SetupLogging()

	var err error
	if embedEtcd, err = embed.StartEtcd(cfg); err != nil {
		log.Fatal(err)
	}

	select {
	case <-embedEtcd.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		embedEtcd.Server.Stop()
		log.Fatal("Etcd server took > 1min to start")
	}
	etcdClient = v3client.New(embedEtcd.Server)

	go func() { log.Fatal(<-embedEtcd.Err()) }()
}

var (
	embedEtcd  *embed.Etcd
	etcdClient *clientv3.Client
)
