// +build integration

package integration

import (
	"testing"
	"time"

	"github.com/jgraettinger/urkel"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
)

var (
	etcdPodSelector    = "app.kubernetes.io/name=etcd"
	summerPodSelector  = "app.kubernetes.io/name=summer"
	brokerPodSelector  = "app.kubernetes.io/name=gazette"
	minioPodSelector   = "app.kubernetes.io/name=minio"
	chunkerPodSelector = "app.kubernetes.io/name=chunker"
	testNamespace      = "soak"
)

func TestPartitionWithinEtcdCluster(t *testing.T) {
	var pods = urkel.FetchPods(t, testNamespace, etcdPodSelector)

	// Expect that brokers and consumers seamlessly switch
	// over to the majority Etcd cluster during partition,
	// and continue to keep their 20s leases alive.
	var fs = urkel.NewFaultSet(t)
	time.AfterFunc(40*time.Second, fs.RemoveAll)

	fs.Partition(pods[:len(pods)/2], pods[len(pods)/2:], urkel.Drop)
	sleepAndRequireNoChunkerFails(t, time.Minute)
}

func TestPartitionOneBrokerFromEtcd(t *testing.T) {
	var etcds = urkel.FetchPods(t, testNamespace, etcdPodSelector)
	var brokers = urkel.FetchPods(t, testNamespace, brokerPodSelector)

	// Expect the broker lease expires within 20s, and other brokers
	// then take over. Broker under test detects its lease expiration,
	// crashes, and restarts.
	var fs = urkel.NewFaultSet(t)
	time.AfterFunc(40*time.Second, fs.RemoveAll)

	fs.Partition(etcds, brokers[:1], urkel.Drop)
	sleepAndRequireNoChunkerFails(t, time.Minute)
}

func TestPartitionOneSummerFromEtcd(t *testing.T) {
	var etcds = urkel.FetchPods(t, testNamespace, etcdPodSelector)
	var summers = urkel.FetchPods(t, testNamespace, summerPodSelector)

	// Expect the summer lease expires within 20s, and other summers
	// take over. Summer under test detects its lease expiration,
	// crashes, and restarts.
	var fs = urkel.NewFaultSet(t)
	time.AfterFunc(40*time.Second, fs.RemoveAll)

	fs.Partition(etcds, summers[:1], urkel.Drop)
	sleepAndRequireNoChunkerFails(t, time.Minute)
}

func TestActivePartitionOneSummerFromBrokers(t *testing.T) {
	var brokers = urkel.FetchPods(t, testNamespace, brokerPodSelector)
	var summers = urkel.FetchPods(t, testNamespace, summerPodSelector)

	// Expect the summer is stalled for 20 seconds, then recovers quickly
	// enough to fulfill the 30s sum SLA.
	var fs = urkel.NewFaultSet(t)
	time.AfterFunc(20*time.Second, fs.RemoveAll)

	fs.Partition(brokers, summers[:1], urkel.Reject)
	sleepAndRequireNoChunkerFails(t, 30*time.Second)
}

func TestActivePartitionOneBrokerFromSummers(t *testing.T) {
	var brokers = urkel.FetchPods(t, testNamespace, brokerPodSelector)
	var summers = urkel.FetchPods(t, testNamespace, summerPodSelector)

	// Expect summers are stalled for 20 seconds, then recover quickly
	// enough to fulfill the 30s sum SLA.
	var fs = urkel.NewFaultSet(t)
	time.AfterFunc(20*time.Second, fs.RemoveAll)

	fs.Partition(brokers[:1], summers, urkel.Reject)
	sleepAndRequireNoChunkerFails(t, 30*time.Second)
}

func TestActivePartitionBrokers(t *testing.T) {
	var pods = urkel.FetchPods(t, testNamespace, brokerPodSelector)

	// Expect brokers are stalled for 20 seconds, then recover quickly
	// enough to fulfill the 30s sum SLA.
	var fs = urkel.NewFaultSet(t)
	time.AfterFunc(20*time.Second, fs.RemoveAll)

	fs.Partition(pods[:len(pods)/2], pods[len(pods)/2:], urkel.Reject)
	sleepAndRequireNoChunkerFails(t, 30*time.Second)
}

func TestPartitionBrokersFromMinio(t *testing.T) {
	var brokers = urkel.FetchPods(t, testNamespace, brokerPodSelector)
	var minio = urkel.FetchPods(t, testNamespace, minioPodSelector)

	// Expect brokers tolerate persists failing while partitioned.
	var fs = urkel.NewFaultSet(t)
	time.AfterFunc(40*time.Second, fs.RemoveAll)

	fs.Partition(brokers, minio, urkel.Drop)
	sleepAndRequireNoChunkerFails(t, time.Minute)
}

func sleepAndRequireNoChunkerFails(t *testing.T, dur time.Duration) {
	time.Sleep(dur)
	for _, p := range urkel.FetchPods(t, testNamespace, chunkerPodSelector) {
		require.NotEqualf(t, v1.PodFailed, p.Status.Phase, "Chunker %s is failed", p.Name)
	}
}
