// +build integration

package integration

import (
	"testing"
	"time"

	"github.com/jgraettinger/urkel"
)

var (
	etcdPodSelector   = "app.kubernetes.io/name=etcd"
	summerPodSelector = "app.kubernetes.io/name=summer"
	brokerPodSelector = "app.kubernetes.io/name=gazette"
	minioPodSelector  = "app.kubernetes.io/name=minio"
	testNamespace     = "soak"
)

func TestPartitionWithinEtcdCluster(t *testing.T) {
	var pods = urkel.FetchPods(t, testNamespace, etcdPodSelector)

	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Partition(pods[:len(pods)/2], pods[len(pods)/2:], urkel.Drop)
	time.Sleep(time.Minute)
}

func TestPartitionOneBrokerFromEtcd(t *testing.T) {
	var etcds = urkel.FetchPods(t, testNamespace, etcdPodSelector)
	var brokers = urkel.FetchPods(t, testNamespace, brokerPodSelector)

	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Partition(etcds, brokers[:1], urkel.Drop)
	time.Sleep(time.Minute)
}

func TestPartitionOneSummerFromEtcd(t *testing.T) {
	var etcds = urkel.FetchPods(t, testNamespace, etcdPodSelector)
	var summers = urkel.FetchPods(t, testNamespace, summerPodSelector)

	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Partition(etcds, summers[:1], urkel.Drop)
	time.Sleep(time.Minute)
}

func TestActivePartitionOneSummerFromBrokers(t *testing.T) {
	var brokers = urkel.FetchPods(t, testNamespace, brokerPodSelector)
	var summers = urkel.FetchPods(t, testNamespace, summerPodSelector)

	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Partition(brokers, summers[:1], urkel.Reject)
	time.Sleep(10 * time.Second)
}

func TestActivePartitionOneBrokerFromSummers(t *testing.T) {
	var brokers = urkel.FetchPods(t, testNamespace, brokerPodSelector)
	var summers = urkel.FetchPods(t, testNamespace, summerPodSelector)

	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Partition(brokers[:1], summers, urkel.Reject)
	time.Sleep(10 * time.Second)
}

func TestActivePartitionBrokers(t *testing.T) {
	var pods = urkel.FetchPods(t, testNamespace, brokerPodSelector)

	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Partition(pods[:len(pods)/2], pods[len(pods)/2:], urkel.Reject)
	time.Sleep(10 * time.Second)
}

func TestPartitionBrokersFromMinio(t *testing.T) {
	var brokers = urkel.FetchPods(t, testNamespace, brokerPodSelector)
	var minio = urkel.FetchPods(t, testNamespace, minioPodSelector)

	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Partition(brokers, minio, urkel.Drop)
	time.Sleep(time.Minute)
}
