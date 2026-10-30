// +build integration

package integration

import (
	"testing"
	"time"

	"github.com/jgraettinger/urkel"
)

func TestCrashOneEtcdMember(t *testing.T) {
	var pod = urkel.FetchPods(t, testNamespace, etcdPodSelector)[0]

	// Etcd cluster, brokers, and summers recover seamlessly from an Etcd crash.
	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Crash(pod)
	sleepAndRequireNoChunkerFails(t, 30*time.Second)
}

func TestCrashTwoBrokers(t *testing.T) {
	var pods = urkel.FetchPods(t, testNamespace, brokerPodSelector)[:2]

	// Up to two brokers can fail without risking append durability.
	// The crash will cause a stall for < 20s while their Etcd leases expire.
	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Crash(pods...)
	sleepAndRequireNoChunkerFails(t, 30*time.Second)
}

func TestCrashOneSummer(t *testing.T) {
	var pod = urkel.FetchPods(t, testNamespace, summerPodSelector)[0]

	// A summer can crash without violating exactly-once semantics, and without
	// a significant impact to live-ness. The crash will a stall for < 20s while
	// its Etcd leases expire.
	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Crash(pod)
	sleepAndRequireNoChunkerFails(t, 30*time.Second)
}

func TestDeleteAllBrokers(t *testing.T) {
	var pods = urkel.FetchPods(t, testNamespace, brokerPodSelector)

	// Brokers will seamlessly hand off journals and exit. Summers and chunkers
	// transition to new owners without incident.
	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Delete(pods...)
	sleepAndRequireNoChunkerFails(t, 30*time.Second)
}

func TestDeleteAllSummers(t *testing.T) {
	var pods = urkel.FetchPods(t, testNamespace, summerPodSelector)

	// Summers will seamlessly hand off shard processing and exit.
	var fs = urkel.NewFaultSet(t)
	defer fs.RemoveAll()

	fs.Delete(pods...)
	sleepAndRequireNoChunkerFails(t, 30*time.Second)
}
