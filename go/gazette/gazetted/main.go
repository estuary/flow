package main

import (
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/coreos/go-etcd/etcd"
	"github.com/pippio/api-server/service"
	"github.com/pippio/api-server/varz"
	"github.com/pippio/gazette"
	"time"
)

var (
	// Service discovery.
	etcdEndpoint = flag.String("etcdEndpoint", "http://127.0.0.1:4001",
		"Etcd network service address")

	// Informational.
	releaseTag = flag.String("tag", "<none>", "Release tag")
	replica    = flag.String("replica", "<none>", "Replica number")
)

const ConfigJournalPath = "/pippio/journals"

type foobar struct{}

func (f foobar) OnEtcdResponse(response *etcd.Response, tree *etcd.Node) {
	log.WithFields(log.Fields{
		"action":   response.Action,
		"node.Key": response.Node.Key,
	}).Info("update")

	stack := []etcd.Nodes{tree.Nodes}
	for i := len(stack); i != 0; i = len(stack) {

		if j := len(stack[i-1]); j == 0 {
			stack = stack[:i-1]
		} else {
			node := stack[i-1][0]
			stack[i-1] = stack[i-1][1:]

			fmt.Println("\t" + node.Key)

			if len(node.Nodes) != 0 {
				stack = append(stack, node.Nodes)
			}
		}
	}
}

func main() {
	flag.Parse()

	log.SetFormatter(&log.TextFormatter{DisableTimestamp: true})
	log.AddHook(&varz.LogCallerHook{})
	log.AddHook(&varz.LogServiceHook{
		Service: "gazetted", Tag: *releaseTag, Replica: *replica})

	// Start Etcd config provider.
	etcdService := service.NewEtcdClientService(*etcdEndpoint)

	var foo gazette.Service

	if err := etcdService.Subscribe(ConfigJournalPath, &foo); err != nil {
		log.WithField("err", err).Fatal()
	}

	time.Sleep(time.Hour)
}
