package main

import (
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/logging"
	"github.com/pippio/api-server/proxy/config"
	"github.com/pippio/api-server/varz"
	"github.com/pippio/gazette"
	"net/http"
)

var (
	// Service discovery.
	etcdEndpoint = flag.String("etcdEndpoint", "http://127.0.0.1:4001",
		"Etcd network service address")

	// Informational.
	releaseTag = flag.String("tag", "<none>", "Release tag")
	replica    = flag.String("replica", "<none>", "Replica number")

	spoolDirectory = flag.String("spoolDir", "/var/tmp/gazette",
		"Local directory for journal spools")
)

const ConfigJournalPath = "/pippio/journals"

func main() {
	flag.Parse()

	log.SetFormatter(&log.TextFormatter{DisableTimestamp: true})
	log.AddHook(&varz.LogCallerHook{})
	log.AddHook(&varz.LogServiceHook{
		Service: "gazetted", Tag: *releaseTag, Replica: *replica})

	// Start Etcd config provider.
	//etcdService := service.NewEtcdClientService(*etcdEndpoint)

	etcdProvider, err := config.NewEtcdProvider(*etcdEndpoint, "")
	if err != nil {
		log.WithField("err", err).Fatal()
	}
	storageContext, err := logging.NewGCSContext(etcdProvider)
	if err != nil {
		log.WithField("err", err).Fatal()
	}
	gazette := gazette.NewService(*spoolDirectory, storageContext)

	log.Fatal(http.ListenAndServe(":8080", gazette))
	//if err := etcdService.Subscribe(ConfigJournalPath, &foo); err != nil {
	//		log.WithField("err", err).Fatal()
	//	}

	//time.Sleep(time.Hour)
}

/*
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
*/
