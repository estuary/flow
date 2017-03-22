// gazconsumer lol
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"

	"io"

	"github.com/pippio/consensus"
	"github.com/pippio/endpoints"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/varz"
)

const (
	bytesPerGigabyte    = 1024 * 1024 * 1024
	journalNotBeingRead = -1
	minimumLag          = 1048576 * 512
	infinityLag         = -1
)

var (
	short = flag.Bool("short", false, "Provide lag estimate in GiB only.")
)

type varsData struct {
	Gazette *gazetteData `json:"gazette"`
}

type gazetteData struct {
	Readers map[string]*readerData `json:"readers"`
}

type readerData struct {
	Bytes int64 `json:"bytes"`
	Head  int64 `json:"head"`
}

type memberData struct {
	masters, replicas int
	totalLag          int64
}

type ConsumerData struct {
	memberNames  []string
	journalNames []string
	members      map[string]memberData
	owners       map[string]string

	journalLag map[string]int64
}

func (cd ConsumerData) TotalLag() int64 {
	var res int64 = 0
	for _, lag := range cd.journalLag {
		res += lag
	}
	return res
}

func main() {
	endpoints.ParseFromEnvironment()
	log.SetFormatter(new(log.JSONFormatter))
	flag.Parse()
	varz.StartDebugListener()

	printLags()
	//var cdata = makeConsumerData(flag.Arg(0))
	//printOutput(cdata)
	// fmt.Printf("Mike Lag: %s", cdata.journalLag)

	//printOutput(cdata)
}

func printOutput(cdata ConsumerData) {
	if *short {
		var consumerLag int64
		for _, member := range cdata.memberNames {
			var info = cdata.members[member]
			if info.totalLag != infinityLag {
				consumerLag += info.totalLag
			}
		}
		fmt.Println(consumerLag / bytesPerGigabyte)
	} else {
		printLong(cdata, os.Stdout)
	}
}

func printLong(cdata ConsumerData, out io.Writer) {
	var journalsTable = tablewriter.NewWriter(out)
	journalsTable.SetHeader([]string{"Journal", "Lag", "Owner"})
	sort.Strings(cdata.journalNames)
	for _, journal := range cdata.journalNames {
		var owner = cdata.owners[journal]
		if lag, ok := cdata.journalLag[journal]; !ok || lag == journalNotBeingRead {
			journalsTable.Append([]string{journal, "<no data>", owner})
		} else if lag == 0 {
			journalsTable.Append([]string{journal, "<up-to-date>", owner})
		} else {
			journalsTable.Append([]string{journal, humanize.Bytes(uint64(lag)), owner})
		}
	}
	journalsTable.Render()
	var membersTable = tablewriter.NewWriter(out)
	membersTable.SetHeader([]string{"Member", "Masters", "Replicas", "Total Lag"})
	sort.Strings(cdata.memberNames)
	for _, member := range cdata.memberNames {
		var info = cdata.members[member]
		var lagStr string
		if info.totalLag == infinityLag {
			lagStr = "\u221e"
		} else {
			lagStr = humanize.Bytes(uint64(info.totalLag))
		}

		membersTable.Append([]string{
			member, strconv.Itoa(info.masters), strconv.Itoa(info.replicas), lagStr})
	}
	membersTable.Render()
}

func makeConsumerData(topic string) ConsumerData {
	var cdata ConsumerData
	cdata.members = make(map[string]memberData)
	cdata.owners = make(map[string]string)
	readHeads, writeHeads := getHeads(topic, &cdata)
	getJournalLag(writeHeads, readHeads, &cdata)
	return cdata
}

func makeServices(topic string) (*http.Client, *gazette.Client, *etcd.Response) {
	var httpClient = &http.Client{
		Timeout:   5 * time.Second,
		Transport: gazette.MakeHttpTransport(),
	}
	var gazetteClient, err = gazette.NewClient(*endpoints.GazetteEndpoint)
	if err != nil {
		log.WithField("err", err).Fatal("failed to init gazette client")
	}
	etcdClient, err := etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *endpoints.EtcdEndpoint}})
	if err != nil {
		log.WithField("err", err).Fatal("failed to init etcd client")
	}
	var keysAPI = etcd.NewKeysAPI(etcdClient)
	var key = path.Join(topic, "items")
	shardsRoot, err := keysAPI.Get(context.Background(), key,
		&etcd.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		log.WithFields(log.Fields{"err": err, "key": key}).Fatal("can't load etcd key")
	}
	return httpClient, gazetteClient, shardsRoot
}

type MemberInfo struct {
	Journal journal.Name
	Member  string
	Master  bool
}

func makeEtcd(topic string) *etcd.Response {
	etcdClient, err := etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *endpoints.EtcdEndpoint}})
	if err != nil {
		log.WithField("err", err).Fatal("failed to init etcd client")
	}
	var keysAPI = etcd.NewKeysAPI(etcdClient)
	var key = path.Join(topic, "items")
	shardsRoot, err := keysAPI.Get(context.Background(), key,
		&etcd.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		log.WithFields(log.Fields{"err": err, "key": key}).Fatal("can't load etcd key")
	}
	return shardsRoot
}

func getMasterRepInfo(topic string) []MemberInfo {
	shardsRoot := makeEtcd(topic)

	var memberInfos []MemberInfo

	for _, node := range shardsRoot.Node.Nodes {
		var route = consensus.NewRoute(shardsRoot, node)
		var prefix = len(route.Item.Key) + 1

		// Derive journal name from item name.
		var parts = strings.Split(path.Base(route.Item.Key), "-")
		var journalName = fmt.Sprintf("pippio-journals/%s/part-%s",
			strings.Join(parts[1:len(parts)-1], "-"), parts[len(parts)-1])

		for i, member := range route.Entries {
			var memberID = member.Key[prefix:]

			// |memberID| should be an IP:port pair.
			if host, _, err := net.SplitHostPort(memberID); err != nil {
				log.WithFields(log.Fields{"master": memberID, "err": err}).Error(
					"route replica name is not a host/port pair")
			} else {
				memberInfos = append(memberInfos, MemberInfo{
					Member:  host,
					Master:  (i == 0),
					Journal: journal.Name(journalName),
				})
			}
		}
	}

	return memberInfos
}

func getHeads(topic string, cdata *ConsumerData) (map[string]int64, map[string]int64) {
	httpClient, gazetteClient, shardsRoot := makeServices(topic)

	var readHeadOutput = make(chan journalHeadResult, 1024)
	var writeHeadOutput = make(chan journalHeadResult, 1024)
	var readHeadWg = new(sync.WaitGroup)
	var writeHeadWg = new(sync.WaitGroup)
	for _, node := range shardsRoot.Node.Nodes {
		var route = consensus.NewRoute(shardsRoot, node)
		var prefix = len(route.Item.Key) + 1

		// Derive journal name from item name.
		var parts = strings.Split(path.Base(route.Item.Key), "-")
		var journal = fmt.Sprintf("pippio-journals/%s/part-%s",
			strings.Join(parts[1:len(parts)-1], "-"), parts[len(parts)-1])

		cdata.journalNames = append(cdata.journalNames, journal)
		writeHeadWg.Add(1)
		go fetchWriteHead(journal, gazetteClient, writeHeadOutput, writeHeadWg)

		if len(route.Entries) == 0 {
			// Item has no master.
			log.WithField("item", route.Item.Key).Warn("no master for item")
			continue
		}

		for i, member := range route.Entries {
			var memberID = member.Key[prefix:]
			var info, ok = cdata.members[memberID]

			if !ok {
				cdata.memberNames = append(cdata.memberNames, memberID)
			}

			if i == 0 {
				info.masters++
				cdata.owners[journal] = memberID

				// |memberID| should be an IP:port pair.
				if host, _, err := net.SplitHostPort(memberID); err != nil {
					log.WithFields(log.Fields{"master": memberID, "err": err}).Error(
						"route replica name is not a host/port pair")
				} else {
					var debugURL = fmt.Sprintf("http://%s:8090/debug/vars", host)
					readHeadWg.Add(1)
					go fetchReadHead(journal, httpClient, debugURL, readHeadOutput, readHeadWg)
				}
			} else {
				info.replicas++
			}

			cdata.members[memberID] = info
		}
	}
	return collectHeads(readHeadWg, readHeadOutput, writeHeadWg, writeHeadOutput)
}

func collectHeads(readHeadWg *sync.WaitGroup, readHeadOutput chan journalHeadResult,
	writeHeadWg *sync.WaitGroup, writeHeadOutput chan journalHeadResult) (map[string]int64, map[string]int64) {
	var readHeads = make(map[string]int64)
	var writeHeads = make(map[string]int64)
	readHeadWg.Wait()
	close(readHeadOutput)
	for r := range readHeadOutput {
		if r.err != nil {
			log.WithFields(log.Fields{"err": r.err, "name": r.name}).Warn(
				"failed to retrieve read-head info")
		}
		readHeads[r.name] = r.head
	}
	writeHeadWg.Wait()
	close(writeHeadOutput)
	for r := range writeHeadOutput {
		if r.err != nil {
			log.WithFields(log.Fields{"err": r.err, "name": r.name}).Warn(
				"failed to retrieve write-head info")
		}
		writeHeads[r.name] = r.head
	}
	return readHeads, writeHeads
}

func getJournalLag(writeHeads map[string]int64, readHeads map[string]int64, cdata *ConsumerData) map[string]int64 {
	// Determine total lag value per-member.
	var journalLag = make(map[string]int64)
	for journal, writeHead := range writeHeads {
		var owner = cdata.owners[journal]
		var member = cdata.members[owner]
		if readHead, ok := readHeads[journal]; !ok {
			// |journalLag[journal]| remains unavailable.
		} else if readHead == journalNotBeingRead {
			journalLag[journal] = journalNotBeingRead
			member.totalLag = infinityLag
		} else if member.totalLag != infinityLag {
			var delta = writeHead - readHead
			if delta >= minimumLag {
				journalLag[journal] = delta
				member.totalLag += delta
			} else {
				journalLag[journal] = 0
			}
		}
		cdata.members[owner] = member
	}
	cdata.journalLag = journalLag
	return journalLag
}

type journalHeadResult struct {
	name string
	head int64
	err  error
}

func fetchWriteHead(name string, client *gazette.Client, output chan<- journalHeadResult, wg *sync.WaitGroup) {
	var args = journal.ReadArgs{Journal: journal.Name(name)}
	var result, _ = client.Head(args)
	output <- journalHeadResult{name: name, err: result.Error, head: result.WriteHead}
	wg.Done()
}

func fetchReadHead(name string, client *http.Client, url string, output chan<- journalHeadResult, wg *sync.WaitGroup) {
	var vars varsData
	var result = journalHeadResult{name: name}

	// Connect to the debug port (8090) of the IP.
	var resp *http.Response
	resp, result.err = client.Get(url)

	if result.err != nil {
		log.WithFields(log.Fields{"url": url, "err": result.err}).Error(
			"failed to access debug URL")
	} else if result.err = json.NewDecoder(resp.Body).Decode(&vars); result.err != nil {
		log.WithFields(log.Fields{"url": url, "err": result.err}).Error(
			"failed to decode debug URL")
	} else if reader, ok := vars.Gazette.Readers[name]; !ok {
		// The journal is owned by the member, but is not being read at this time.
		result.head = journalNotBeingRead
	} else {
		result.head = reader.Head
	}

	if resp != nil {
		resp.Body.Close()
	}
	output <- result
	wg.Done()
}
