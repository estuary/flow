// Given a list of -prefix arguments, expand those prefixes to a list of Etcd
// paths to Gazette consumers and compare the state of the consumer to the
// state of the source journals to determine how much data backlog exists for
// each consumer. In monitor mode, expose all consumers as Prometheus metrics
// for continuous monitoring. Otherwise, print the state of each consumer as a
// table to stdout.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	humanize "github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pippio/gazette/consensus"
	"github.com/pippio/gazette/envflag"
	"github.com/pippio/gazette/envflagfactory"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/metrics"
	"github.com/pippio/varz"
)

const (
	journalNotBeingRead = -1
)

var (
	minimumDisplayLagFlag = flag.String("minimumDisplayLag", "10MB",
		"Minimum lag to display. Anything lower is displayed as Zero.")
	monitor = flag.Bool("monitor", false,
		"Use arguments to provide metrics to Prometheus.")
	monitorInterval = flag.Duration("monitorInterval", time.Minute,
		"How often to update metrics.")
	prefixList      prefixFlagSet
	etcdEndpoint    = envflagfactory.NewEtcdServiceEndpoint()
	gazetteEndpoint = envflagfactory.NewGazetteServiceEndpoint()

	// Global service objects.
	keysAPI       etcd.KeysAPI
	gazetteClient *gazette.Client
	httpClient    *http.Client
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
	states            map[string]string
	hasUnreadJournals bool
	totalLag          int64
}

type consumerData struct {
	memberNames  []string
	journalNames []string
	members      map[string]memberData
	owners       map[string]string

	journalLag map[string]journalData
}

type journalData struct {
	lag   int64
	state string
}

func (cd consumerData) totalLag() int64 {
	var res int64 = 0
	for _, data := range cd.journalLag {
		res += data.lag
	}
	return res
}

func main() {
	flag.Var(&prefixList, "prefix", "Specify an Etcd prefix to check for consumers.")
	envflag.CommandLine.Parse()
	defer varz.Initialize("gazconsumer").Cleanup()

	minimumDisplayLag = calcMinimumDisplayLag()

	httpClient = &http.Client{
		Timeout:   5 * time.Second,
		Transport: gazette.MakeHttpTransport(),
	}

	var err error
	gazetteClient, err = gazette.NewClient(*gazetteEndpoint)
	if err != nil {
		log.WithField("err", err).Fatal("failed to init gazette client")
	}
	etcdClient, err := etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *etcdEndpoint}})
	if err != nil {
		log.WithField("err", err).Fatal("failed to init etcd client")
	}
	keysAPI = etcd.NewKeysAPI(etcdClient)

	// Expand |prefixList| into |consumerList|.
	var consumerList = expandPrefixList(prefixList)

	checkConsumers(consumerList)
	if *monitor {
		prometheus.MustRegister(metrics.GazconsumerCollectors()...)
		prometheus.MustRegister(metrics.GazetteClientCollectors()...)

		for _ = range time.Tick(*monitorInterval) {
			consumerList = expandPrefixList(prefixList)
			checkConsumers(consumerList)
		}
	}
}

// Check all specified consumers once, and log metrics or to stdout based on
// whether |monitor| mode is enabled.
func checkConsumers(consumerList []string) {
	for _, consumer := range consumerList {
		var cdata = buildConsumerData(consumer)
		// Do not log statistics for consumers with 0 members.
		if *monitor {
			if len(cdata.memberNames) > 0 {
				varz.ObtainCount("gazconsumer", "lag", "#consumer", consumer).Set(cdata.totalLag())
				metrics.GazconsumerLagBytes.WithLabelValues(consumer).Set(float64(cdata.totalLag()))
			}
		} else {
			fmt.Printf("Consumer: %s\n\n", consumer)
			printLong(cdata, os.Stdout)
		}
	}
}

func bytesForDisplay(lag int64) string {
	if minimumDisplayLag > uint64(lag) {
		lag = 0
	}

	return humanize.Bytes(uint64(lag))
}

// Human readable output for a |consumerData|.
func printLong(cdata consumerData, out io.Writer) {
	var journalsTable = tablewriter.NewWriter(out)
	journalsTable.SetHeader([]string{"Journal", "Lag", "State", "Owner"})
	sort.Strings(cdata.journalNames)
	for _, journal := range cdata.journalNames {
		var owner = cdata.owners[journal]
		if data, ok := cdata.journalLag[journal]; !ok {
			journalsTable.Append([]string{journal, "unknown", data.state, owner})
		} else {
			journalsTable.Append([]string{journal, bytesForDisplay(data.lag), data.state, owner})
		}
	}
	journalsTable.Render()
	var membersTable = tablewriter.NewWriter(out)
	membersTable.SetHeader([]string{"Member", "Masters", "Replicas", "Total Lag"})
	sort.Strings(cdata.memberNames)
	for _, member := range cdata.memberNames {
		var info = cdata.members[member]
		var lagFmt = "%s"
		if info.hasUnreadJournals {
			lagFmt = ">= %s (incomplete data)"
		}

		membersTable.Append([]string{
			member, strconv.Itoa(info.masters), strconv.Itoa(info.replicas),
			fmt.Sprintf(lagFmt, bytesForDisplay(info.totalLag))})
	}
	membersTable.Render()
}

// Given a |consumerPath| pointing to a valid consumer in Etcd, build a
// |consumerData| structure showing all the journals being read by that
// consumer and the state of each live consuming master of each of those
// journals.
func buildConsumerData(consumerPath string) consumerData {
	var key = path.Join(consumerPath, "items")
	var itemsRoot, err = keysAPI.Get(context.Background(), key,
		&etcd.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		log.WithFields(log.Fields{"err": err, "key": key}).Fatal("can't load etcd key")
	}

	// Optional: Use Etcd offsets of the consumer to produce a maximum lag that
	// can be relied upon even for shards that are in-transition. If the offsets
	// don't exist, |makeEtcdOffsetMap| does nothing.
	key = path.Join(consumerPath, "offsets")
	var offsetsRoot, _ = keysAPI.Get(context.Background(), key,
		&etcd.GetOptions{Recursive: true})
	var etcdOffsets map[string]int64
	if offsetsRoot != nil && offsetsRoot.Node != nil {
		etcdOffsets = makeEtcdOffsetMap(key, offsetsRoot.Node)
	}

	var consumer = consumerData{
		members: make(map[string]memberData),
		owners:  make(map[string]string),
	}

	var readHeads, writeHeads = getHeads(itemsRoot, &consumer)
	getJournalLag(etcdOffsets, writeHeads, readHeads, &consumer)

	return consumer
}

// Based on the items/ hierarchy of a Gazette consumer |itemsRoot|, populate
// |cdata| with:
// - The read-heads for all journals being mastered by all replicas in the
//   consumer group.
// - The write-heads for all the journals (asking Gazette itself.)
func getHeads(itemsRoot *etcd.Response, cdata *consumerData) (map[string]int64, map[string]int64) {
	var readHeadOutput = make(chan journalHeadResult, 1024)
	var writeHeadOutput = make(chan journalHeadResult, 1024)
	var readHeadWg = new(sync.WaitGroup)
	var writeHeadWg = new(sync.WaitGroup)
	for _, node := range itemsRoot.Node.Nodes {
		var route = consensus.NewRoute(node)
		var prefix = len(route.Item.Key) + 1

		// Derive journal name from item name.
		var parts = strings.Split(path.Base(route.Item.Key), "-")
		var journal = fmt.Sprintf("pippio-journals/%s/part-%s",
			strings.Join(parts[1:len(parts)-1], "-"), parts[len(parts)-1])

		cdata.journalNames = append(cdata.journalNames, journal)
		writeHeadWg.Add(1)
		go fetchWriteHead(journal, gazetteClient, writeHeadOutput, writeHeadWg)

		if len(route.Entries) == 0 {
			// Item has no master. This is normal if the consumer is not running.
			continue
		}

		for i, member := range route.Entries {
			var memberID = member.Key[prefix:]
			var info, ok = cdata.members[memberID]

			if !ok {
				cdata.memberNames = append(cdata.memberNames, memberID)
				info.states = make(map[string]string)
			}

			// Stores "primary", "ready" or "recovering" state.
			info.states[journal] = member.Value

			if i == 0 {
				info.masters++
				cdata.owners[journal] = memberID

				// |memberID| should be an IP:port pair.
				if host, _, err := net.SplitHostPort(memberID); err != nil {
					log.WithFields(log.Fields{"master": memberID, "err": err}).Error(
						"route replica name is not a host/port pair")
				} else {
					// Connect to the debug port (8090) of the IP.
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

// Wait for dispatched calls of fetchReadHead and fetchWriteHead to return, and
// collect the results in |readHeads| and |writeHeads| return values.
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

// Using the collected values:
// - |readHeads| from live consumer processes
// - |etcdOffsets| from Gazette consumer state snapshot located in Etcd which lower-bounds
//   the state of the consumer in the absence of a live member
// - |writeHeads| from Gazette indicating the size of each source journal
// Populate |cdata| which contains information about the total lag of the consumer on a per
// shard basis.
func getJournalLag(etcdOffsets, writeHeads, readHeads map[string]int64, cdata *consumerData) {
	// Determine total lag value per-member.
	var journalLag = make(map[string]journalData)
	for journal, writeHead := range writeHeads {
		var owner = cdata.owners[journal]
		var member = cdata.members[owner]
		var readHead int64
		var ok bool
		var state = "OK"
		var etcdState string

		if readHead, ok = readHeads[journal]; !ok {
			// |journalLag[journal]| remains unavailable.
			state = "NoOwner"
			readHead = etcdOffsets[journal]
		} else if etcdState, ok = member.states[journal]; ok && etcdState == "recovering" {
			// The shard reports that it is recovering. For now, use the read
			// offsets of the journal stored in Etcd, assuming that the replica
			// will recover to that point eventually (e.g. the lag cannot
			// be less than that amount.)
			state = "Recovering"
			readHead = etcdOffsets[journal]
		} else if readHead == journalNotBeingRead {
			// The shard is not recovering, but is not busy reading the
			// journal.
			state = "NotReading"
			readHead = etcdOffsets[journal]
		} else if readHead < etcdOffsets[journal] {
			// The shard is not recovering and is actively reading. The
			// offsets stored for this shard in Etcd exceed the ones reported
			// by the replica.
			state = "EtcdAhead"
			readHead = etcdOffsets[journal]
		}

		var delta = writeHead - readHead
		if delta >= 0 {
			journalLag[journal] = journalData{lag: delta, state: state}
			member.totalLag += delta
		} else {
			journalLag[journal] = journalData{lag: 0, state: state}
		}
		cdata.members[owner] = member
	}
	cdata.journalLag = journalLag
}

type journalHeadResult struct {
	name string
	head int64
	err  error
}

// Worker method to request the write-head (e.g. latest offset) of a specified
// journal from Gazette. Parallelized so all requests occur simultaneously.
func fetchWriteHead(name string, client *gazette.Client, output chan<- journalHeadResult, wg *sync.WaitGroup) {
	var args = journal.ReadArgs{Journal: journal.Name(name)}
	var result, _ = client.Head(args)
	output <- journalHeadResult{name: name, err: result.Error, head: result.WriteHead}
	wg.Done()
}

// Worker method to request the read-head (e.g. latest read offset) of a
// specified journal on a specified replica of a consumer which is mastering
// the shard, by connecting to the replica's debug port and checking its
// .gazette.readers JSON field.  Parallelized so all requests occur
// simultaneously.
func fetchReadHead(name string, client *http.Client, url string, output chan<- journalHeadResult, wg *sync.WaitGroup) {
	var vars varsData
	var result = journalHeadResult{name: name}
	var resp *http.Response
	resp, result.err = client.Get(url)

	if result.err != nil {
		log.WithFields(log.Fields{"url": url, "err": result.err}).Warn(
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

// Checks all members of |prefixList| to see if it is a Gazette consumer
// directory (e.g. containing "items" or "members" children) or not. If it is,
// return it. If not, and it is a directory, visit the child nodes to see if
// they are Gazette consumers. Returns a list of valid consumer Etcd prefixes
// that result from walking the Etcd hierarchy.
func expandPrefixList(prefixList []string) []string {
	var consumerList []string
	for i := 0; i < len(prefixList); i++ {
		var prefix = prefixList[i]
		var prefixNode, err = keysAPI.Get(context.Background(), prefix, nil)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "key": prefix}).Fatal("could not get Etcd key")
		}

		var isConsumer bool
		for _, child := range prefixNode.Node.Nodes {
			if base := path.Base(child.Key); base == "items" || base == "members" {
				// Path is a consumer root.
				consumerList = append(consumerList, prefix)
				isConsumer = true
				break
			}
		}
		if !isConsumer {
			for _, child := range prefixNode.Node.Nodes {
				// Path is a directory that contains consumer roots. Check each
				// one individually.
				prefixList = append(prefixList, child.Key)
			}
		}
	}

	return consumerList
}

// Given |root| as the etcd.Node representing the offsets/ directory of a
// Gazette consumer, build a journal : read offset mapping that represents our
// best guess for what the lag value for a given journal is when there is
// nothing actively reading that journal. The key is that the lag cannot be
// greater than the amount stored in Etcd.
func makeEtcdOffsetMap(key string, root *etcd.Node) map[string]int64 {
	var offsets = make(map[string]int64)
	var visit = root.Nodes
	for i := 0; i < len(visit); i++ {
		var node = visit[i]
		if node.Dir {
			visit = append(visit, node.Nodes...)
		} else {
			var off, err = strconv.ParseInt(node.Value, 16, 64)
			if err != nil {
				log.WithFields(log.Fields{"key": key, "err": err}).Warn("can't parse offset")
			} else {
				offsets[strings.TrimPrefix(node.Key, key+"/")] = off
			}
		}
	}
	return offsets
}

// Collect successive -prefix /x/y/z flag usages into a slice.
type prefixFlagSet []string

func (f *prefixFlagSet) String() string {
	return strings.Join(*f, ",")
}

func (f *prefixFlagSet) Set(prefix string) error {
	*f = append(*f, prefix)
	return nil
}

func calcMinimumDisplayLag() uint64 {
	if thresh, err := humanize.ParseBytes(*minimumDisplayLagFlag); err != nil {
		log.WithField("err", err).WithField("flag", *minimumDisplayLagFlag).Fatal("Invalid minimum lag")
		return 0
	} else {
		return thresh
	}
}

var minimumDisplayLag uint64
