package gazette

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/api-server/discovery"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"
)

var (
	kContentRangeRegexp = regexp.MustCompile("bytes\\s+(\\d+)-\\d+/\\d+")
)

// TODO(johnny): Track last use of each journal, and periodically clear out
// indices for those not in use.
type ReadClient struct {
	router discovery.HRWRouter

	cfs      cloudstore.FileSystem
	journals map[string]readClientIndex

	mu sync.Mutex
}

type readClientIndex struct {
	updates chan Fragment
	watcher *IndexWatcher
	tail    *Tail
}

func NewReadClient(gazetteContext *discovery.KeyValueService,
	cfs cloudstore.FileSystem, replicas int) *ReadClient {

	client := &ReadClient{
		cfs:      cfs,
		journals: make(map[string]readClientIndex),
	}
	// Use a nil observer, as we don't want to track route changes.
	client.router = discovery.NewHRWRouter(replicas, nil)
	gazetteContext.AddObserver(MembersPrefix, client.onMembershipChange)

	return client
}

func (c *ReadClient) HeadJournalAt(journal string, offset int64) (*http.Response, error) {
	c.mu.Lock()
	route := c.router.Route(journal)
	c.mu.Unlock()

	if len(route) == 0 {
		return nil, errors.New("no replica route")
	}
	ep := route[rand.Int()%len(route)].Value.(*discovery.Endpoint)

	request, err := ep.NewHTTPRequest("HEAD", "/"+journal, nil)
	if err != nil {
		return nil, err
	}

	if response, err := http.DefaultClient.Do(request); err != nil {
		return nil, err
	} else if response.StatusCode != http.StatusOK {
		return nil, errors.New("server returned bad status")
	} else if offsetStr := response.Header.Get(WriteHeadHeader); offsetStr == "" {
		return nil, errors.New("expected " + WriteHeadHeader + " header")
	} else {
		return response, nil
	}
}

func (c *ReadClient) OpenJournalAt(journal string,
	offset int64) (io.ReadCloser, error) {

	c.mu.Lock()
	index := c.obtainJournalIndex(journal)
	c.mu.Unlock()

	// Defer evaluating a read until the index is fully loaded.
	index.watcher.WaitForInitialLoad()

	op := ReadOp{
		Journal:  journal,
		Offset:   offset,
		Blocking: false,
		Result:   make(chan ReadResult, 1),
	}
	index.tail.Read(op)
	result := <-op.Result

	if result.Error == nil {
		return result.Fragment.ReaderFromOffset(offset, c.cfs)
	} else if result.Error != ErrNotYetAvailable {
		return nil, result.Error
	}

	// Not available in the persisted index. Ask a current replica for a reader.
	c.mu.Lock()
	route := c.router.Route(journal)
	c.mu.Unlock()

	if len(route) == 0 {
		return nil, errors.New("no replica route")
	}
	ep := route[rand.Int()%len(route)].Value.(*discovery.Endpoint)

	request, err := ep.NewHTTPRequest("GET", "/"+journal+"?"+url.Values{
		"offset": {strconv.FormatInt(offset, 10)},
		"block":  {"true"}}.Encode(), nil)
	if err != nil {
		return nil, err
	}

	if responseOffset, response, err := c.doReadRequest(request); err != nil {
		return nil, err
	} else if offset != -1 && responseOffset != offset {
		// Server returns a different offset only when reading from write head (-1).
		return nil, errors.New("server returned wrong read head")
	} else {
		return response.Body, nil
	}
}

func (c *ReadClient) obtainJournalIndex(journal string) readClientIndex {
	index, ok := c.journals[journal]
	if !ok {
		index.updates = make(chan Fragment)
		index.watcher = NewIndexWatcher(journal, c.cfs, index.updates).
			StartWatchingIndex()
		index.tail = NewTail(journal, index.updates).StartServingOps()

		c.journals[journal] = index
	}
	return index
}

func (c *ReadClient) onMembershipChange(members, old, new discovery.KeyValues) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.router.RebuildRoutes(members, old, new)
}

func (c *ReadClient) doReadRequest(request *http.Request) (
	int64, *http.Response, error) {

	log.WithFields(log.Fields{"url": request.URL.String()}).
		Info("issuing gazette read")

	if response, err := http.DefaultClient.Do(request); err != nil {
		return 0, nil, err
	} else if response.StatusCode != http.StatusPartialContent {
		body, _ := ioutil.ReadAll(response.Body)
		return 0, nil, errors.New(string(body))
	} else if crStr := response.Header.Get("Content-Range"); crStr == "" {
		return 0, nil, errors.New("expected Content-Range header")
	} else if m := kContentRangeRegexp.FindStringSubmatch(crStr); len(m) == 0 {
		return 0, nil, errors.New("invalid Content-Range " + crStr)
	} else if rOffset, err := strconv.ParseInt(m[1], 10, 64); err != nil {
		return 0, nil, err
	} else {
		return rOffset, response, nil
	}
}

/*
func (r *JournalReadClient) Stop() {
	r.index.Stop()
	close(r.updates)
	r.tail.Stop()
}
*/
