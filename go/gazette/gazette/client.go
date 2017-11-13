package gazette

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/journal"
	"github.com/LiveRamp/gazette/keepalive"
	"github.com/LiveRamp/gazette/metrics"
)

//go:generate mockery -inpkg -name=httpClient

const (
	// By default, all client operations are applied against the default
	// endpoint. However, the client will cache the last |kClientRouteCacheSize|
	// redirect or Location: headers received for distinct paths, and directly
	// route future requests to cached locations. This allows the client to
	// discover direct, responsible endpoints for journals it uses.
	kClientRouteCacheSize = 1024

	statsJournalBytes = "bytes"
	statsJournalHead  = "head"
)

type httpClient interface {
	Do(*http.Request) (*http.Response, error)
	Get(url string) (*http.Response, error)
}

type requestData struct {
	Method    string
	Timestamp time.Time
}

type currentRequestList struct {
	m  map[string]requestData
	mu sync.Mutex
}

var kContentRangeRegexp = regexp.MustCompile("bytes\\s+(\\d+)-\\d+/\\d+")

type Client struct {
	// Endpoint which is queried by default.
	defaultEndpoint *url.URL

	// Maps request.URL.Path to previously-received "Location:" headers,,
	// stripped of URL query arguments. Future requests of the same URL path are
	// first attempted against the cached endpoint.
	locationCache *lru.Cache

	// Exported reader/writer statistics, and a mutex to guard creation of journal
	// specific entries in the maps.
	stats struct {
		sync.Mutex
		readers, writers *expvar.Map
	}
	// Expvar'd list of timestamped, in-flight requests, for debugging hung
	// requests.
	requests *currentRequestList

	// Underlying HTTP Client to use for all requests.
	httpClient httpClient
	// Test support: allow time.Now() to be swapped out.
	timeNow func() time.Time
}

// NewClient returns a new Client. To export metrics, register the
// prometheus.Collector instances in metrics.GazetteClientCollectors().
func NewClient(endpoint string) (*Client, error) {
	return NewClientWithHttpClient(endpoint, &http.Client{})
}

func NewClientWithHttpClient(endpoint string, hc *http.Client) (*Client, error) {
	// Assume HTTP if no protocol is specified.
	if strings.Index(endpoint, "://") == -1 {
		endpoint = "http://" + endpoint
	}

	ep, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	cache, err := lru.New(kClientRouteCacheSize)
	if err != nil {
		return nil, err
	}

	// If an API consumer sets his own transport, respect it, though things may
	// fail if (for example) the file URL handler is not set.
	if hc.Transport == nil {
		hc.Transport = MakeHttpTransport()
	}

	c := &Client{
		defaultEndpoint: ep,
		locationCache:   cache,
		httpClient:      hc,
		requests:        &currentRequestList{m: make(map[string]requestData)},
		timeNow:         time.Now,
	}

	// Create expvar skeleton under /gazette.
	c.stats.readers = new(expvar.Map).Init()
	c.stats.writers = new(expvar.Map).Init()

	gazetteMap.Set("readers", c.stats.readers)
	gazetteMap.Set("writers", c.stats.writers)
	gazetteMap.Set("requests", c.requests)

	return c, nil
}

// If you want to use your own |http.Transport| with Gazette, start with this one.
func MakeHttpTransport() *http.Transport {
	// See definition of |http.DefaultTransport| here:
	// https://golang.org/pkg/net/http/#RoundTripper
	// We don't use |http.DefaultTransport| itself, as it is difficult to
	// deep-copy it.
	var httpTransport = &http.Transport{
		Dial: keepalive.Dialer.Dial,
		// Force cloud storage to decompress fragments. Go's standard `gzip`
		// package is several times slower than zlib, and we additionally see a
		// parallelism benefit when multiple fragments are fetched concurrently.
		DisableCompression:    true,
		ExpectContinueTimeout: 1 * time.Second,
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	// When testing, fragment locations are "persisted" to the local filesystem,
	// and file:// URL's are returned by Gazette servers. Register a protocol
	// handler so they may be opened by the client.
	httpTransport.RegisterProtocol("file", http.NewFileTransport(http.Dir("/")))

	return httpTransport
}

func (c *Client) Head(args journal.ReadArgs) (journal.ReadResult, *url.URL) {
	request, err := http.NewRequest("HEAD", c.buildReadURL(args).String(), nil)
	if err != nil {
		return journal.ReadResult{Error: err}, nil
	}
	if args.Context != nil {
		request = request.WithContext(args.Context)
	}
	response, err := c.Do(request)
	if err != nil {
		return journal.ReadResult{Error: err}, nil
	}

	result, fragmentLocation := c.parseReadResult(args, response)
	response.Body.Close()
	return result, fragmentLocation
}

func (c *Client) GetDirect(args journal.ReadArgs) (journal.ReadResult, io.ReadCloser) {
	request, err := http.NewRequest("GET", c.buildReadURL(args).String(), nil)
	if err != nil {
		return journal.ReadResult{Error: err}, nil
	}
	if args.Context != nil {
		request = request.WithContext(args.Context)
	}
	response, err := c.Do(request)
	if err != nil {
		return journal.ReadResult{Error: err}, nil
	}

	result, _ := c.parseReadResult(args, response)
	if result.Error != nil {
		response.Body.Close()
		return result, nil
	}
	return result, c.makeReadStatsWrapper(response.Body, args.Journal, result.Offset)
}

func (c *Client) Get(args journal.ReadArgs) (journal.ReadResult, io.ReadCloser) {
	// Perform a non-blocking HEAD first, to check for an available persisted fragment.
	headArgs := args
	headArgs.Blocking = false
	headArgs.Deadline = time.Time{}
	result, fragmentLocation := c.Head(headArgs)

	if result.Error == journal.ErrNotYetAvailable {
		// Fall-through, re-attempting request as a GET.
	} else if result.Error != nil {
		return result, nil
	} else if fragmentLocation != nil {
		if body, err := c.openFragment(fragmentLocation, result); err != nil {
			result.Error = err
			return result, nil
		} else {
			return result, c.makeReadStatsWrapper(body, args.Journal, result.Offset)
		}
	}
	// No persisted fragment is available. We must repeat the request as a GET.
	// Data will be streamed directly from the server.
	return c.GetDirect(args)
}

func (c *Client) obtainJournalCounters(name journal.Name, isWrite bool, offset int64) (counter *expvar.Int, head *expvar.Int) {
	var root *expvar.Map
	if isWrite {
		root = c.stats.writers
	} else {
		root = c.stats.readers
	}

	if journalVar := root.Get(string(name)); journalVar == nil {
		c.stats.Lock()

		// Repeat the check with the lock held.
		if journalVar = root.Get(string(name)); journalVar == nil {
			journalMap := new(expvar.Map).Init()
			counter, head = new(expvar.Int), new(expvar.Int)

			head.Set(offset)
			journalMap.Set(statsJournalBytes, counter)
			journalMap.Set(statsJournalHead, head)

			// Expose |journalMap| last to prevent partial reporting.
			root.Set(string(name), journalMap)
		}

		c.stats.Unlock()
	} else {
		// |journalVar| is expected to be pre-populated as above.
		journalMap := journalVar.(*expvar.Map)
		counter = journalMap.Get(statsJournalBytes).(*expvar.Int)
		head = journalMap.Get(statsJournalHead).(*expvar.Int)
		head.Set(offset)
	}

	return
}

func (c *Client) makeReadStatsWrapper(stream io.ReadCloser, name journal.Name, offset int64) io.ReadCloser {
	expRead, expOffset := c.obtainJournalCounters(name, false, offset)

	return readStatsWrapper{
		stream: stream,
		name:   name,
		read:   expRead,
		offset: expOffset,
	}
}

// Returns a reader by reading directly from a fragment. |location| is a
// potentially signed or authorized URL to fragment storage. The fragment is
// opened, seek'd to the desired |result.Offset|, and returned. Note we don't
// use a range request here, as the fragment is usually gzip'd (and implicitly
// decompressed while being read).
func (c *Client) openFragment(location *url.URL,
	result journal.ReadResult) (io.ReadCloser, error) {

	response, err := c.httpClient.Get(location.String())
	if err != nil {
		return nil, err
	} else if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, fmt.Errorf("fetching fragment: %s", response.Status)
	}
	// Attempt to seek to |result.Offset| within the fragment.
	delta := result.Offset - result.Fragment.Begin
	if _, err := io.CopyN(ioutil.Discard, response.Body, delta); err != nil {
		response.Body.Close()
		return nil, fmt.Errorf("seeking fragment: %s", err)
	}

	var deltaF64 = float64(delta)
	metrics.GazetteReadBytesTotal.Add(deltaF64)
	metrics.GazetteDiscardBytesTotal.Add(deltaF64)
	return response.Body, nil // Success.
}

// Creates the Journal of the given name.
func (c *Client) Create(name journal.Name) error {
	url := c.defaultEndpoint // Copy.
	url.Path = "/" + name.String()

	request, err := http.NewRequest("POST", url.String(), nil)
	if err != nil {
		return err
	}
	// Issue the request without using or updating the Journal location cache.
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	return journal.ErrorFromResponse(response)
}

// Performs a Gazette PUT operation, which appends content to the named journal.
// Put panics if |args.Content| does not implement io.ReadSeeker.
func (c *Client) Put(args journal.AppendArgs) journal.AppendResult {
	request, err := http.NewRequest("PUT", "/"+args.Journal.String(), args.Content)
	if err != nil {
		return journal.AppendResult{Error: err}
	}
	if _, ok := c.locationCache.Get(request.URL.Path); !ok {
		// Speculatively issue a HEAD to fill the location cache for this path.
		result, _ := c.Head(journal.ReadArgs{Journal: args.Journal, Blocking: false, Offset: -1})
		if result.Error != nil && result.Error != journal.ErrNotYetAvailable {
			return journal.AppendResult{Error: result.Error}
		}
	}

	// Use Seek() to determine the content length, if available.
	rs := args.Content.(io.ReadSeeker)
	if start, err := rs.Seek(0, os.SEEK_CUR); err != nil {
	} else if end, err := rs.Seek(0, os.SEEK_END); err != nil {
	} else if _, err := rs.Seek(start, os.SEEK_SET); err != nil {
	} else {
		request.ContentLength = end - start
	}

	response, err := c.Do(request)
	if err != nil {
		return journal.AppendResult{Error: err}
	}
	defer response.Body.Close()
	result := c.parseAppendResponse(response)

	// Record the result.WriteHead as well as a cumulative count of all
	// bytes written to this journal, if the write succeeded.
	if result.Error == nil {
		written, _ := c.obtainJournalCounters(args.Journal, true, result.WriteHead)
		written.Add(request.ContentLength)
	}

	return result
}

func (c *Client) buildReadURL(args journal.ReadArgs) *url.URL {
	v := url.Values{
		"offset": {strconv.FormatInt(args.Offset, 10)},
		"block":  {strconv.FormatBool(args.Blocking)},
	}
	var blockms int64
	if !args.Deadline.IsZero() {
		blockms = args.Deadline.Sub(c.timeNow()).Nanoseconds() / time.Millisecond.Nanoseconds()
		v.Add("blockms", strconv.FormatInt(blockms, 10))
	}
	u := url.URL{
		Path:     "/" + string(args.Journal),
		RawQuery: v.Encode(),
	}
	return &u
}

func (c *Client) parseReadResult(args journal.ReadArgs,
	response *http.Response) (result journal.ReadResult, fragmentLocation *url.URL) {

	// Attempt to parse Content-Range offset.
	contentRangeStr := response.Header.Get("Content-Range")
	if contentRangeStr != "" {
		if m := kContentRangeRegexp.FindStringSubmatch(contentRangeStr); len(m) == 0 {
			result.Error = fmt.Errorf("invalid Content-Range: %s", contentRangeStr)
			return
		} else if offset, err := strconv.ParseInt(m[1], 10, 64); err != nil {
			// Regular expression match asserts this should parse.
			log.WithFields(log.Fields{"err": err, "match": m[1]}).Panic("failed to convert")
		} else {
			result.Offset = offset
		}
	}
	// Attempt to parse write head.
	writeHeadStr := response.Header.Get(WriteHeadHeader)
	if writeHeadStr != "" {
		if head, err := strconv.ParseInt(writeHeadStr, 10, 64); err != nil {
			result.Error = fmt.Errorf("parsing %s: %s", WriteHeadHeader, err)
			return
		} else {
			result.WriteHead = head
		}
	}

	// Attempt to parse fragment information.
	var fragmentNameStr = response.Header.Get(FragmentNameHeader)
	if fragmentNameStr != "" {
		if fragment, err := journal.ParseFragment(args.Journal, fragmentNameStr); err != nil {
			result.Error = fmt.Errorf("parsing %s: %s", FragmentNameHeader, err)
			return
		} else {
			result.Fragment = fragment
		}
	}

	// Attach |RemoteModTime| if possible.
	var fragmentLastModifiedStr = response.Header.Get(FragmentLastModifiedHeader)
	if fragmentLastModifiedStr != "" {
		var err error
		result.Fragment.RemoteModTime, err = time.Parse(http.TimeFormat, fragmentLastModifiedStr)
		if err != nil {
			result.Error = fmt.Errorf("parsing %s: %s", FragmentLastModifiedHeader, err)
			return
		}
	}

	if result.Error = journal.ErrorFromResponse(response); result.Error != nil {
		return
	}

	// We have a "success" status from the server. Expect required headers are present.
	if contentRangeStr == "" {
		result.Error = fmt.Errorf("expected Content-Range header")
		return
	} else if writeHeadStr == "" {
		result.Error = fmt.Errorf("expected %s header", WriteHeadHeader)
		return
	}
	// Fragment name is optional (it won't be available on blocked requests).

	// Fragment location is optional, but expect that it parses if present.
	if location := response.Header.Get(FragmentLocationHeader); location != "" {
		if l, err := url.Parse(location); err != nil {
			result.Error = fmt.Errorf("parsing %s: %s", FragmentLocationHeader, err)
			return
		} else {
			fragmentLocation = l
		}
	}
	return
}

// Note: This String() implementation is primarily for the benefit of expvar,
// which expects the string to be a serialized JSON object.
func (l *currentRequestList) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if msg, err := json.Marshal(l.m); err != nil {
		return fmt.Sprintf("%q", err.Error())
	} else {
		return string(msg)
	}
}

// Not to be confused with client.Put, basically a map interface with locking.
// Creates or updates a key in the locked map.
func (l *currentRequestList) Put(key string, value requestData) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.m[key] = value
}

// Not to be confused with client.Put, basically a map interface with locking.
// Deletes a key from the locked map.
func (l *currentRequestList) Delete(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.m, key)
}

func (c *Client) parseAppendResponse(response *http.Response) journal.AppendResult {
	var result = journal.AppendResult{
		Error: journal.ErrorFromResponse(response),
	}
	if writeHead := response.Header.Get(WriteHeadHeader); writeHead != "" {
		var err error
		if result.WriteHead, err = strconv.ParseInt(writeHead, 10, 64); err != nil {
			log.WithFields(log.Fields{"err": err, "writeHead": writeHead}).
				Error("error parsing write head")
		}
	}
	return result
}

// Thin layer upon http.Do(), which manages re-writes from and update to the
// Client.locationCache. Specifically, request.Path is mapped into a previously-
// stored Location re-write. If none is available, the request is re-written to
// reference the default endpoint. Cache entries are updated on successful
// redirect or response with a Location: header. On error, cache entries are
// expunged (eg, future requests are performed against the default endpoint).
func (c *Client) Do(request *http.Request) (*http.Response, error) {
	var cacheKey = request.URL.Path // We may mutate |request| later.

	// Apply a cached re-write for this request path if found.
	if cached, ok := c.locationCache.Get(cacheKey); ok {
		location := cached.(*url.URL)
		request.URL.Scheme = location.Scheme
		request.URL.User = location.User
		request.URL.Host = location.Host
		request.URL.Path = location.Path
		// Note that RawQuery is not re-written.
	} else {
		// Otherwise, re-write to use the default endpoint.
		request.URL.Scheme = c.defaultEndpoint.Scheme
		request.URL.User = c.defaultEndpoint.User
		request.URL.Host = c.defaultEndpoint.Host
		// Note that Path & RawQuery are not re-written.
	}

	c.requests.Put(request.URL.String(), requestData{request.Method, c.timeNow()})
	defer c.requests.Delete(request.URL.String())

	response, err := c.httpClient.Do(request)
	if err != nil {
		c.locationCache.Remove(cacheKey)
		return response, err
	}

	if location, err := response.Location(); err == nil {
		// The response included a Location header. Cache it for future use.
		// It probably also indicates request failure as well (30X or 404 response).
		c.locationCache.Add(cacheKey, location)
	} else if err != http.ErrNoLocation {
		log.WithField("err", err).Warn("parsing Gazette Location header")
	} else if response.Request != nil {
		// We successfully talked to a Gazette server. Cache the final request path
		// resulting in this response. If we followed a redirect chain, this will
		// cache the redirected URL for future use.
		c.locationCache.Add(cacheKey, response.Request.URL)
	}
	return response, err
}

// Returns the |Fragment| whose Modified time is closest to but prior to the
// given |t|. Can return a zeroed Fragment structure, if no fragment matches.
func (c *Client) FragmentBeforeTime(name journal.Name, t time.Time) (journal.Fragment, error) {
	var args = journal.ReadArgs{Journal: name}
	var result journal.ReadResult

	if result, _ = c.Head(args); result.Error != nil {
		return journal.Fragment{}, result.Error
	}

	// Retrieve the latest write-head of the journal. We then binary-search
	// between 0 and the write-head to locate the fragment.
	var writeHead = result.WriteHead
	var off = search(writeHead, func(off int64) bool {
		args.Offset = off

		if result, _ = c.Head(args); result.Error != nil {
			panic(result.Error)
		} else if result.Fragment.RemoteModTime.IsZero() {
			// No remote fragment means we've reached the end of
			// fragment-backed byte ranges. We assume that means it is after
			// the desired timestamp.
			return true
		} else {
			return result.Fragment.RemoteModTime.After(t)
		}
	})

	// Cannot satisfy the time |t| condition.
	if off >= writeHead {
		return journal.Fragment{}, nil
	}

	// TODO(joshk): Find a way to skip the last Head() call.
	args.Offset = off - 1
	if args.Offset < 0 {
		args.Offset = 0
	}

	if result, _ = c.Head(args); result.Error != nil {
		return journal.Fragment{}, result.Error
	} else {
		return result.Fragment, nil
	}
}

// Returns a list of |Fragment|s that service the given offset range in |journal|.
func (c *Client) FragmentsInRange(name journal.Name, minOff, maxOff int64) ([]journal.Fragment, error) {
	var off = minOff
	var fragments []journal.Fragment

	for maxOff == -1 || off < maxOff {
		var args = journal.ReadArgs{Journal: name, Offset: off}
		if result, locURI := c.Head(args); result.Error != nil {
			return nil, result.Error
		} else if locURI == nil {
			// Reached the end of offsets with fragments.
			break
		} else {
			fragments = append(fragments, result.Fragment)
			off = result.Fragment.End
		}
	}

	return fragments, nil
}

type readStatsWrapper struct {
	stream io.ReadCloser
	name   journal.Name
	read   *expvar.Int
	offset *expvar.Int
}

func (r readStatsWrapper) Read(p []byte) (n int, err error) {
	if n, err = r.stream.Read(p); err == nil {
		r.offset.Add(int64(n))
		r.read.Add(int64(n))
		metrics.GazetteReadBytesTotal.Add(float64(n))
	}
	return
}

func (r readStatsWrapper) Close() error {
	return r.stream.Close()
}

// Version of sort.Search which uses int64 parameters.
func search(n int64, f func(int64) bool) int64 {
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	var i, j int64 = 0, n
	for i < j {
		h := i + (j-i)/2 // avoid overflow when computing h
		// i ≤ h < j
		if !f(h) {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
	return i
}
