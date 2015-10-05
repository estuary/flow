package gazette

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/discovery"
	"github.com/pippio/api-server/varz"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	// Upper-bound time we'll wait for a write request to succeed. This should
	// be very large, as we never want a thundering herd where clients time out
	// an operational but slow broker, but we also need to guard against brokers
	// who die (and don't send a RST).
	kWriteClientIOTimeout = time.Minute * 5

	// Time to wait in between broker write errors.
	kWriteClientCooloffTimeout = time.Second * 5

	kMaxWriteSpoolSize = 1 << 27 // A single spool is up to 128MiB.
	kWriteQueueSize    = 1024    // Allows a total of 128GiB of spooled writes.

	// Local disk-backed temporary directory where pending writes are spooled.
	kWriteTmpDirectory = "/var/tmp/gazette-writes"
)

type pendingWrite struct {
	journal string
	file    *os.File
	offset  int64
	started time.Time

	// Signals successful write.
	promise Promise
}

var pendingWritePool = sync.Pool{
	New: func() interface{} {
		err := os.MkdirAll(kWriteTmpDirectory, 0700)
		if err != nil {
			return err
		}
		f, err := ioutil.TempFile(kWriteTmpDirectory, "gazette-write")
		if err != nil {
			return err
		}
		// File is collected as soon as this final descriptor is closed.
		// Note this means that Stat/Truncate/etc will no longer succeed.
		os.Remove(f.Name())

		write := &pendingWrite{file: f}
		return write
	}}

func releasePendingWrite(p *pendingWrite) {
	*p = pendingWrite{file: p.file}
	if _, err := p.file.Seek(0, 0); err != nil {
		log.WithField("err", err).Warn("failed to seek(0) releasing pending write")
	} else {
		pendingWritePool.Put(p)
	}
}

func writeAllOrNone(write *pendingWrite, buffer []byte) error {
	n, err := write.file.Write(buffer)
	if err == nil {
		write.offset += int64(n)
	} else {
		write.file.Seek(write.offset, 0)
	}
	return err
}

type WriteClient struct {
	avgWriteSize *varz.Average
	avgWriteMs   *varz.Average
	writeBytes   *varz.Count

	writeQueue chan *pendingWrite
	writeMap   map[string]*pendingWrite // Still in |writeQueue| and append-able.
	router     discovery.HRWRouter
	mu         sync.Mutex

	closed chan struct{}
}

func NewWriteClient(gazetteContext *discovery.KeyValueService) *WriteClient {
	client := &WriteClient{
		avgWriteSize: varz.ObtainAverage("gazette", "avgWriteSize"),
		avgWriteMs:   varz.ObtainAverage("gazette", "avgWriteMs"),
		writeBytes:   varz.ObtainCount("gazette", "writeBytes"),
		writeQueue:   make(chan *pendingWrite, kWriteQueueSize),
		writeMap:     make(map[string]*pendingWrite),
	}
	// Use a replica-count of 1 (because we always route to the broker),
	// and we don't want to observe route changes.
	client.router = discovery.NewHRWRouter(1, nil)
	gazetteContext.AddObserver(MembersPrefix, client.onMembershipChange)

	// TODO(johnny): Could have multiple workers to improve coalescing.
	go client.serveWrites()
	return client
}

func (c *WriteClient) Close() {
	c.writeQueue <- nil
	<-c.closed
}

func (c *WriteClient) onMembershipChange(members, old,
	new discovery.KeyValues) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.router.RebuildRoutes(members, old, new)
}

func (c *WriteClient) obtainWrite(
	journal string) (*pendingWrite, bool, error) {
	// Is a non-full pendingWrite for this journal already in |writeQueue|?
	write, ok := c.writeMap[journal]
	if ok && write.offset < kMaxWriteSpoolSize {
		return write, false, nil
	}
	popped := pendingWritePool.Get()

	if err, ok := popped.(error); ok {
		return nil, false, err
	} else {
		write = popped.(*pendingWrite)
		write.journal = journal
		write.promise = make(Promise)
		write.started = time.Now()
		c.writeMap[journal] = write
		return write, true, nil
	}
}

// Appends |buf| to |journal|. The returned Promise is resolved when the
// write completes, so that the client may optionally block on it.
func (c *WriteClient) Write(journal string, buf []byte) (Promise, error) {
	var promise Promise

	c.mu.Lock()
	write, isNew, err := c.obtainWrite(journal)
	if err == nil {
		err = writeAllOrNone(write, buf)
		promise = write.promise // Retain, as we can't access |write| after unlock.
	}
	c.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if isNew {
		c.writeQueue <- write
	}
	return promise, nil
}

func (c *WriteClient) serveWrites() {
	for {
		write := <-c.writeQueue
		if write == nil {
			// Signals Close().
			break
		}

		c.mu.Lock()
		if c.writeMap[write.journal] == write {
			delete(c.writeMap, write.journal)
		}
		c.mu.Unlock()

		if err := c.onWrite(write); err != nil {
			log.WithFields(log.Fields{"journal": write.journal, "err": err}).
				Error("write failed")
		}
	}
	close(c.closed)
}

func (c *WriteClient) onWrite(write *pendingWrite) error {
	var client = http.Client{
		Timeout:   kWriteClientIOTimeout,
		Transport: http.DefaultTransport,
	}
	// We now have exclusive ownership of |write|. Iterate
	// attempting to write to server, until it's acknowledged.
	for i := 0; true; i++ {
		if i != 0 {
			time.Sleep(kWriteClientCooloffTimeout)
		}

		c.mu.Lock()
		route := c.router.Route(write.journal)
		c.mu.Unlock()

		if len(route) == 0 {
			log.WithField("journal", write.journal).Warn("no broker route")
			continue
		}
		ep := route[0].Value.(*discovery.Endpoint)

		if _, err := write.file.Seek(0, 0); err != nil {
			return err // Not recoverable
		}
		request, err := ep.NewHTTPRequest("PUT", "/"+write.journal,
			io.LimitReader(write.file, write.offset))
		if err != nil {
			return err // Not recoverable
		}

		response, err := client.Do(request)
		if err != nil {
			log.WithFields(log.Fields{"journal": write.journal, "err": err}).
				Warn("write failed")
			continue
		} else if response.StatusCode != http.StatusNoContent {
			log.WithFields(log.Fields{"journal": write.journal,
				"status": response.Status}).Warn("write failed")
			response.Body.Close()
			continue
		} else {
			// Success. Notify any waiting clients.
			write.promise.Resolve()

			c.avgWriteSize.Add(float64(write.offset))
			c.avgWriteMs.Add(float64(time.Now().Sub(write.started)) /
				float64(time.Millisecond))
			c.writeBytes.Add(write.offset)

			releasePendingWrite(write)
			return nil
		}
	}
	panic("not reached")
}
