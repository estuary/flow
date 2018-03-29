package gazette

import (
	"bytes"
	"flag"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/metrics"
)

var (
	// Time to wait in between broker write errors. Exposed for debugging.
	writeServiceCoolOffTimeout = time.Second * 5

	writeConcurrency = flag.Int("gazetteWriteConcurrency", 4,
		"Concurrency of asynchronous, locally-spooled Gazette write client")
)

const (
	kMaxWriteSpoolSize = 1 << 27 // A single spool is up to 128MiB.
	kWriteQueueSize    = 1024    // Allows a total of 128GiB of spooled writes.

	// Local disk-backed temporary directory where pending writes are spooled.
	gazetteWriteTmpDir = "/var/tmp/gazette-writes"
)

type pendingWrite struct {
	journal journal.Name
	file    *os.File
	offset  int64
	started time.Time
	result  *journal.AsyncAppend
}

var pendingWritePool = sync.Pool{
	New: func() interface{} {
		f, err := ioutil.TempFile(gazetteWriteTmpDir, "gazette-write")
		if err != nil {
			return err
		}
		// File is collected as soon as this final descriptor is closed.
		// Note this means that Stat/Truncate/etc will no longer succeed.
		os.Remove(f.Name())

		write := &pendingWrite{file: f}
		return write
	}}

func releasePendingWrite(p *pendingWrite) error {
	*p = pendingWrite{file: p.file}
	if _, err := p.file.Seek(0, 0); err != nil {
		return err
	}
	pendingWritePool.Put(p)
	return nil
}

func writeAllOrNone(write *pendingWrite, r io.Reader) error {
	n, err := io.Copy(write.file, r)
	if err == nil {
		write.offset += int64(n)
	} else {
		write.file.Seek(write.offset, 0)
	}
	return err
}

// WriteService wraps a Client to provide asynchronous batching and automatic retries
// of writes to Gazette journals. Writes to each journal are spooled to local
// disk (and never memory), so back-pressure from slow or down brokers does not
// affect busy writers (at least, until disk runs out). Writes are retried
// indefinitely, until aknowledged by a broker.
type WriteService struct {
	client  *Client
	stopped chan struct{} // Coordinates exit of service loops.

	// Concurrent write queues (defaults to *writeConcurrency).
	writeQueue []chan *pendingWrite

	// Indexes pendingWrite's which are in |writeQueue|, and still append-able.
	writeIndex   map[journal.Name]*pendingWrite
	writeIndexMu sync.Mutex
}

func NewWriteService(client *Client) *WriteService {
	var writeService = &WriteService{
		client:     client,
		stopped:    make(chan struct{}),
		writeQueue: nil,
		writeIndex: make(map[journal.Name]*pendingWrite),
	}

	writeService.SetConcurrency(*writeConcurrency)

	return writeService
}

func (c *WriteService) SetConcurrency(concurrency int) {
	c.writeQueue = make([]chan *pendingWrite, concurrency)

	for i := range c.writeQueue {
		c.writeQueue[i] = make(chan *pendingWrite, kWriteQueueSize)
	}
}

// Begins the write service loop. Be sure to invoke Stop() prior to process
// exit, to ensure that all pending writes have been flushed.
func (c *WriteService) Start() {
	var err = os.MkdirAll(gazetteWriteTmpDir, 0700)
	if err != nil {
		panic(err)
	}

	for i := range c.writeQueue {
		go c.serveWrites(i)
	}
}

// Stops the write service loop. Returns only after all writes have completed.
func (c *WriteService) Stop() {
	for i := range c.writeQueue {
		close(c.writeQueue[i])
	}
	for _ = range c.writeQueue {
		<-c.stopped
	}
}

func (c *WriteService) obtainWrite(name journal.Name) (*pendingWrite, bool, error) {
	// Is a non-full pendingWrite for this journal already in |writeQueue|?
	write, ok := c.writeIndex[name]
	if ok && write.offset < kMaxWriteSpoolSize {
		return write, false, nil
	}
	popped := pendingWritePool.Get()

	if err, ok := popped.(error); ok {
		return nil, false, err
	} else {
		write = popped.(*pendingWrite)
		write.journal = name
		write.result = &journal.AsyncAppend{
			Ready: make(chan struct{}),
		}
		write.started = time.Now()
		c.writeIndex[name] = write
		return write, true, nil
	}
}

// Appends |buffer| to |journal|. Either all of |buffer| is written, or none
// of it is. Returns a AsyncAppendwhich is resolved when the write has
// been fully committed.
func (c *WriteService) Write(name journal.Name, buf []byte) (*journal.AsyncAppend, error) {
	return c.ReadFrom(name, bytes.NewReader(buf))
}

// Appends |r|'s content to |journal|, by reading until io.EOF. Either all of
// |r| is written, or none of it is. Returns an AsyncAppend which is
// resolved when the write has been fully committed.
func (c *WriteService) ReadFrom(name journal.Name, r io.Reader) (*journal.AsyncAppend, error) {
	var result *journal.AsyncAppend
	var writeErr error

	c.writeIndexMu.Lock()
	write, isNew, obtainErr := c.obtainWrite(name)
	if obtainErr == nil {
		writeErr = writeAllOrNone(write, r)
		result = write.result // Retain, as we can't access |write| after unlock.
	}
	c.writeIndexMu.Unlock()

	if obtainErr != nil {
		return nil, obtainErr
	}
	if isNew {
		// Hash |name| to identify a service loop to queue |write| on. This allows
		// for multiple, concurrent service loops while ensuring that |writes| from
		// a single client are strictly in-order.
		route := int(crc32.Checksum([]byte(name), crc32.IEEETable))
		c.writeQueue[route%len(c.writeQueue)] <- write
	}
	return result, writeErr
}

func (c *WriteService) serveWrites(index int) {
	for {
		write := <-c.writeQueue[index]
		if write == nil {
			// Signals Close().
			break
		}

		c.writeIndexMu.Lock()
		if c.writeIndex[write.journal] == write {
			delete(c.writeIndex, write.journal)
		}
		c.writeIndexMu.Unlock()

		if err := c.onWrite(write); err != nil {
			metrics.GazetteWriteFailureTotal.Inc()
			log.WithFields(log.Fields{"journal": write.journal, "err": err}).
				Error("write failed")
		}
	}
	c.stopped <- struct{}{} // Signal exit.
}

func (c *WriteService) onWrite(write *pendingWrite) error {
	// We now have exclusive ownership of |write|. Iterate
	// attempting to write to server, until it's acknowledged.
	for true {
		if _, err := write.file.Seek(0, 0); err != nil {
			return err // Not recoverable
		}
		result := c.client.Put(journal.AppendArgs{
			Journal: write.journal,
			Content: io.NewSectionReader(write.file, 0, write.offset),
		})

		switch result.Error {
		case nil:
			break

		case journal.ErrNotBroker:
			// The route topology has changed, generally due to a service update.
			// Immediately retry against the indicated broker.
			continue

		case journal.ErrNotFound:
			// First-write case: Implicitly create a Journal which doesn't yet exist.
			if err := c.client.Create(write.journal); err != nil {
				log.WithFields(log.Fields{"journal": write.journal, "err": err}).
					Warn("failed to create journal")
				time.Sleep(writeServiceCoolOffTimeout)
			} else {
				log.WithField("journal", write.journal).Info("created journal")
			}
			continue

		default:
			metrics.GazetteWriteFailureTotal.Inc()
			time.Sleep(writeServiceCoolOffTimeout)
			continue
		}

		// Success. Notify any waiting clients.
		write.result.AppendResult = result
		close(write.result.Ready)

		metrics.GazetteWriteDurationTotal.Add(time.Now().Sub(write.started).Seconds())
		metrics.GazetteWriteBytesTotal.Add(float64(write.offset))
		metrics.GazetteWriteCountTotal.Inc()

		if err := releasePendingWrite(write); err != nil {
			log.WithField("err", err).Error("failed to release pending write")
		}
		return nil
	}
	panic("not reached")
}

// Adapter to allow |WriteService| to return io.Writers for arbitrary journals
// that can be written to directly.
type namedWriter struct {
	writeService *WriteService
	name         journal.Name
	sync         bool
}

func (c *WriteService) WriterFor(name journal.Name, sync bool) io.Writer {
	return &namedWriter{
		writeService: c,
		name:         name,
		sync:         sync,
	}
}

func (w *namedWriter) Write(data []byte) (int, error) {
	result, err := w.writeService.Write(w.name, data)
	if err != nil {
		return 0, err
	}
	if w.sync {
		<-result.Ready
	}
	return len(data), nil
}
