package journal

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/url"
	"sync"
)

// MemoryBroker provides an in-memory implementation of the Client interface.
// The intended use is within unit tests which exercise components coordinating
// through the Client interface.
type MemoryBroker struct {
	// DelayWrites indicates that writes should queue (and their promises not resolve) until:
	//   * The next explicit Flush, or
	//   * DelayWrites is set to false and another Write occurs.
	DelayWrites bool
	// Content written to each journal.
	Content map[Name]*bytes.Buffer
	// Pending content which will be written on the next Flush (or write, if !DelayWrites).
	Pending map[Name]*bytes.Buffer

	// Next promise which will resolve on the next write flush.
	promise chan struct{}
	// |cond| signals to blocked Get operations that content may be available.
	cond *sync.Cond
	mu   sync.Mutex
}

// NewMemoryBroker returns an initialized, zero-value MemoryBroker.
func NewMemoryBroker() *MemoryBroker {
	var j = &MemoryBroker{
		Content: make(map[Name]*bytes.Buffer),
		Pending: make(map[Name]*bytes.Buffer),
		promise: make(chan struct{}),
	}
	j.cond = sync.NewCond(&j.mu)

	return j
}

func (j *MemoryBroker) Write(name Name, b []byte) (*AsyncAppend, error) {
	return j.ReadFrom(name, bytes.NewReader(b))
}

func (j *MemoryBroker) ReadFrom(name Name, r io.Reader) (*AsyncAppend, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	var cur, next = obtainBuffer(name, j.Content), obtainBuffer(name, j.Pending)
	var _, err = next.ReadFrom(r)

	var result = &AsyncAppend{
		Ready:        j.promise,
		AppendResult: AppendResult{WriteHead: int64(cur.Len() + next.Len())},
	}

	if !j.DelayWrites {
		j.flush()
	}
	return result, err
}

func (j *MemoryBroker) Get(args ReadArgs) (ReadResult, io.ReadCloser) {
	j.mu.Lock()
	defer j.mu.Unlock()

	var buf = obtainBuffer(args.Journal, j.Content)

	if args.Offset == -1 {
		args.Offset = int64(buf.Len())
	}

	for {
		if int(args.Offset) < buf.Len() {
			return ReadResult{
				Offset:    args.Offset,
				WriteHead: int64(buf.Len()),
				Fragment:  Fragment{Begin: 0, End: int64(buf.Len())},
			}, ioutil.NopCloser(bytes.NewReader(buf.Bytes()[args.Offset:]))
		} else if !args.Blocking {
			return ReadResult{
				Error:     ErrNotYetAvailable,
				WriteHead: int64(buf.Len()),
			}, nil
		} else if err := args.Context.Err(); err != nil {
			return ReadResult{Error: err}, nil
		}

		j.cond.Wait()
	}
	panic("not reached")
}

func (j *MemoryBroker) Head(args ReadArgs) (ReadResult, *url.URL) {
	panic("not yet implemented")
}

func (j *MemoryBroker) Create(journal Name) error {
	panic("not yet implemented")
}

// Flush resolves all pending writes and wakes any blocked read operations.
func (j *MemoryBroker) Flush() {
	j.mu.Lock()
	j.flush()
	j.mu.Unlock()
}

func (j *MemoryBroker) flush() {
	defer j.cond.Broadcast()

	var promise = j.promise
	j.promise = make(chan struct{})
	defer close(promise)

	for name, src := range j.Pending {
		obtainBuffer(name, j.Content).ReadFrom(src)
		src.Reset()
	}
}

func obtainBuffer(name Name, m map[Name]*bytes.Buffer) *bytes.Buffer {
	var buf, ok = m[name]
	if !ok {
		buf = new(bytes.Buffer)
		m[name] = buf
	}
	return buf
}
