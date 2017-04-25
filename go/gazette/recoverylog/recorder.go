package recoverylog

import (
	"bytes"
	"crypto/rand"
	"io"
	"io/ioutil"
	"math"
	"math/big"
	"path/filepath"
	"sync"

	log "github.com/Sirupsen/logrus"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

// Well-known RocksDB paths which should be treated as properties,
// rather than tracked as Fnodes.
var propertyFiles = map[string]struct{}{
	// Database GUID created at initialization of empty database.
	"/IDENTITY": {},
}

// Recorder observes a sequence of changes to a file-system, and preserves
// those changes via a written Gazette journal of file-system operations.
type Recorder struct {
	fsm *FSM
	// Generated unique ID of this Recorder.
	id Author
	// Prefix length to strip from filenames in recorded operations.
	stripLen int
	// Client for interacting with |opLog|.
	writer journal.Writer
	// A recent write, which will be used to update the FSM Offset once committed.
	pendingWrite *journal.AsyncAppend
	// Used to serialize access to |fsm| and writes to |opLog|.
	mu sync.Mutex
}

func NewRecorder(fsm *FSM, stripLen int, writer journal.Writer) (*Recorder, error) {
	recorderId, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32-1))
	if err != nil {
		return nil, err
	}

	recorder := &Recorder{
		fsm:      fsm,
		id:       Author(recorderId.Int64()) + 1,
		stripLen: stripLen,
		writer:   writer,
	}

	// Issue an initial WriteBarrier to determine a lower-bound offset
	// for all subsequent recorded operations.
	op := recorder.WriteBarrier()
	<-op.Ready
	recorder.fsm.LogMark.Offset = op.WriteHead

	return recorder, nil
}

// Note that we can't ever fail to write some portion of the recorded log, and
// then return control back to the database (and its client). To do so would
// allow for inconsistency in the local database state, vs the recorded log. For
// this reason, Recorder's implementation is crash-only and Panic()s on error.

func (r *Recorder) normalizePath(path string) string {
	return filepath.Clean(path[r.stripLen:])
}

// rocks.EnvObserver implementation.
func (r *Recorder) NewWritableFile(path string) rocks.WritableFileObserver {
	path = r.normalizePath(path)

	if _, isProperty := propertyFiles[path]; isProperty {
		log.WithField("path", path).Panic("unexpected open of property path")
	}

	defer r.mu.Unlock()
	r.mu.Lock()

	prevFnode, prevExists := r.fsm.Links[path]

	// Decompose the creation into two operations:
	//  * Unlinking |prevFnode| linked at |path| if |prevExists|.
	//  * Creating the new fnode backing |path|.
	var frame []byte

	if prevExists {
		frame = r.process(RecordedOp{
			Unlink: &RecordedOp_Link{Fnode: prevFnode, Path: path}}, nil)
	}
	frame = r.process(RecordedOp{Create: &RecordedOp_Create{Path: path}}, frame)

	// Perform an atomic write of both operations.
	r.recordFrame(frame)
	return &fileRecorder{r, r.fsm.Links[path], 0}
}

// rocks.EnvObserver implementation.
func (r *Recorder) DeleteFile(path string) {
	path = r.normalizePath(path)

	if _, isProperty := propertyFiles[path]; isProperty {
		log.WithField("path", path).Panic("unexpected delete of property path")
	}

	defer r.mu.Unlock()
	r.mu.Lock()

	fnode, ok := r.fsm.Links[path]
	if !ok {
		log.WithFields(log.Fields{"path": path}).Panic("delete of unknown path")
	}

	r.recordFrame(r.process(
		RecordedOp{Unlink: &RecordedOp_Link{Fnode: fnode, Path: path}}, nil))
}

// rocks.EnvObserver implementation.
func (r *Recorder) DeleteDir(dirname string) { /* No-op */ }

// rocks.EnvObserver implementation.
func (r *Recorder) LinkFile(src, target string) {
	src, target = r.normalizePath(src), r.normalizePath(target)

	if _, isProperty := propertyFiles[target]; isProperty {
		log.WithFields(log.Fields{"src": src, "target": target}).
			Panic("unexpected link of property path")
	}

	defer r.mu.Unlock()
	r.mu.Lock()

	fnode, ok := r.fsm.Links[src]
	if !ok {
		log.WithFields(log.Fields{"path": src}).Panic("link of unknown path")
	}

	r.recordFrame(r.process(
		RecordedOp{Link: &RecordedOp_Link{Fnode: fnode, Path: target}}, nil))
}

// rocks.EnvObserver implementation.
func (r *Recorder) RenameFile(srcPath, targetPath string) {
	src, target := r.normalizePath(srcPath), r.normalizePath(targetPath)

	defer r.mu.Unlock()
	r.mu.Lock()

	fnode, ok := r.fsm.Links[src]
	if !ok {
		log.WithFields(log.Fields{"path": src}).Panic("link of unknown path")
	}
	prevFnode, prevExists := r.fsm.Links[target]

	// Decompose the rename into multiple operations:
	//  * Unlinking |prevFnode| linked at |target| if |prevExists|.
	//  * If |target| is a property, recording a property update.
	//  * If |target| is not a property, linking the |fnode| to |target|.
	//  * Unlinking the |fnode| from |src|.
	var frame []byte

	if prevExists {
		frame = r.process(RecordedOp{
			Unlink: &RecordedOp_Link{Fnode: prevFnode, Path: target}}, frame)
	}

	if _, isProperty := propertyFiles[target]; isProperty {
		content, err := ioutil.ReadFile(targetPath)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "path": targetPath}).Panic("reading file")
		}
		frame = r.process(RecordedOp{
			Property: &Property{Path: target, Content: string(content)}}, frame)
	} else {
		frame = r.process(RecordedOp{
			Link: &RecordedOp_Link{Fnode: fnode, Path: target}}, frame)
	}

	frame = r.process(RecordedOp{
		Unlink: &RecordedOp_Link{Fnode: fnode, Path: src}}, frame)

	// Perform an atomic write of all four potential operations.
	r.recordFrame(frame)
}

// Builds and returns a set of state-machine hints which may be used to fully
// reconstruct the state of this Recorder.
func (r *Recorder) BuildHints() FSMHints {
	defer r.mu.Unlock()
	r.mu.Lock()

	return r.fsm.BuildHints()
}

// Issues an empty write. When this barrier write completes, it is
// guaranteed that all content written prior to barrier has also committed.
func (r *Recorder) WriteBarrier() *journal.AsyncAppend {
	defer r.mu.Unlock()
	r.mu.Lock()

	return r.recordFrame(nil)
}

func (r *Recorder) process(op RecordedOp, b []byte) []byte {
	if r.fsm.NextSeqNo == 0 {
		op.SeqNo = 1
	} else {
		op.SeqNo = r.fsm.NextSeqNo
	}
	op.Checksum = r.fsm.NextChecksum
	op.Author = r.id

	var err error
	var offset = len(b)

	if b, err = topic.FixedFraming.Encode(&op, b); err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("framing")
	}
	if err = r.fsm.Apply(&op, b[offset + topic.FixedFrameHeaderLength:]); err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("recorder FSM error")
	}
	return b
}

type fileRecorder struct {
	*Recorder

	// File being tracked, and the next write offset within the file.
	fnode  Fnode
	offset int64
}

// rocks.EnvObserver implementation.
func (r *fileRecorder) Append(data []byte) {
	defer r.mu.Unlock()
	r.mu.Lock()

	var frame = r.process(RecordedOp{Write: &RecordedOp_Write{
		Fnode:  r.fnode,
		Offset: r.offset,
		Length: int64(len(data)),
	}}, nil)

	// Perform an atomic write of the operation and its data.
	r.recordFromReader(io.MultiReader(
		bytes.NewReader(frame),
		bytes.NewReader(data)))

	r.offset += int64(len(data))
}

// rocks.EnvObserver implementation.
func (r *fileRecorder) Close()                         {}
func (r *fileRecorder) Sync()                          { <-r.WriteBarrier().Ready }
func (r *fileRecorder) Fsync()                         { <-r.WriteBarrier().Ready }
func (r *fileRecorder) RangeSync(offset, nbytes int64) { <-r.WriteBarrier().Ready }

func (r *Recorder) recordFromReader(frame io.Reader) *journal.AsyncAppend {
	result, err := r.writer.ReadFrom(r.fsm.LogMark.Journal, frame)
	if err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
	r.updateWriteHead(result)
	return result
}

func (r *Recorder) recordFrame(frame []byte) *journal.AsyncAppend {
	result, err := r.writer.Write(r.fsm.LogMark.Journal, frame)
	if err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
	r.updateWriteHead(result)
	return result
}

// We want to regularly shift forward the FSM offset to reflect operations
// which have been recorded, so that we minimize the amount of recovery log
// which must be read on playback. We additionally want to use WriteHeads
// returned directly from Gazette (rather than, eg, counting bytes) as this
// provides a tight bound while still being correct in the case of competing
// writes from multiple Recorders. With each issued write, we check whether
// a previously retained write has completed and update the FSM offset if so.
func (r *Recorder) updateWriteHead(write *journal.AsyncAppend) {
	if r.pendingWrite == nil {
		r.pendingWrite = write
	} else if r.pendingWrite.Ready == nil {
		// Indicates |pendingWrite| was modified outside of Recorder.
		panic("pendingWrite.Ready is nil")
	}

	select {
	case <-r.pendingWrite.Ready:
		// A previous append operation has completed. Update from the returned
		// WriteHead, and track |write| as the next |pendingWrite|.
		r.fsm.LogMark.Offset = r.pendingWrite.WriteHead
		r.pendingWrite = write
	default:
		// |pendingWrite| hasn't committed yet. Drop |write|.
		return
	}
}
