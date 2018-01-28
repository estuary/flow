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

	log "github.com/sirupsen/logrus"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/topic"
)

// Well-known RocksDB paths which should be treated as properties,
// rather than tracked as Fnodes.
var propertyFiles = map[string]struct{}{
	// Database GUID created at initialization of empty database.
	"/IDENTITY": {},
}

// Recorder observes a sequence of changes to a file-system, and preserves
// those changes via a written Gazette journal of file-system operations.
// Note that we can't ever fail to write some portion of the recorded log, and
// then return control back to the database (and its client). To do so would
// allow for inconsistency in the local database state, vs the recorded log. For
// this reason, Recorder's implementation is crash-only and Panic()s on error.
type Recorder struct {
	// State machine managing RecordedOp transitions.
	fsm *FSM
	// Generated unique ID of this Recorder.
	id Author
	// Prefix length to strip from filenames in recorded operations.
	stripLen int
	// Client for interacting with the recovery log.
	writer journal.Writer
	// Last observed write head of the recovery log journal.
	writeHead int64
	// A recent write, which will be used to update |writeHead| once committed.
	pendingWrite *journal.AsyncAppend
	// Used to serialize access to |fsm| and writes to the recovery log.
	mu sync.Mutex
}

// NewRecorder creates and returns a Recorder.
func NewRecorder(fsm *FSM, id Author, stripLen int, writer journal.Writer) *Recorder {
	var recorder = &Recorder{
		fsm:      fsm,
		id:       id,
		stripLen: stripLen,
		writer:   writer,
	}

	// Issue a write barrier to determine the current write head, which is a lower-bound offset
	// for all subsequent recorded operations.
	var op = recorder.WriteBarrier()
	<-op.Ready
	recorder.writeHead = op.WriteHead

	return recorder
}

// NewRandomAuthorID creates and returns a new, randomized Author ID.
func NewRandomAuthorID() (Author, error) {
	if id, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32-1)); err != nil {
		return 0, err
	} else {
		return Author(id.Int64()) + 1, nil
	}
}

// NewWritableFile is an implementation of rocks.EnvObserver to track the creation of new files.
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
		frame = r.process(newUnlinkOp(prevFnode, path), nil)
	}
	frame = r.process(newCreateOp(path), frame)

	// Perform an atomic write of both operations.
	r.recordFrame(frame)
	return &fileRecorder{r, r.fsm.Links[path], 0}
}

// DeleteFile is an implementation of rocks.EnvObserver to track file deletions.
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

	r.recordFrame(r.process(newUnlinkOp(fnode, path), nil))
}

// DeleteDir is an implementation of rocks.EnvObserver to track directory deletions.
func (r *Recorder) DeleteDir(dirname string) { /* No-op */ }

// LinkFile is an implementation of rocks.EnvObserver to track file hard-links.
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

	r.recordFrame(r.process(newLinkOp(fnode, target), nil))
}

// RenameFile is an implementation of rocks.EnvObserver to track file renames.
func (r *Recorder) RenameFile(srcPath, targetPath string) {
	var src, target = r.normalizePath(srcPath), r.normalizePath(targetPath)

	defer r.mu.Unlock()
	r.mu.Lock()

	var fnode, ok = r.fsm.Links[src]
	if !ok {
		log.WithFields(log.Fields{"path": src}).Panic("link of unknown path")
	}
	var prevFnode, prevExists = r.fsm.Links[target]

	// Decompose the rename into multiple operations:
	//  * Unlinking |prevFnode| linked at |target| if |prevExists|.
	//  * If |target| is a property, recording a property update.
	//  * If |target| is not a property, linking the |fnode| to |target|.
	//  * Unlinking the |fnode| from |src|.
	var frame []byte

	if prevExists {
		frame = r.process(newUnlinkOp(prevFnode, target), frame)
	}

	if _, isProperty := propertyFiles[target]; isProperty {
		var content, err = ioutil.ReadFile(targetPath)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "path": targetPath}).Panic("reading file")
		}
		frame = r.process(newPropertyOp(target, string(content)), frame)
	} else {
		frame = r.process(newLinkOp(fnode, target), frame)
	}
	frame = r.process(newUnlinkOp(fnode, src), frame)

	// Perform an atomic write of all four potential operations.
	r.recordFrame(frame)
}

// BuildHints returns FSMHints which may be played back to fully reconstruct the
// local filesystem state observed by this Recorder.
func (r *Recorder) BuildHints() FSMHints {
	defer r.mu.Unlock()
	r.mu.Lock()

	return r.fsm.BuildHints()
}

// WriteBarrier issues an zero-byte write. When this barrier write completes, it is
// guaranteed that all content written prior to the barrier has also committed.
func (r *Recorder) WriteBarrier() *journal.AsyncAppend {
	defer r.mu.Unlock()
	r.mu.Lock()

	return r.recordFrame(nil)
}

func (r *Recorder) normalizePath(path string) string {
	return filepath.Clean(path[r.stripLen:])
}

func (r *Recorder) process(op RecordedOp, b []byte) []byte {
	op.Author = r.id
	op.SeqNo = r.fsm.NextSeqNo
	op.Checksum = r.fsm.NextChecksum

	var offset = len(b)
	b = frameRecordedOp(op, b)

	// Use writeHead as a lower-bound for FirstOffset. As a meta-field, it's not
	// stored in the written frame, but is used by FSM in the production of hints.
	op.FirstOffset = r.writeHead

	if err := r.fsm.Apply(&op, b[offset+topic.FixedFrameHeaderLength:]); err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("recorder FSM error")
	}
	return b
}

func frameRecordedOp(op RecordedOp, b []byte) []byte {
	var err error
	if b, err = topic.FixedFraming.Encode(&op, b); err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("framing")
	}
	return b
}

// fileRecorder records operations applied to a specific file opened with NewWritableFile.
type fileRecorder struct {
	*Recorder

	// File being tracked, and the next write offset within the file.
	fnode  Fnode
	offset int64
}

// Append is an implementation of rocks.WriteableFileObserver to track file appends.
func (r *fileRecorder) Append(data []byte) {
	defer r.mu.Unlock()
	r.mu.Lock()

	var frame = r.process(newWriteOp(r.fnode, r.offset, int64(len(data))), nil)

	// Perform an atomic write of the operation and its data.
	r.recordFromReader(io.MultiReader(
		bytes.NewReader(frame),
		bytes.NewReader(data)))

	r.offset += int64(len(data))
}

// Close is a no-op implementation of rocks.WriteableFileObserver.
func (r *fileRecorder) Close() {}

// Sync, Fsync, and RangeSync are implementations of rocks.WritableFileObserver which
// issue and block on a WriteBarrier. This behavior means that when RocksDB issues an
// underlying file sync (eg, to ensure durability of a transaction), that sync completes
// only after all recorded operations have *also* been committed to the recovery log.
func (r *fileRecorder) Sync()                          { <-r.WriteBarrier().Ready }
func (r *fileRecorder) Fsync()                         { <-r.WriteBarrier().Ready }
func (r *fileRecorder) RangeSync(offset, nbytes int64) { <-r.WriteBarrier().Ready }

func (r *Recorder) recordFromReader(frame io.Reader) *journal.AsyncAppend {
	result, err := r.writer.ReadFrom(r.fsm.Log, frame)
	if err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
	r.updateWriteHead(result)
	return result
}

func (r *Recorder) recordFrame(frame []byte) *journal.AsyncAppend {
	result, err := r.writer.Write(r.fsm.Log, frame)
	if err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
	r.updateWriteHead(result)
	return result
}

// updateWriteHead performs a non-blocking check to determine if a recent issued
// write has completed, and if so, it updates the |writeHead| attached to Recorder.
// It may retain |write| to inspect on a future invocation. Rationale:
//
// We want to regularly shift forward the FSM offset to reflect operations
// which have been recorded, so that we minimize the amount of recovery log
// which must be read on playback. We additionally want to use write heads
// returned directly from Gazette (rather than, eg, counting bytes) as this
// provides a tight bound while still being correct in the case of competing
// writes from multiple Recorders.
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
		r.writeHead = r.pendingWrite.WriteHead
		r.pendingWrite = write
	default:
		// |pendingWrite| hasn't committed yet. Drop |write|.
		return
	}
}

func newCreateOp(path string) RecordedOp {
	return RecordedOp(RecordedOp{Create: &RecordedOp_Create{Path: path}})
}

func newLinkOp(fnode Fnode, path string) RecordedOp {
	return RecordedOp{Link: &RecordedOp_Link{Fnode: fnode, Path: path}}
}

func newUnlinkOp(fnode Fnode, path string) RecordedOp {
	return RecordedOp{Unlink: &RecordedOp_Link{Fnode: fnode, Path: path}}
}

func newWriteOp(fnode Fnode, offset, length int64) RecordedOp {
	return RecordedOp{Write: &RecordedOp_Write{Fnode: fnode, Offset: offset, Length: length}}
}

func newPropertyOp(path, content string) RecordedOp {
	return RecordedOp{Property: &Property{Path: path, Content: content}}
}
