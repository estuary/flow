package recoverylog

import (
	"bytes"
	"crypto/rand"
	"io"
	"math"
	"math/big"
	"path/filepath"
	"sync"

	log "github.com/Sirupsen/logrus"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
)

// Recorder observes a sequence of changes to a file-system, and preserves
// those changes via a written Gazette journal of file-system operations.
type Recorder struct {
	fsm *FSM
	// Generated unique ID of this Recorder.
	id uint32
	// Prefix length to strip from filenames in recorded operations.
	stripLen int
	// Client for interacting with |opLog|.
	writer journal.Writer
	// Used to serialize access to |fsm| and writes to |opLog|.
	mu sync.Mutex
}

func NewRecorder(fsm *FSM, stripLen int, writer journal.Writer) (*Recorder, error) {
	recorderId, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32-1))
	if err != nil {
		return nil, err
	}

	return &Recorder{
		fsm:      fsm,
		id:       uint32(recorderId.Int64()) + 1,
		stripLen: stripLen,
		writer:   writer,
	}, nil
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

	defer r.mu.Unlock()
	r.mu.Lock()

	frame := r.process(RecordedOp{Create: &RecordedOp_Create{Path: path}})
	fnode := r.fsm.Links[path]

	if _, err := r.writer.Write(r.fsm.LogMark.Journal, frame); err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
	return &fileRecorder{r, fnode, 0}
}

// rocks.EnvObserver implementation.
func (r *Recorder) DeleteFile(path string) {
	path = r.normalizePath(path)

	defer r.mu.Unlock()
	r.mu.Lock()

	fnode, ok := r.fsm.Links[path]
	if !ok {
		log.WithFields(log.Fields{"path": path}).Panic("delete of unknown path")
	}
	frame := r.process(RecordedOp{Unlink: &RecordedOp_Link{Fnode: fnode, Path: path}})

	if _, err := r.writer.Write(r.fsm.LogMark.Journal, frame); err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
}

// rocks.EnvObserver implementation.
func (r *Recorder) DeleteDir(dirname string) { /* No-op */ }

// rocks.EnvObserver implementation.
func (r *Recorder) LinkFile(src, target string) {
	src, target = r.normalizePath(src), r.normalizePath(target)

	defer r.mu.Unlock()
	r.mu.Lock()

	fnode, ok := r.fsm.Links[src]
	if !ok {
		log.WithFields(log.Fields{"path": src}).Panic("link of unknown path")
	}
	frame := r.process(RecordedOp{Link: &RecordedOp_Link{Fnode: fnode, Path: target}})

	if _, err := r.writer.Write(r.fsm.LogMark.Journal, frame); err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
}

// rocks.EnvObserver implementation.
func (r *Recorder) RenameFile(src, target string) {
	src, target = r.normalizePath(src), r.normalizePath(target)

	defer r.mu.Unlock()
	r.mu.Lock()

	fnode, ok := r.fsm.Links[src]
	if !ok {
		log.WithFields(log.Fields{"path": src}).Panic("link of unknown path")
	}
	prevFnode, prevExists := r.fsm.Links[target]

	// Decompose the rename into three operations:
	//  * Unlinking the fnode previously linked at |target|.
	//  * Linking the new fnode to |target|.
	//  * Unlinking the fnode from |src|.
	var unlinkTarget, linkTarget, unlinkSource []byte

	if prevExists {
		unlinkTarget = r.process(RecordedOp{
			Unlink: &RecordedOp_Link{Fnode: prevFnode, Path: target}})
	}
	linkTarget = r.process(RecordedOp{
		Link: &RecordedOp_Link{Fnode: fnode, Path: target}})
	unlinkSource = r.process(RecordedOp{
		Unlink: &RecordedOp_Link{Fnode: fnode, Path: src}})

	// Perform an atomic write of all three operations.
	if _, err := r.writer.ReadFrom(r.fsm.LogMark.Journal, io.MultiReader(
		bytes.NewReader(unlinkTarget),
		bytes.NewReader(linkTarget),
		bytes.NewReader(unlinkSource))); err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
}

func (r *Recorder) BuildHints() FSMHints {
	defer r.mu.Unlock()
	r.mu.Lock()

	return r.fsm.BuildHints()
}

func (r *Recorder) process(op RecordedOp) []byte {
	if r.fsm.NextSeqNo == 0 {
		op.SeqNo = 1
	} else {
		op.SeqNo = r.fsm.NextSeqNo
	}
	op.Checksum = r.fsm.NextChecksum
	op.Recorder = r.id

	var frame []byte
	if err := message.Frame(&op, &frame); err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("framing")
	}

	err := r.fsm.Apply(&op, frame[message.HeaderLength:])
	if err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("recorder FSM error")
	}
	r.fsm.LogMark.Offset += int64(len(frame))

	return frame
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

	frame := r.process(RecordedOp{Write: &RecordedOp_Write{
		Fnode:  r.fnode,
		Offset: r.offset,
		Length: int64(len(data)),
	}})

	// Perform an atomic write of the operation and its data.
	if _, err := r.writer.ReadFrom(r.fsm.LogMark.Journal, io.MultiReader(
		bytes.NewReader(frame),
		bytes.NewReader(data))); err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}

	r.offset += int64(len(data))
	r.fsm.LogMark.Offset += int64(len(data))
}

// rocks.EnvObserver implementation.
func (r *fileRecorder) Close()                         { r.sync() }
func (r *fileRecorder) Sync()                          { r.sync() }
func (r *fileRecorder) Fsync()                         { r.sync() }
func (r *fileRecorder) RangeSync(offset, nbytes int64) { r.sync() }

func (r *fileRecorder) sync() {
	promise, err := r.writer.Write(r.fsm.LogMark.Journal, nil)
	if err != nil {
		log.WithField("err", err).Panic("writing sync frame")
	}
	promise.Wait()
}
