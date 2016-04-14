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
	"github.com/pippio/gazette/message"
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

	if _, isProperty := propertyFiles[path]; isProperty {
		log.WithField("path", path).Panic("unexpected open of property path")
	}

	defer r.mu.Unlock()
	r.mu.Lock()

	prevFnode, prevExists := r.fsm.Links[path]

	// Decompose the creation into two operations:
	//  * Unlinking |prevFnode| linked at |path| if |prevExists|.
	//  * Creating the new fnode backing |path|.
	var unlinkPrev, createNew []byte

	if prevExists {
		unlinkPrev = r.process(RecordedOp{
			Unlink: &RecordedOp_Link{Fnode: prevFnode, Path: path}})
	}
	createNew = r.process(RecordedOp{Create: &RecordedOp_Create{Path: path}})

	// Perform an atomic write of both operations.
	if _, err := r.writer.ReadFrom(r.fsm.LogMark.Journal, io.MultiReader(
		bytes.NewReader(unlinkPrev),
		bytes.NewReader(createNew))); err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
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
	frame := r.process(RecordedOp{Link: &RecordedOp_Link{Fnode: fnode, Path: target}})

	if _, err := r.writer.Write(r.fsm.LogMark.Journal, frame); err != nil {
		log.WithField("err", err).Panic("writing op frame")
	}
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
	var unlinkTarget, updateProperty, linkTarget, unlinkSource []byte

	if prevExists {
		unlinkTarget = r.process(RecordedOp{
			Unlink: &RecordedOp_Link{Fnode: prevFnode, Path: target}})
	}

	if _, isProperty := propertyFiles[target]; isProperty {
		content, err := ioutil.ReadFile(targetPath)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "path": targetPath}).Panic("reading file")
		}
		updateProperty = r.process(RecordedOp{
			Property: &RecordedOp_Property{Path: target, Content: string(content)}})
	} else {
		linkTarget = r.process(RecordedOp{
			Link: &RecordedOp_Link{Fnode: fnode, Path: target}})
	}

	unlinkSource = r.process(RecordedOp{
		Unlink: &RecordedOp_Link{Fnode: fnode, Path: src}})

	// Perform an atomic write of all three operations.
	if _, err := r.writer.ReadFrom(r.fsm.LogMark.Journal, io.MultiReader(
		bytes.NewReader(unlinkTarget),
		bytes.NewReader(updateProperty),
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

// Shifts the FSM offset for successive operations to |writeHead|, which will
// lower-bound the effective offset of any new operations being recorded.
// We use periodic updates from commits to update our offset rather than
// counting written bytes, as this provides a tight bound while still being
// correct in the case of competing writes from multiple Recorders.
func (r *Recorder) UpdateWriteHead(writeHead int64) { r.fsm.LogMark.Offset = writeHead }

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
}

// rocks.EnvObserver implementation.
func (r *fileRecorder) Close()                         {}
func (r *fileRecorder) Sync()                          { r.sync() }
func (r *fileRecorder) Fsync()                         { r.sync() }
func (r *fileRecorder) RangeSync(offset, nbytes int64) { r.sync() }

func (r *fileRecorder) sync() {
	result, err := r.writer.Write(r.fsm.LogMark.Journal, nil)
	if err != nil {
		log.WithField("err", err).Panic("writing sync frame")
	}
	<-result.Ready
}
