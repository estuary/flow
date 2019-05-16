package recoverylog

import (
	"bufio"
	"crypto/rand"
	"io/ioutil"
	"math"
	"math/big"
	"path"
	"path/filepath"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	log "github.com/sirupsen/logrus"
)

// propertyFiles is well-known paths which should be treated as properties,
// rather than tracked as Fnodes.
var propertyFiles = map[string]struct{}{
	// RocksDB Database GUID created at initialization of empty database.
	"/IDENTITY": {},
}

// Recorder observes a sequence of changes to a file-system, and preserves
// those changes via a written Gazette journal of file-system operations.
// Note that we can't ever fail to write some portion of the recorded log, and
// then return control back to the database (and its client). To do so would
// allow for inconsistency in the local database state, vs the recorded log. For
// this reason, Recorder's implementation is crash-only and panics on error.
type Recorder struct {
	// State machine managing RecordedOp transitions.
	fsm *FSM
	// Generated unique ID of this Recorder.
	id Author
	// Prefix length to strip from filenames in recorded operations.
	stripLen int
	// Appender to the recovery log. We also rely on AsyncJournalClient to guard the
	// Recorder against concurrent access (since StartAppend also acquires a mutex).
	cl client.AsyncJournalClient
	// Last observed write head of the recovery log journal.
	writeHead int64
	// A recent append transaction, which we monitor for completion and, when Done,
	// use to update |writeHead| to the new maximum observed offset of the log.
	//
	// Regularly shifting |writeHead| forward results in a tighter lower-bound
	// on recorded operation offsets which are fed to the FSM (and to FSMHints),
	// reducing the amount of superfluous log which must be read on playback.
	// We additionally want to use offsets returned directly from Gazette
	// (rather than, eg, counting bytes), as they better account for writes from
	// competing Recorders and are guaranteed to align with message boundaries.
	recentTxn *client.AsyncAppend
	// Scratch buffer for framing RecordedOps.
	buf []byte
}

// NewRecorder creates and returns a Recorder.
func NewRecorder(fsm *FSM, id Author, dir string, cl client.AsyncJournalClient) *Recorder {
	var recorder = &Recorder{
		fsm:      fsm,
		id:       id,
		stripLen: len(filepath.Clean(dir)),
		cl:       cl,
	}
	// Issue a write barrier to determine the current write head, which will
	// lower-bound the offset for all subsequent recorded operations.
	<-recorder.WeakBarrier().Done()

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

// RecordCreate records the creation of file |path|, and returns a
// FileRecorder which records file operations.
func (r *Recorder) RecordCreate(path string) *FileRecorder {
	path = r.normalizePath(path)

	if _, isProperty := propertyFiles[path]; isProperty {
		log.WithField("path", path).Panic("unexpected open of property path")
	}
	var txn = r.lockAndBeginTxn()

	// Decompose the creation into two operations:
	//  * Unlinking |prevFnode| linked at |path| if |prevExists|.
	//  * Creating the new fnode backing |path|.
	if prevFnode, prevExists := r.fsm.Links[path]; prevExists {
		r.process(newUnlinkOp(prevFnode, path), txn.Writer())
	}
	r.process(newCreateOp(path), txn.Writer())
	var fr = &FileRecorder{Recorder: r, fnode: r.fsm.Links[path]}

	r.unlockAndReleaseTxn(txn)
	return fr
}

// RecordRemove records the removal of the file at |path|.
func (r *Recorder) RecordRemove(path string) {
	path = r.normalizePath(path)

	if _, isProperty := propertyFiles[path]; isProperty {
		log.WithField("path", path).Panic("unexpected delete of property path")
	}
	var txn = r.lockAndBeginTxn()

	var fnode, ok = r.fsm.Links[path]
	if !ok {
		log.WithFields(log.Fields{"path": path}).Panic("delete of unknown path")
	}
	r.process(newUnlinkOp(fnode, path), txn.Writer())
	r.unlockAndReleaseTxn(txn)
}

// RecordLink records the creation of a hard link from |src| to |target|.
func (r *Recorder) RecordLink(src, target string) {
	src, target = r.normalizePath(src), r.normalizePath(target)

	if _, isProperty := propertyFiles[target]; isProperty {
		log.WithFields(log.Fields{"src": src, "target": target}).
			Panic("unexpected link of property path")
	}
	var txn = r.lockAndBeginTxn()

	var fnode, ok = r.fsm.Links[src]
	if !ok {
		log.WithFields(log.Fields{"path": src}).Panic("link of unknown path")
	}
	r.process(newLinkOp(fnode, target), txn.Writer())
	r.unlockAndReleaseTxn(txn)
}

// RecordRename records the rename of |src| to |target|.
func (r *Recorder) RecordRename(src, target string) {
	var origTarget = target
	src, target = r.normalizePath(src), r.normalizePath(target)
	var txn = r.lockAndBeginTxn()

	var fnode, ok = r.fsm.Links[src]
	if !ok {
		log.WithFields(log.Fields{"path": src}).Panic("rename of unknown path")
	}

	// Decompose the rename into multiple operations:
	//  * Unlinking |prevFnode| linked at |target| if |prevExists|.
	//  * If |target| is a property, recording a property update.
	//  * If |target| is not a property, linking the |fnode| to |target|.
	//  * Unlinking the |fnode| from |src|.
	if prevFnode, prevExists := r.fsm.Links[target]; prevExists {
		r.process(newUnlinkOp(prevFnode, target), txn.Writer())
	}

	if _, isProperty := propertyFiles[target]; isProperty {
		var content, err = ioutil.ReadFile(origTarget)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "path": origTarget}).Panic("reading file")
		}
		r.process(newPropertyOp(target, string(content)), txn.Writer())
	} else {
		r.process(newLinkOp(fnode, target), txn.Writer())
	}
	r.process(newUnlinkOp(fnode, src), txn.Writer())
	r.unlockAndReleaseTxn(txn)
}

// BuildHints returns FSMHints which may be played back to fully reconstruct the
// local filesystem state observed by this Recorder. It may block while pending
// operations sync to the log.
func (r *Recorder) BuildHints() (FSMHints, error) {
	// We must be careful to not return FSMHints which reference operations
	// not yet committed to the log. Wrap BuildHints within a write-barrier
	// transaction, both to protect FSM from concurrent modification, and to
	// ensure we don't return constructed hints until all operations involved
	// in their construction have already committed.
	var txn = r.lockAndBeginTxn()
	var hints = r.fsm.BuildHints()
	r.unlockAndReleaseTxn(txn)

	<-txn.Done()
	return hints, txn.Err()
}

// StrongBarrier issues a zero-byte append which has dependencies on all other
// pending appends of the AsyncAppendClient. When this barrier completes, it is
// guaranteed that all writes of the AsyncAppendClient which were pending at
// issuance of the barrier (including writes of this Recorder) have committed.
func (r *Recorder) StrongBarrier() *client.AsyncAppend {
	var txn = r.lockAndBeginTxn(r.cl.PendingExcept(r.fsm.Log)...)
	r.unlockAndReleaseTxn(txn)

	return txn
}

// WeakBarrier issues a zero-byte append with no dependencies. When this barrier
// completes, it is guaranteed that all content recorded prior to the barrier
// has also committed. Writes to *other* journals which were pending at issuance
// of the barrier may still be ongoing.
func (r *Recorder) WeakBarrier() *client.AsyncAppend {
	var txn = r.lockAndBeginTxn()
	r.unlockAndReleaseTxn(txn)

	return txn
}

func (r *Recorder) normalizePath(fpath string) string {
	return path.Clean(filepath.ToSlash(fpath[r.stripLen:]))
}

func (r *Recorder) lockAndBeginTxn(dependencies ...*client.AsyncAppend) *client.AsyncAppend {
	// Locking is implied by StartAppend, which allows just one writer per journal.
	// The lock is held until Release is called by unlockAndReleaseTxn.
	var txn = r.cl.StartAppend(r.fsm.Log, dependencies...)

	if r.recentTxn == nil {
		r.recentTxn = txn
	}

	select {
	case <-r.recentTxn.Done():
		// A previous write has completed. Update our |writeHead|.
		if r.recentTxn.Err() != nil {
			// Aborted. Ignore.
		} else if end := r.recentTxn.Response().Commit.End; end < r.writeHead {
			log.WithFields(log.Fields{"writeHead": r.writeHead, "end": end, "log": r.fsm.Log}).
				Panic("invalid writeHead at lockAndBeginTxn")
		} else {
			r.writeHead = end
			r.recentTxn = txn
		}
	default:
		// Don't block.
	}

	return txn
}

func (r *Recorder) unlockAndReleaseTxn(txn *client.AsyncAppend) {
	if err := txn.Release(); err != nil {
		log.WithField("err", err).Panic("unlockAndReleaseTxn failed")
	}
}

func (r *Recorder) process(op RecordedOp, bw *bufio.Writer) {
	op.Author = r.id
	op.SeqNo = r.fsm.NextSeqNo
	op.Checksum = r.fsm.NextChecksum

	var err error
	r.buf, err = message.FixedFraming.Encode(&op, r.buf[:0])
	if err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("fixed-framing encode failed")
	}
	bw.Write(r.buf)

	// Use writeHead as a lower-bound for FirstOffset. As a meta-field, it's not
	// stored in the written frame, but is used by FSM in the production of hints.
	op.FirstOffset = r.writeHead

	if err = r.fsm.Apply(&op, r.buf[message.FixedFrameHeaderLength:]); err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("recorder FSM error")
	}
}

// FileRecorder records operations applied to a specific file opened with RecordCreate.
type FileRecorder struct {
	*Recorder

	// File being tracked, and the next write offset within the file.
	fnode  Fnode
	offset int64
}

// RecordWrite records the write of |data| at the current file offset.
func (r *FileRecorder) RecordWrite(data []byte) {
	var txn = r.Recorder.lockAndBeginTxn()
	r.frameAppend(data, txn.Writer())
	r.Recorder.unlockAndReleaseTxn(txn)
}

func (r *FileRecorder) frameAppend(b []byte, bw *bufio.Writer) {
	var l = int64(len(b))
	r.process(newWriteOp(r.fnode, r.offset, l), bw)
	bw.Write(b)
	r.offset += l
}

func newCreateOp(path string) RecordedOp {
	return RecordedOp{Create: &RecordedOp_Create{Path: path}}
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
