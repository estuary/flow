package recoverylog

import (
	"bufio"
	"io/ioutil"
	"path"
	"path/filepath"
	"sync"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// propertyFiles is well-known paths which should be treated as properties,
// rather than tracked as Fnodes.
var propertyFiles = map[string]struct{}{
	// RocksDB Database GUID created at initialization of empty database.
	"/IDENTITY": {},
}

// Recorder observes a sequence of changes to a file-system and preserves
// those changes via a written journal of file-system operations.
//
// Recorder, and the recovery log protocol, allow for the possibility of
// /multiple/ active recorders each sequencing to the same recovery log.
// The design allows for disambiguating which Recorder's "view" of the log
// should be reconstructed on playback by using FSMHints produced by the
// appropriate Recorder instance. Taken to an extreme, it's possible (though
// silly) to have _many_ distinct file-systems all recorded into a single
// combined journal, and to rely exclusively on FSMHints to advertise the
// specific, consistent file-system to be recovered and further recorded.
// Put differently, N concurrent Recorders will encode a "tree" to the log
// with N self-consistent branches, where FSMHints then inform readers of
// a specific branch to be recovered.
//
// Note that journal registers & checks can be used to prevent the multiple-
// writers scenario from occurring in the first place. However recovery log
// behavior (which pre-dates registers) will remain as-is for compatibility
// and for the additional resilience and explicitness it provides.
//
// Recorder expects to record only operations that have already happened (and
// were consistent with the local file-system). For this reason, Recorder is
// crash-only in its handling of inconsistent file operations (eg, an attempt
// to remove a path that isn't already known to the Recorder).
type Recorder struct {
	// State machine managing RecordedOp transitions.
	FSM *FSM
	// Generated unique ID of this Recorder.
	Author Author
	// Directory which roots local files. It must be a clean path, and must
	// prefix all recorded files, or Recorder panics. It's stripped from
	// recorded file names, making the log invariant to the choice of Dir.
	Dir string
	// Client for Recorder's use.
	Client client.AsyncJournalClient
	// CheckRegisters preconditions each operation appended to the recovery log.
	// Typically it should be:
	//   &pb.LabelSelector{Include: author.Fence()}
	// Which ensures that this Recorder's operations will immediately stop
	// appending to the log if another future Player injects a hand-off.
	CheckRegisters *pb.LabelSelector

	// |recentTxn| is a recent append, which we monitor for completion and,
	// when Done, use to update |writeHead| to the new maximum observed offset
	// of the recovery log.
	//
	// Regularly shifting |writeHead| forward results in a tighter lower-bound
	// on recorded operation offsets which are fed to the FSM (and to FSMHints),
	// reducing the amount of superfluous log which must be read on playback.
	// We additionally want to use offsets returned directly from brokers
	// (rather than, eg, counting bytes), as they better account for appends from
	// competing Recorders and are guaranteed to align with message boundaries.
	recentTxn *client.AsyncAppend
	// Last observed write head of the recovery log journal.
	writeHead int64
	// Synchronization over lazy initialization.
	lazyInit sync.Once
	// Scratch buffer for framing RecordedOps.
	buf []byte
}

// RecordCreate records the creation of file |path|, and returns a
// FileRecorder which records file operations.
func (r *Recorder) RecordCreate(path string) *FileRecorder {
	r.init()
	path = r.normalizePath(path)

	if _, isProperty := propertyFiles[path]; isProperty {
		log.WithField("path", path).Panic("unexpected open of property path")
	}
	var txn = r.lockAndBeginTxn(nil)

	// Decompose the creation into two operations:
	//  * Unlinking |prevFnode| linked at |path| if |prevExists|.
	//  * Creating the new fnode backing |path|.
	if prevFnode, prevExists := r.FSM.Links[path]; prevExists {
		r.process(newUnlinkOp(prevFnode, path), txn.Writer())
	}
	r.process(newCreateOp(path), txn.Writer())
	var fr = &FileRecorder{Recorder: r, fnode: r.FSM.Links[path]}

	r.unlockAndReleaseTxn(txn)
	return fr
}

// RecordRemove records the removal of the file at |path|.
func (r *Recorder) RecordRemove(path string) {
	r.init()
	path = r.normalizePath(path)

	if _, isProperty := propertyFiles[path]; isProperty {
		log.WithField("path", path).Panic("unexpected delete of property path")
	}
	var txn = r.lockAndBeginTxn(nil)

	var fnode, ok = r.FSM.Links[path]
	if !ok {
		log.WithFields(log.Fields{"path": path}).Panic("delete of unknown path")
	}
	r.process(newUnlinkOp(fnode, path), txn.Writer())
	r.unlockAndReleaseTxn(txn)
}

// RecordLink records the creation of a hard link from |src| to |target|.
func (r *Recorder) RecordLink(src, target string) {
	r.init()
	src, target = r.normalizePath(src), r.normalizePath(target)

	if _, isProperty := propertyFiles[target]; isProperty {
		log.WithFields(log.Fields{"src": src, "target": target}).
			Panic("unexpected link of property path")
	}
	var txn = r.lockAndBeginTxn(nil)

	var fnode, ok = r.FSM.Links[src]
	if !ok {
		log.WithFields(log.Fields{"path": src}).Panic("link of unknown path")
	}
	r.process(newLinkOp(fnode, target), txn.Writer())
	r.unlockAndReleaseTxn(txn)
}

// RecordRename records the rename of |src| to |target|.
func (r *Recorder) RecordRename(src, target string) {
	r.init()
	var origTarget = target
	src, target = r.normalizePath(src), r.normalizePath(target)
	var txn = r.lockAndBeginTxn(nil)

	var fnode, ok = r.FSM.Links[src]
	if !ok {
		log.WithFields(log.Fields{"path": src}).Panic("rename of unknown path")
	}

	// Decompose the rename into multiple operations:
	//  * Unlinking |prevFnode| linked at |target| if |prevExists|.
	//  * If |target| is a property, recording a property update.
	//  * If |target| is not a property, linking the |fnode| to |target|.
	//  * Unlinking the |fnode| from |src|.
	if prevFnode, prevExists := r.FSM.Links[target]; prevExists {
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
	var txn = r.lockAndBeginTxn(nil)
	var hints = r.FSM.BuildHints()
	r.unlockAndReleaseTxn(txn)

	<-txn.Done()
	return hints, txn.Err()
}

// Barrier issues a zero-byte append which will not commence until |waitFor|
// operations have successfully resolved.
func (r *Recorder) Barrier(waitFor client.OpFutures) *client.AsyncAppend {
	var txn = r.lockAndBeginTxn(waitFor)
	r.unlockAndReleaseTxn(txn)

	return txn
}

func (r *Recorder) init() {
	r.lazyInit.Do(func() {
		if r.Dir != path.Clean(r.Dir) {
			log.WithFields(log.Fields{"dir": r.Dir, "cleaned": path.Clean(r.Dir)}).
				Panic("recorder.Dir is not a Clean path")
		}
		// Issue a write barrier to determine the current write head, which will
		// lower-bound the offset for all subsequent recorded operations.
		<-r.Barrier(nil).Done()
	})
}

func (r *Recorder) normalizePath(fpath string) string {
	return path.Clean(filepath.ToSlash(fpath[len(r.Dir):]))
}

func (r *Recorder) lockAndBeginTxn(waitFor client.OpFutures) *client.AsyncAppend {
	// Locking is implied by StartAppend, which allows just one writer per journal.
	// The lock is held until Release is called by unlockAndReleaseTxn.
	var txn = r.Client.StartAppend(pb.AppendRequest{
		Journal:        r.FSM.Log,
		CheckRegisters: r.CheckRegisters,
	}, waitFor)

	if r.recentTxn == nil {
		r.recentTxn = txn
	}

	select {
	case <-r.recentTxn.Done():
		// A previous write has completed. Update our |writeHead|.
		if r.recentTxn.Err() != nil {
			// Aborted. Ignore.
		} else if end := r.recentTxn.Response().Commit.End; end < r.writeHead {
			log.WithFields(log.Fields{"writeHead": r.writeHead, "end": end, "log": r.FSM.Log}).
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
	op.Author = r.Author
	op.SeqNo = r.FSM.NextSeqNo
	op.Checksum = r.FSM.NextChecksum

	var err error
	r.buf, err = message.FixedFraming.Encode(&op, r.buf[:0])
	if err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("fixed-framing encode failed")
	}
	_, _ = bw.Write(r.buf)

	// Use writeHead as a lower-bound for FirstOffset. As a meta-field, it's not
	// stored in the written frame, but is used by FSM in the production of hints.
	op.FirstOffset = r.writeHead

	if err = r.FSM.Apply(&op, r.buf[message.FixedFrameHeaderLength:]); err != nil {
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
	var txn = r.Recorder.lockAndBeginTxn(nil)
	r.frameAppend(data, txn.Writer())
	r.Recorder.unlockAndReleaseTxn(txn)
}

func (r *FileRecorder) frameAppend(b []byte, bw *bufio.Writer) {
	var l = int64(len(b))
	r.process(newWriteOp(r.fnode, r.offset, l), bw)
	_, _ = bw.Write(b)
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
