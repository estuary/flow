package recoverylog

import (
	"bytes"
	"io"
	"time"

	log "github.com/Sirupsen/logrus"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/async"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
)

var kErrRetryInterval = 10 * time.Second

// Recorder observes a sequence of changes to a file-system, and preserves
// those changes via a file Manifest and Gazette journal of written content.
type Recorder struct {
	manifest Manifest
	// Prefix length to strip from filenames in recorded operations.
	stripLen int
	// Journal & clients via which file operations are recorded.
	opLog  journal.Name
	header journal.Header
	writer journal.Writer
}

func NewRecorder(manifest Manifest, stripLen int, opLog journal.Name,
	header journal.Header, writer journal.Writer) Recorder {
	return Recorder{
		manifest: manifest,
		stripLen: stripLen,
		opLog:    opLog,
		header:   header,
		writer:   writer,
	}
}

// rocks.EnvObserver implementation.
func (p *Recorder) NewWritableFile(fname string) rocks.WritableFileObserver {
	fname = fname[p.stripLen:]

	// Determine the approximate current write head of the recovery log.
	var offset int64
	retryable("fetching current journal head", func() error {
		response, err := p.header.HeadJournalAt(journal.NewMark(p.opLog, -1))
		if err != nil {
			return err
		}
		offset, err = gazette.ParseWriteHead(response)
		return err
	})

	// Create new FileRecord backing the file.
	record, err := p.manifest.CreateFile(fname, offset)
	if err != nil {
		log.WithFields(log.Fields{"fname": fname, "err": err}).Panic("create file")
	}

	// Record file allocation into the recovery log.
	op := RecordedOp{Alloc: &RecordedOp_Alloc{Id: record.Id}}

	var frame []byte
	if err := message.Frame(&op, &frame); err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("framing")
	}
	retryable("recording file allocation", func() error {
		_, err := p.writer.Write(p.opLog, frame)
		return err
	})
	return &fileRecorder{p, record, 0}
}

// rocks.EnvObserver implementation.
func (p *Recorder) DeleteFile(fname string) {
	fname = fname[p.stripLen:]

	record, err := p.manifest.DeleteFile(fname)
	if err != nil {
		log.WithFields(log.Fields{"fname": fname, "err": err}).Panic("delete file")
	}
	if len(record.Links) != 0 {
		// File is still active.
		log.WithFields(log.Fields{"fname": fname, "links": record.Links}).
			Debug("deleted file has links remaining")
		return
	}
	// Record file de-allocation into the recovery log.
	op := RecordedOp{Dealloc: &RecordedOp_Dealloc{Id: record.Id}}

	var frame []byte
	if err := message.Frame(&op, &frame); err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("framing")
	}
	retryable("recording file de-allocation", func() error {
		_, err := p.writer.Write(p.opLog, frame)
		return err
	})
}

// rocks.EnvObserver implementation.
func (p *Recorder) DeleteDir(dirname string) { /* No-op. */ }

// rocks.EnvObserver implementation.
func (p *Recorder) RenameFile(src, target string) {
	src, target = src[p.stripLen:], target[p.stripLen:]

	_, err := p.manifest.RenameFile(src, target)
	if err != nil {
		log.WithFields(log.Fields{"src": src, "target": target, "err": err}).
			Panic("rename file")
	}
}

// rocks.EnvObserver implementation.
func (p *Recorder) LinkFile(src, target string) {
	src, target = src[p.stripLen:], target[p.stripLen:]

	_, err := p.manifest.LinkFile(src, target)
	if err != nil {
		log.WithFields(log.Fields{"src": src, "target": target, "err": err}).
			Panic("link file")
	}
}

type fileRecorder struct {
	*Recorder
	fileRecord *FileRecord

	// Tracks the next write offset within the file.
	offset int64
}

// rocks.EnvObserver implementation.
func (r *fileRecorder) Append(data []byte) {
	op := RecordedOp{
		Write: &RecordedOp_Write{
			Id:     r.fileRecord.Id,
			Offset: r.offset,
			Length: int64(len(data)),
		},
	}
	// Frame append operation buffers.
	var frame []byte
	if err := message.Frame(&op, &frame); err != nil {
		log.WithFields(log.Fields{"op": op, "err": err}).Panic("framing")
	}
	// Write operation frame and data payload.
	retryable("recording file append", func() error {
		_, err := r.writer.ReadFrom(r.opLog, io.MultiReader(
			bytes.NewReader(frame), bytes.NewReader(data)))
		return err
	})
	r.offset += int64(len(data))
}

// rocks.EnvObserver implementation.
func (r *fileRecorder) Close()                         { r.sync() }
func (r *fileRecorder) Sync()                          { r.sync() }
func (r *fileRecorder) Fsync()                         { r.sync() }
func (r *fileRecorder) RangeSync(offset, nbytes int64) { r.sync() }

func (r *fileRecorder) sync() {
	var promise async.Promise

	retryable("syncing file", func() error {
		var err error
		promise, err = r.writer.Write(r.opLog, nil)
		return err
	})
	promise.Wait()
}

// Note that we can't ever fail to write some portion of the recorded log,
// and then return control back to the database (and it's client). To do so
// would allow for inconsistency in the local database state, vs the recorded
// log. We either have to succeed completely, or retry indefinitly. Or crash,
// which basically amounts to retrying (but with more noise).
func retryable(msg string, f func() error) {
	for {
		err := f()
		if err == nil {
			return
		}
		log.WithField("err", err).Warn(msg)
		time.Sleep(kErrRetryInterval)
	}
}
