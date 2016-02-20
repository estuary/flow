package recoverylog

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
)

const (
	// Subdirectory into which Fnodes are played-back.
	kFnodeStagingDir = ".fnodes"
	// Duration for which Player reads of the recovery log block.
	kBlockInterval = 1 * time.Second
	// Cool-off applied on non-aborting errors.
	kErrCooloffInterval = 5 * time.Second
)

type Player struct {
	fsm *FSM
	// Prefix added to recovered file paths.
	localDir string
	// Mapping of live Fnodes to local backing files.
	backingFiles map[Fnode]*os.File

	// Signals to Play() service loop that Cancel() has been called.
	cancelCh chan struct{}
	// Signals to Play() service loop that MakeLive() has been called.
	makeLiveCh chan struct{}
	// Closed by Play() to signal to MakeLive() that Play() has exited.
	playExitCh chan error
}

// Prepares playback of |hints| into |localDir|. The recovery log may actively
// be in use by another process while playback occurs, allowing for warm-
// standby replication.
func PreparePlayback(hints FSMHints, localDir string) (*Player, error) {
	// File nodes are staged into a directory within |localDir| during playback.
	fileNodesDir := filepath.Join(localDir, kFnodeStagingDir)

	// Remove all prior content under |localDir|.
	if err := os.RemoveAll(localDir); err != nil {
		return nil, err
	} else if err := os.MkdirAll(fileNodesDir, 0755); err != nil {
		return nil, err
	}

	return &Player{
		fsm:          NewFSM(hints),
		localDir:     localDir,
		backingFiles: make(map[Fnode]*os.File),
		cancelCh:     make(chan struct{}),
		makeLiveCh:   make(chan struct{}),
		// Buffered because Play() may exit before MakeLive() is called.
		playExitCh: make(chan error, 1),
	}, nil
}

// A number of errors can be generated during playback, but most are retry-able.
// abortPlaybackErr signals an unrecoverable error which should abort playback.
type abortPlaybackErr struct {
	error
}

// Requests that Player finalize playback. An exit without error means Play()
// has exited as well, after successfully restoring local file state to match
// operations in the recovery-log through the current write head. The Player
// FSM instance is returned, which can be used to construct Recorder for
// recording further file state changes.
func (p *Player) MakeLive() (*FSM, error) {
	close(p.makeLiveCh)

	// Wait for Play() to exit.
	if err := <-p.playExitCh; err != nil {
		return nil, err
	}
	return p.fsm, nil
}

// Requests that Player cancel playback and exit with an error.
// Ignored if Play() has already exited.
func (p *Player) Cancel() { close(p.cancelCh) }

// Begins playing the prepared player. Returns on the first encountered
// unrecoverable error, or upon a successful MakeLive().
func (p *Player) Play(getter journal.Getter) error {
	var err error
	defer func() { p.playExitCh <- err }()

	// Play until we're asked to make a snapshot live, *and* we're at the
	// recovery-log write head.
	for makeLive, atHead := false, false; !makeLive || !atHead; {
		select {
		case <-p.makeLiveCh:
			makeLive = true
			p.makeLiveCh = nil // Don't receive again.
			continue
		case <-p.cancelCh:
			err = fmt.Errorf("Play cancelled")
			return err
		default:
			// Pass.
		}

		result, reader := getter.Get(journal.ReadArgs{
			Journal:  p.fsm.LogMark.Journal,
			Offset:   p.fsm.LogMark.Offset,
			Deadline: time.Now().Add(kBlockInterval),
		})

		if result.Error != nil {
			log.WithFields(log.Fields{"err": err, "mark": p.fsm.LogMark}).Warn("while fetching log")
			time.Sleep(kErrCooloffInterval)
			continue
		}
		p.fsm.LogMark.Offset = result.Offset
		err = p.playSomeLog(reader)

		if abortErr, ok := err.(abortPlaybackErr); ok {
			err = abortErr.error
			return err
		} else if err != nil {
			log.WithFields(log.Fields{"err": err, "mark": p.fsm.LogMark}).Warn("during log playback")
			time.Sleep(kErrCooloffInterval)
		} else {
			// We issue blocking reads, so for this condition to be satisfied we must
			// have read through to the WriteHead that existed at read start, and
			// during the blocking interval no additional content arrived (otherwise
			// our offset would be beyond |result.WriteHead|).
			atHead = (p.fsm.LogMark.Offset == result.WriteHead)
		}
	}
	err = p.makeLive()
	return err
}

func (p *Player) playSomeLog(r io.Reader) error {
	var err error
	var frame []byte

	mr := journal.NewMarkedReader(p.fsm.LogMark, r)

	for err == nil {
		// Step mark forward only at whole message boundaries.
		p.fsm.LogMark = mr.Mark

		var op RecordedOp
		if _, err = message.Parse(&op, mr, &frame); err == io.EOF {
			// EOF on message boundary is not an error.
			return nil
		} else if err != nil {
			continue
		}

		// Run the operation through the FSM to verify validity.
		fsmErr := p.fsm.Apply(&op, frame)
		if fsmErr != nil {
			// Log but otherwise ignore FSM errors: |player| is still in a consistent
			// state, and we may make further progress later in the log.
			if fsmErr == ErrFnodeNotTracked {
				// Common when recovering from the middle of the log. makeLive() asserts
				// that only tracked fnodes remain when playback completes.
			} else if fsmErr == ErrWrongSeqNo && op.SeqNo < p.fsm.NextSeqNo {
				// Replay of a previous operation.
			} else {
				log.WithFields(log.Fields{"op": op, "err": fsmErr}).Warn("playback FSM error")
			}

			// For bytestream consistency Write ops must still skip |op.Length| bytes.
			if op.Write != nil {
				_, err = io.CopyN(ioutil.Discard, mr, op.Write.Length)
			}
			continue
		}

		// The operation is valid. Apply local playback actions.
		if op.Create != nil {
			err = p.create(Fnode(op.SeqNo))
		} else if op.Unlink != nil {
			err = p.unlink(op.Unlink.Fnode)
		} else if op.Write != nil {
			err = p.write(op.Write, mr)
		}
	}
	return err
}

func (p *Player) stagedPath(fnode Fnode) string {
	fname := strconv.FormatInt(int64(fnode), 10)
	return filepath.Join(p.localDir, kFnodeStagingDir, fname)
}

func (p *Player) create(fnode Fnode) error {
	backingFile, err := os.OpenFile(p.stagedPath(fnode),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666) // Expect file to not exist.
	if err == nil {
		p.backingFiles[fnode] = backingFile
	}
	return asAbortingError(err)
}

func (p *Player) unlink(fnode Fnode) error {
	if _, isLive := p.fsm.LiveNodes[fnode]; isLive {
		// Live links remain for |fnode|. Take no action.
		return nil
	}
	backingFile := p.backingFiles[fnode]

	// Close and remove the local backing file.
	if err := backingFile.Close(); err != nil {
		return asAbortingError(err)
	} else if err = os.Remove(p.stagedPath(fnode)); err != nil {
		return asAbortingError(err)
	}
	delete(p.backingFiles, fnode)
	return nil
}

func (p *Player) write(op *RecordedOp_Write, r io.Reader) error {
	r = io.LimitReader(r, op.Length)

	if p.fsm.LiveNodes[op.Fnode].SkipWrites {
		_, err := io.CopyN(ioutil.Discard, r, op.Length)
		return err
	}
	backingFile := p.backingFiles[Fnode(op.Fnode)]

	// Seek and write at the indicated offset.
	if _, err := backingFile.Seek(op.Offset, 0); err != nil {
		return asAbortingError(err)
	}

	// Copy from |r| to |fnode|, separately tracking read & write errors.
	// Write errors are aborting, while read errors are not.
	var total int64
	var readErr, writeErr error
	buffer := make([]byte, 4*1024)

	for readErr == nil && writeErr == nil {
		var nr, nw int
		nr, readErr = r.Read(buffer[:])
		nw, writeErr = backingFile.Write(buffer[:nr])

		if nr != nw && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		total += int64(nw)
	}
	if writeErr != nil {
		return asAbortingError(writeErr)
	} else if readErr != io.EOF {
		return readErr
	} else if total != op.Length {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (p *Player) makeLive() error {
	if p.fsm.HasHints() {
		return fmt.Errorf("FSM has remaining unused hints (reached log end?)")
	}
	for fnode, liveNode := range p.fsm.LiveNodes {
		if liveNode.SkipWrites {
			return fmt.Errorf("fnode %d hinted to SkipWrites but is still live", fnode)
		}
		backingFile := p.backingFiles[fnode]
		delete(p.backingFiles, fnode)

		// Link backing-file into target paths.
		for link := range liveNode.Links {
			targetPath := filepath.Join(p.localDir, link)

			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return err
			} else if err = os.Link(p.stagedPath(fnode), targetPath); err != nil {
				return err
			}
		}
		// Close and removed the staged file.
		if err := backingFile.Close(); err != nil {
			return err
		} else if err = os.Remove(p.stagedPath(fnode)); err != nil {
			return err
		}
	}
	if len(p.backingFiles) != 0 {
		// Invariant: |FSM.LiveNodes| should fully describe all backing files.
		log.WithField("files", p.backingFiles).Panic("backing files not in FSM")
	}
	// Remove staging directory.
	if err := os.Remove(filepath.Join(p.localDir, kFnodeStagingDir)); err != nil {
		return err
	}
	return nil
}

func asAbortingError(err error) error {
	if err != nil {
		return abortPlaybackErr{err}
	} else {
		return nil
	}
}
