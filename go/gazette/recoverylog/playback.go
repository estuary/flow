package recoverylog

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	// Byte-delta threshold between current offset and next playback offset,
	// at which we will abort a current log reader and re-open at the next offset.
	logSkipThreshold = 1 << 29 // 512MB.
)

// Error returned by Player.Play() & MakeLive() upon Player.Cancel().
var ErrPlaybackCancelled = fmt.Errorf("playback cancelled")

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
	} else if err := os.MkdirAll(fileNodesDir, 0777); err != nil {
		return nil, err
	}

	fsm, err := NewFSM(hints)
	if err != nil {
		return nil, err
	}

	return &Player{
		fsm:          fsm,
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
// Ignored if Play has already exited.
func (p *Player) Cancel() { close(p.cancelCh) }

// As an alternative to Cancel, SetCancelChan arranges for a subsequent Play
// invocation to cancel playback upon |cancelCh| becoming select-able.
func (p *Player) SetCancelChan(cancelCh chan struct{}) {
	p.cancelCh = cancelCh
}

// Begins playing the prepared player. Returns on the first encountered
// unrecoverable error, or upon a successful MakeLive().
func (p *Player) Play(client journal.Client) error {
	var err error
	defer func() { p.playExitCh <- err }()

	var writeHead int64

	// Closure which issues a write barrier to transactionally determine the
	// minimum write head that we must read through. This will also create the
	// recovery log, if it doesn't exist.
	writeBarrier := func() error {
		if barrier, err := client.Write(p.fsm.LogMark.Journal, nil); err != nil {
			return err
		} else if <-barrier.Ready; barrier.Error != nil {
			return barrier.Error
		} else {
			writeHead = barrier.WriteHead
			return nil
		}
	}
	if err = writeBarrier(); err != nil {
		return err
	}

	// Play until we're asked to make a snapshot live, *and* we're at the
	// recovery-log write head.
	for makeLive, atHead := false, false; !makeLive || !atHead; {
		select {
		case <-p.makeLiveCh:
			makeLive = true
			p.makeLiveCh = nil // Don't receive again.

			// Transactionally update |writeHead|. We must read through this offset
			// before we consider playback to be complete.
			if err = writeBarrier(); err != nil {
				return err
			}
			continue
		case <-p.cancelCh:
			err = ErrPlaybackCancelled
			return err
		default:
			// Pass.
		}

		var readArgs = journal.ReadArgs{
			Journal:  p.fsm.LogMark.Journal,
			Offset:   p.fsm.LogMark.Offset,
			Deadline: time.Now().Add(kBlockInterval),
		}

		// Do FSM hints indicate we should jump forward in the log?
		if s := p.fsm.hintedSegments; len(s) != 0 && s[0].FirstOffset > readArgs.Offset {
			readArgs.Offset = s[0].FirstOffset
		}
		result, reader := client.Get(readArgs)

		if result.Error != nil {
			log.WithFields(log.Fields{"err": result.Error, "mark": p.fsm.LogMark}).
				Warn("while fetching log")
			time.Sleep(kErrCooloffInterval)
			continue
		}

		if result.WriteHead > writeHead {
			// A Read WriteHead can increment the target |writeHead|, but should not
			// decrease it. Reads are not transactional, and we can get a stale offset.
			writeHead = result.WriteHead
		}
		p.fsm.LogMark.Offset = result.Offset
		err = p.playSomeLog(reader)

		if abortErr, ok := err.(abortPlaybackErr); ok {
			err = abortErr.error
			return err
		} else if err != nil {
			log.WithFields(log.Fields{"err": err, "mark": p.fsm.LogMark}).
				Warn("during log playback")
			time.Sleep(kErrCooloffInterval)
		} else {
			// We issue blocking reads, so for this condition to be satisfied we must
			// have read through to the WriteHead that existed at read start, and
			// during the blocking interval no additional content arrived (otherwise
			// our offset would be beyond |writeHead|).
			atHead = (p.fsm.LogMark.Offset == writeHead)
		}
	}
	err = p.makeLive()
	return err
}

func (p *Player) playSomeLog(r io.ReadCloser) error {
	var err error
	var frame []byte

	mr := journal.NewMarkedReader(p.fsm.LogMark, r)
	defer mr.Close()

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
				// |op| is prior to the next hinted SeqNo.
				if s := p.fsm.hintedSegments; len(s) != 0 &&
					s[0].FirstOffset-mr.Mark.Offset > logSkipThreshold {
					// Return early to abort the current reader.
					return nil
				}
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
		return fmt.Errorf("FSM has remaining unused hints: %+v", p.fsm)
	}
	for fnode, liveNode := range p.fsm.LiveNodes {
		backingFile := p.backingFiles[fnode]
		delete(p.backingFiles, fnode)

		// Link backing-file into target paths.
		for link := range liveNode.Links {
			targetPath := filepath.Join(p.localDir, link)

			if err := os.MkdirAll(filepath.Dir(targetPath), 0777); err != nil {
				return err
			} else if err = os.Link(p.stagedPath(fnode), targetPath); err != nil {
				return err
			}
			log.WithFields(log.Fields{"fnode": fnode, "target": targetPath}).Info("linked file")
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

	// Write property files.
	for path, content := range p.fsm.Properties {
		targetPath := filepath.Join(p.localDir, path)

		// Write |content| to |targetPath|. Expect it to not exist.
		if err := os.MkdirAll(filepath.Dir(targetPath), 0777); err != nil {
			return err
		} else if fout, err := os.OpenFile(targetPath,
			os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666); err != nil {
			return err
		} else if _, err = io.Copy(fout, strings.NewReader(content)); err != nil {
			return err
		} else if err = fout.Close(); err != nil {
			return err
		}
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
