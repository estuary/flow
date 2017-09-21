package recoverylog

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/metrics"
	"github.com/pippio/gazette/topic"
	"github.com/pippio/varz"
)

const (
	// Subdirectory into which Fnodes are played-back.
	fnodeStagingDir = ".fnodes"
	// Duration for which Player reads of the recovery log block.
	blockInterval = 1 * time.Second
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
	// Closed by Play() to signal that playback has reached the log head.
	atHeadCh chan struct{}
}

// NewPlayer returns a new Player for recovering the log indicated by |hints|
// into |localDir|. An error is returned if FSMHints are invalid.
func NewPlayer(hints FSMHints, localDir string) (*Player, error) {
	var fsm, err = NewFSM(hints)
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
		atHeadCh:   make(chan struct{}),
	}, nil
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

// IsAtLogHead returns true if playback has reached the WriteHead returned
// by a Gazette Journal read. Note that Gazette reads are not transactional,
// and this determination may be slightly stale.
func (p *Player) IsAtLogHead() bool {
	select {
	case <-p.atHeadCh:
		return true
	default:
		return false
	}
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

	// Error checks in this function consistently use |err|. We defer sending
	// |err| on return, to make it available for MakeLive as well.
	var err error
	defer func() {
		if err != nil {
			// Remove partial content from disk on playback failure or cancel.
			p.cleanupAfterAbort()
		}
		p.playExitCh <- err
	}()

	if err = p.preparePlayback(); err != nil {
		return err
	}

	// Note - here the fsm.LogMark is initialized to -1 on a new Player.
	var rr = journal.NewRetryReader(p.fsm.LogMark, client)
	defer rr.Close()

	// Configure |rr| to periodically return EOF when no content is available.
	rr.EOFTimeout = blockInterval

	var atHeadCh = p.atHeadCh // Retain on stack so it may be nil'd.
	var br = bufio.NewReader(rr)
	var makeLiveBarrier *journal.AsyncAppend

	// Play until we're asked to make ourselves live, we've read through to the
	// transactionally determined recoverylog WriteHead, and we time out
	// waiting for new log content.
	for {

		select {
		case <-p.makeLiveCh:
			p.makeLiveCh = nil // Don't select again.

			// Issue an empty write (a write barrier) to transactionally determine
			// the minimum WriteHead we must read through.
			if makeLiveBarrier, err = client.Write(p.fsm.LogMark.Journal, nil); err != nil {
				return err
			}
			<-makeLiveBarrier.Ready

			if err = makeLiveBarrier.Error; err != nil {
				return err
			}

		case <-p.cancelCh:
			err = ErrPlaybackCancelled
			return err

		default:
			// Non-blocking.
		}

		if s := p.fsm.hintedSegments; len(s) != 0 && s[0].FirstOffset > rr.AdjustedMark(br).Offset {
			// Seek the RetryReader forward to the next hinted offset.
			if _, err = rr.Seek(s[0].FirstOffset, os.SEEK_SET); err != nil {
				return err
			}
			br.Reset(rr)
			continue
		}

		// Play the next operation. First Peek to ensure the next byte has been
		// pre-fetched, which guarantees resolution of the absolute operation offset.
		if _, err = br.Peek(1); err == nil {
			p.fsm.LogMark = rr.AdjustedMark(br)
			err = p.playOperation(br)
		}

		if err == io.EOF {
			// EOF is returned only on operation message boundaries, and under
			// RetryReader EOFTimeout semantics, only when a deadline read request
			// completed with no content.
			err = nil

			if atHeadCh != nil {
				close(atHeadCh)
				atHeadCh = nil
			}

			if makeLiveBarrier != nil {
				var target = makeLiveBarrier.WriteHead

				// A read WriteHead can increase that of |makeLiveBarrier|, but should
				// not decrease it. Reads are not transactional and can be stale.
				if rr.Result.WriteHead > target {
					target = rr.Result.WriteHead
				}

				if rr.Mark.Offset == target {
					// Exit condition: we timed out waiting for content, we've been asked
					// to make ourselves Live, and we've read to the target write head.
					err = p.makeLive()
					return err
				}
			}
		} else if err != nil {
			// Any other error aborts playback.
			return err
		} else if atHeadCh != nil && rr.Result.WriteHead <= rr.Mark.Offset {
			// Signal that playback has reached the approximate log head.
			close(atHeadCh)
			atHeadCh = nil
		}
	}
}

func (p *Player) preparePlayback() error {
	// File nodes are staged into a directory within |localDir| during playback.
	var fileNodesDir = filepath.Join(p.localDir, fnodeStagingDir)

	// Remove all prior content under |p.localDir|.
	if err := os.RemoveAll(p.localDir); err != nil {
		return err
	} else if err := os.MkdirAll(fileNodesDir, 0777); err != nil {
		return err
	}
	return nil
}

func (p *Player) cleanupAfterAbort() {
	for _, fnode := range p.backingFiles {
		if err := fnode.Close(); err != nil {
			log.WithField("err", err).Warn("closing fnode after abort")
		}
	}
	if err := os.RemoveAll(p.localDir); err != nil {
		log.WithField("err", err).Warn("removing localDir after abort")
	}
}

func (p *Player) playOperation(br *bufio.Reader) error {
	var b, err = topic.FixedFraming.Unpack(br)
	var op RecordedOp

	if err != nil {
		return err
	} else if err = topic.FixedFraming.Unmarshal(b, &op); err == topic.ErrDesyncDetected {
		// Garbage frame. Treat as no-op operation, allowing playback to continue.
		log.WithField("mark", p.fsm.LogMark).Warn("detected de-synchronization")
		return nil
	} else if err != nil {
		return err
	}

	// Run the operation through the FSM to verify validity.
	if fsmErr := p.fsm.Apply(&op, b[topic.FixedFrameHeaderLength:]); fsmErr != nil {
		// Log but otherwise ignore FSM errors: the Player is still in a consistent
		// state, and we may make further progress later in the log.
		if fsmErr == ErrFnodeNotTracked {
			// Fnode is deleted later in the log, and is no longer hinted.
		} else if fsmErr == ErrWrongSeqNo && op.SeqNo < p.fsm.NextSeqNo {
			// |op| is prior to the next hinted SeqNo. We may have started reading
			// from a lower-bound offset, or it may be a duplicated write.
		} else {
			log.WithFields(log.Fields{"op": op, "err": fsmErr}).Warn("playback FSM error")
		}

		// For bytestream consistency Write ops must still skip |op.Length| bytes.
		if op.Write != nil {
			if err := copyFixed(ioutil.Discard, br, op.Write.Length); err != nil {
				return err
			}
		}
		return nil
	}

	// The operation is valid. Apply local playback actions.
	if op.Create != nil {
		return p.create(Fnode(op.SeqNo))
	} else if op.Unlink != nil {
		return p.unlink(op.Unlink.Fnode)
	} else if op.Write != nil {
		recoverBytes.Add(op.Write.Length)
		metrics.RecoveryLogRecoveredBytesTotal.Add(float64(op.Write.Length))
		return p.write(op.Write, br)
	}
	return nil
}

func (p *Player) stagedPath(fnode Fnode) string {
	fname := strconv.FormatInt(int64(fnode), 10)
	return filepath.Join(p.localDir, fnodeStagingDir, fname)
}

func (p *Player) create(fnode Fnode) error {
	backingFile, err := os.OpenFile(p.stagedPath(fnode),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666) // Expect file to not exist.
	if err == nil {
		p.backingFiles[fnode] = backingFile
	}
	return err
}

func (p *Player) unlink(fnode Fnode) error {
	if _, isLive := p.fsm.LiveNodes[fnode]; isLive {
		// Live links remain for |fnode|. Take no action.
		return nil
	}
	backingFile := p.backingFiles[fnode]

	// Close and remove the local backing file.
	if err := backingFile.Close(); err != nil {
		return err
	} else if err = os.Remove(p.stagedPath(fnode)); err != nil {
		return err
	}
	delete(p.backingFiles, fnode)
	return nil
}

func (p *Player) write(op *RecordedOp_Write, r io.Reader) error {
	var backingFile = p.backingFiles[Fnode(op.Fnode)]

	// Seek to the indicated offset.
	if _, err := backingFile.Seek(op.Offset, 0); err != nil {
		return err
	}
	return copyFixed(backingFile, r, op.Length)
}

// Copies exactly |length| bytes from |r| to |w| using temporary buffer |b|.
func copyFixed(w io.Writer, r io.Reader, length int64) error {
	var b = copyBuffers.Get().(*[]byte)
	var n, err = io.CopyBuffer(w, io.LimitReader(r, length), *b)
	copyBuffers.Put(b)

	// Map an EOF prior to |length| bytes as unexpected.
	if err == nil && n != length {
		return io.ErrUnexpectedEOF
	}
	return err
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
	if err := os.Remove(filepath.Join(p.localDir, fnodeStagingDir)); err != nil {
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

var (
	recoverBytes = varz.ObtainCount("gazette", "recoverylog", "recoverBytes")

	copyBuffers = sync.Pool{
		New: func() interface{} {
			var b = make([]byte, 32*1024)
			return &b
		},
	}
)
