package recoverylog

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/metrics"
	"github.com/LiveRamp/gazette/pkg/topic"
)

// Player reads from a log to rebuild encoded file operations onto the local filesystem.
type Player struct {
	hints     FSMHints
	dir       string
	tailingCh chan struct{}
	handoffCh chan Author
	exitCh    chan *FSM

	// TODO(johnny): Plumb through a Context, and remove this (Issue #37).
	cancelCh <-chan struct{}
}

// NewPlayer returns a new Player for recovering the log indicated by |hints|
// into |dir|. An error is returned if |hints| are invalid.
func NewPlayer(hints FSMHints, dir string) (*Player, error) {
	// Validate |hints| are well formed, but discard the built FSM
	// (PlayContext, typically run in a spawned goroutine, manages its own).
	if _, err := NewFSM(hints); err != nil {
		return nil, err
	}
	return &Player{
		hints:     hints,
		dir:       dir,
		tailingCh: make(chan struct{}),
		handoffCh: make(chan Author, 1),
		exitCh:    make(chan *FSM),
	}, nil
}

// PlayContext uses the prepared Player to play back the log. It returns on the
// first encountered unrecoverable error, including context cancellation, or
// upon a successful MakeLive or Handoff.
func (p *Player) PlayContext(ctx context.Context, client journal.Client) error {
	return playLog(ctx, p.hints, p.dir, client, p.tailingCh, p.handoffCh, p.exitCh)
}

// FinishAtWriteHead requests that playback complete upon reaching the current write
// head. If Play returned without an error, FinishAtWriteHead will return its resulting
// FSM (and will otherwise return nil). Only one invocation of FinishAtWriteHead or
// InjectHandoff may be made of a Player instance.
func (p *Player) FinishAtWriteHead() *FSM {
	p.handoffCh <- 0
	close(p.handoffCh)
	return <-p.exitCh
}

// InjectHandoff requests that playback complete upon injecting a no-op handoff of
// the given |author| at the recoverylog head. If Play returned without an error,
// InjectHandoff will return its resulting FSM (and will otherwise return nil).
// Only one invocation of InjectHandoff or FinishAtWriteHead may be made of a
// Player instance. |author| must be non-zero or InjectHandoff panics.
func (p *Player) InjectHandoff(author Author) *FSM {
	if author == 0 {
		log.WithField("author", author).Panic("author must be non-zero")
	}
	p.handoffCh <- author
	close(p.handoffCh)
	return <-p.exitCh
}

// IsTailing returns true if playback has reached the log write head and is
// tailing new log operations as they arrive.
func (p *Player) IsTailing() bool {
	select {
	case <-p.tailingCh:
		return true
	default:
		return false
	}
}

// SetCancelChan arranges for a subsequent Play invocation to cancel playback upon
// |cancelCh| becoming select-able.
//
// Deprecated: Plumb Context through Play, and remove (Issue #37).
func (p *Player) SetCancelChan(cancelCh <-chan struct{}) {
	p.cancelCh = cancelCh
}

// Play using the prepared Player. Returns on the first encountered
// unrecoverable error, or upon a successful MakeLive or Handoff.
//
// Deprecated: Remove after context is plumbed through (Issue #37).
func (p *Player) Play(client journal.Client) error {
	// Start an adapter to close the playLog Context on cancelCh being selectable.
	var ctx, cancel = context.WithCancel(context.TODO())
	if p.cancelCh != nil {
		go func() {
			<-p.cancelCh
			cancel()
		}()
	}
	return p.PlayContext(ctx, client)
}

// playerReader is a cancel-able, buffered RetryReader which may be
// asynchronously Peeked. This is a requirement for the playback loop, which
// generally wants to use blocking reads while retaining an ability to cancel
// pending read operations upon a signal to begin exiting.
type playerReader struct {
	rr          journal.RetryReader
	br          *bufio.Reader      // Wraps |rr|.
	cancelCtx   context.CancelFunc // Cancels the |rr| Context.
	peekReqCh   chan<- struct{}    // Signals to begin a new Peek.
	peekRespCh  <-chan error       // Signalled with the result of a Peek.
	pendingPeek bool               // Indicates that a Peek is underway.
}

func newPlayerReader(ctx context.Context, mark journal.Mark, getter journal.Getter) *playerReader {
	var pr = &playerReader{
		rr: journal.RetryReader{
			MarkedReader: journal.MarkedReader{Mark: mark},
			Getter:       getter,
		},
	}
	pr.rr.Context, pr.cancelCtx = context.WithCancel(ctx)
	pr.br = bufio.NewReader(&pr.rr)

	var reqCh = make(chan struct{}, 1)
	var respCh = make(chan error, 1)

	// Start a "peek pump", which allows us to request that a peek operation
	// happen in the background to prime for reading the next operation.
	go func(br *bufio.Reader, reqCh <-chan struct{}, respCh chan<- error) {
		for range reqCh {
			var _, err = br.Peek(1)
			respCh <- err
		}

		if err := pr.rr.MarkedReader.Close(); err != nil {
			log.WithField("err", err).Warn("error on close")
		}
		close(respCh)
	}(pr.br, reqCh, respCh)

	pr.peekReqCh = reqCh
	pr.peekRespCh = respCh

	return pr
}

func (pr *playerReader) peek() <-chan error {
	if pr.pendingPeek == false {
		pr.pendingPeek = true
		pr.peekReqCh <- struct{}{}
	}
	return pr.peekRespCh
}

func (pr *playerReader) abort() {
	pr.cancelCtx()
	close(pr.peekReqCh)
}

// prepareRead ensures |pr| is prepared to read |nextOffset| in blocking mode |block|.
// It may cancel |pr| and return a new *playerReader in its place.
func prepareRead(ctx context.Context, pr *playerReader, nextOffset int64, block bool) *playerReader {
	// Note that |pr.rr.MarkedReader| and |pr.br| cannot be concurrently accessed
	// if |pr.pendingPeek| is true. The one exception is |MarkedReader.Mark.Journal|,
	// which is never updated by a concurrent Read operation.

	if block && !pr.rr.Blocking {
		// We're switching to blocking mode. We can update the current reader to
		// start blocking with its next read operation.
		pr.rr.Blocking = true
	} else if !block && pr.rr.Blocking {

		if pr.pendingPeek || pr.rr.MarkedReader.ReadCloser != nil {
			// As a blocking read is underway. We must begin a new, non-blocking
			// read operation and cancel the older reader in the background.
			var mark = journal.Mark{Journal: pr.rr.MarkedReader.Mark.Journal, Offset: nextOffset}

			pr.abort()
			pr = newPlayerReader(ctx, mark, pr.rr.Getter)
		}
		pr.rr.Blocking = false
	}

	if !pr.pendingPeek && pr.rr.AdjustedMark(pr.br).Offset != nextOffset {
		// Seek the RetryReader forward to the next hinted offset.
		if _, err := pr.rr.Seek(nextOffset, io.SeekStart); err != nil {
			panic(err) // The contract for RetryReader.Seek is that it never return an error.
		}
		pr.br.Reset(&pr.rr)
	}

	return pr
}

type fnodeFileMap map[Fnode]*os.File

// playerState models the recovery-log playback state machine.
type playerState int

const (
	// Player is reading historical log content.
	playerStateBackfill playerState = iota
	// Player is tailing new log content as it is written.
	playerStateTail playerState = iota
	// Player will exit after reading through the current log head.
	playerStateExitAtHead playerState = iota
	// Player will inject a write barrier to attempt hand-off after reading through the log head.
	playerStateInjectHandoffAtHead playerState = iota
	// Player previously injected a write barrier, and is waiting to read it & determine if hand-off was successful.
	playerStateReadHandoffBarrier playerState = iota
	// Player has completed playback (terminal state).
	playerStateComplete playerState = iota
)

// playLog applies |hints| to play the log (indicated by |hints|) into local
// directory |dir|. It returns an encountered error (which aborts playback),
// and otherwise blocks indefinitely until signalled by |handoffCh|. If signaled
// with a zero-valued Author, playLog exits upon reaching the log head. Otherwise,
// playLog exits upon injecting a properly sequenced no-op RecordedOp which encodes
// the provided Author. If no error is returned, on exit |exitCh| is signaled with
// the FSM recovered after playback.
func playLog(ctx context.Context, hints FSMHints, dir string, client journal.Client,
	tailingCh chan<- struct{}, handoffCh <-chan Author, exitCh chan<- *FSM) (err error) {

	var state = playerStateBackfill
	var fsm *FSM
	var files = make(fnodeFileMap) // Live Fnodes backed by local files.
	var handoff Author             // Author we will hand-off to on exit.

	// Error checks in this function consistently use |err| prior to returning.
	defer func() {
		if err != nil {
			cleanupAfterAbort(dir, files)
		} else if state != playerStateComplete {
			// |err| should be nil only on a successful playback completion.
			log.WithField("state", state).Panic("unexpected state")
		} else {
			exitCh <- fsm
		}
		close(exitCh)
	}()

	if fsm, err = NewFSM(hints); err != nil {
		return
	} else if err = preparePlayback(dir); err != nil {
		return
	}

	// Issue a write barrier to determine the transactional, current log head.
	var asyncAppend *journal.AsyncAppend
	if asyncAppend, err = client.Write(hints.Log, nil); err != nil {
		return
	}
	<-asyncAppend.Ready

	// Next journal |mark| being read.
	var mark = journal.Mark{Journal: hints.Log, Offset: 0}
	// Offset we expect to have read through prior to exiting
	var writeHead = asyncAppend.WriteHead

	var reader = newPlayerReader(ctx, mark, client)
	defer func() { reader.abort() }() // Defer must be wrapped, as |reader| may change.

	for {

		if s := fsm.hintedSegments; len(s) != 0 && s[0].FirstOffset > mark.Offset {
			// Use hinted offset to opportunistically skip through dead chunks of the log.
			// Note that FSM is responsible for updating |hintedSegments| as they're applied.
			mark.Offset = s[0].FirstOffset
		} else if state == playerStateBackfill && mark.Offset == writeHead {
			// We've completed back-fill and are now tailing the journal.
			state = playerStateTail
			close(tailingCh)
		}

		switch state {
		case playerStateBackfill, playerStateTail, playerStateReadHandoffBarrier:
			// Always perform blocking reads from these states.
			reader = prepareRead(ctx, reader, mark.Offset, true)
		case playerStateExitAtHead, playerStateInjectHandoffAtHead:
			// If we believe we're at the log head and want to do something once we confirm it
			// (exit, or inject a no-op), use non-blocking reads.
			reader = prepareRead(ctx, reader, mark.Offset, mark.Offset < writeHead)
		}

		// Begin a read of the next operation. Wait for it to error or for the
		// first byte to be ready, or to be signaled for hand-off.
		select {
		case err = <-reader.peek():
			reader.pendingPeek = false
			break

		case handoff = <-handoffCh:
			// We've been signaled to complete playback and exit when we're able.
			switch state {
			case playerStateBackfill, playerStateTail:
				if handoff == 0 {
					state = playerStateExitAtHead
				} else {
					state = playerStateInjectHandoffAtHead
				}
			default:
				log.WithField("state", state).Panic("unexpected state")
			}
			handoffCh = nil // Do not select again.
			continue
		}

		if err == journal.ErrNotYetAvailable {
			if mark.Offset < writeHead {
				// This error is returned only by a non-blocking reader, and we should have used
				// a non-blocking reader only if we were already at |writeHead|.
				log.WithFields(log.Fields{"mark": mark, "writeHead": writeHead}).
					Panic("unexpected ErrNotYetAvailable")
			}

			switch state {
			case playerStateExitAtHead:
				state = playerStateComplete
				err = makeLive(dir, fsm, files)
				return

			case playerStateInjectHandoffAtHead:
				var noop = frameRecordedOp(RecordedOp{
					SeqNo:    fsm.NextSeqNo,
					Checksum: fsm.NextChecksum,
					Author:   handoff,
				}, nil)

				if asyncAppend, err = client.Write(mark.Journal, noop); err != nil {
					return
				}
				<-asyncAppend.Ready

				// We next must read through the op we just wrote.
				state, writeHead = playerStateReadHandoffBarrier, asyncAppend.WriteHead

			default:
				log.WithField("state", state).Panic("invalid state")
			}
		} else if err != nil {
			// Any other Peek error aborts playback.
			return
		}

		// Gazette read operations can potentially update the log mark. Specifically,
		// if the requested offset has been deleted from the log, the broker will
		// return the next available offset.
		mark = reader.rr.AdjustedMark(reader.br)
		// The read result may have told us that the journal write-head increased,
		// in which case we want to be sure to read through that offset.
		if reader.rr.LastResult.WriteHead > writeHead {
			writeHead = reader.rr.LastResult.WriteHead
		}

		var op RecordedOp
		var applied bool

		if op, applied, err = playOperation(reader.br, mark, fsm, dir, files); err != nil {
			return // playOperation returns only unrecoverable errors.
		}

		// Update |mark| again to reflect the next operation offset we intend to apply.
		// For byte-stream consistency, un-applied Write ops must skip |op.Length| bytes of the stream.
		if mark = reader.rr.AdjustedMark(reader.br); !applied && op.Write != nil {
			mark.Offset += op.Write.Length
		}
		if op.LastOffset != 0 && mark.Offset != op.LastOffset {
			log.WithFields(log.Fields{"op": op, "mark": mark}).Panic("LastOffset must agree with mark")
		}

		// Are we attempting to inject a hand-off and this operation matches our Author?
		if handoff != 0 && op.Author == handoff {
			if state != playerStateReadHandoffBarrier {
				log.WithField("state", state).Panic("unexpected state")
			}

			if applied {
				// We successfully sequenced a no-op into the log, taking control of
				// the log from a current recorder (if one exists).
				state = playerStateComplete
				err = makeLive(dir, fsm, files)
				return
			}

			// We lost the race to inject our write operation, and must try again.
			state = playerStateInjectHandoffAtHead
		}
	}
}

func preparePlayback(dir string) error {
	// File nodes are staged into a directory within |dir| during playback.
	var fileNodesDir = filepath.Join(dir, fnodeStagingDir)

	// Remove all prior content under |dir|.
	if err := os.RemoveAll(dir); err != nil {
		return err
	} else if err := os.MkdirAll(fileNodesDir, 0777); err != nil {
		return err
	}
	return nil
}

func cleanupAfterAbort(dir string, files fnodeFileMap) {
	for _, fnode := range files {
		if err := fnode.Close(); err != nil {
			log.WithField("err", err).Warn("closing fnode after abort")
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		log.WithField("err", err).Warn("removing local directory after abort")
	}
}

// decodeOperation unpacks, unmarshals, and sets offsets of a RecordedOp from Reader |br| at |offset|.
func decodeOperation(br *bufio.Reader, offset int64) (op RecordedOp, frame []byte, err error) {
	if frame, err = topic.FixedFraming.Unpack(br); err != nil {
		return
	} else if err = topic.FixedFraming.Unmarshal(frame, &op); err != nil {
		return
	}

	// First and last offsets are meta-fields never populated by Recorder, and known only upon playback.
	op.FirstOffset, op.LastOffset = offset, offset+int64(len(frame))

	if op.Write != nil {
		op.LastOffset += op.Write.Length
	}
	return
}

// applyOperation attempts to transition |fsm| with operation |op|. It returns whether
// a state transition was applied, or whether an unexpected FSM error occurred. Common and
// expected FSM errors are squelched.
func applyOperation(op RecordedOp, frame []byte, fsm *FSM) (bool, error) {
	if err := fsm.Apply(&op, frame[topic.FixedFrameHeaderLength:]); err == nil {
		return true, nil
	} else if err == ErrFnodeNotTracked {
		// Fnode is hinted as being deleted later in the log. This occurs regularly
		// and we handle by not applying local filesystem operations for this Fnode
		// (as we know it'll be deleted anyway).
	} else if err == ErrNotHintedAuthor {
		// The FSM has remaining playback hints, and this operation doesn't match
		// the next expected Author. This happens frequently during Recorder hand-off;
		// the operation is a dead branch of the log.
	} else if err == ErrWrongSeqNo && op.SeqNo < fsm.NextSeqNo {
		// |op| is prior to the next hinted SeqNo. We may have started reading
		// from a lower-bound offset, or it may be a duplicated write.
	} else {
		return false, err
	}
	return false, nil
}

// reenactOperation replays local file actions represented by RecordedOp |op|, which has been applied to |fsm|.
func reenactOperation(op RecordedOp, fsm *FSM, br *bufio.Reader, dir string, files fnodeFileMap) error {
	if op.Create != nil {
		return create(dir, Fnode(op.SeqNo), files)
	} else if op.Unlink != nil {
		return unlink(dir, op.Unlink.Fnode, fsm, files)
	} else if op.Write != nil {
		metrics.RecoveryLogRecoveredBytesTotal.Add(float64(op.Write.Length))
		return write(op.Write, br, files)
	}
	return nil
}

// playOperation composes operation decode, application, and re-enactment. It logs warnings on recoverable
// errors, and surfaces only those which should abort playback.
func playOperation(br *bufio.Reader, mark journal.Mark, fsm *FSM, dir string,
	files fnodeFileMap) (op RecordedOp, applied bool, err error) {

	// Unpack the next frame and its unmarshaled RecordedOp.
	var frame []byte
	if op, frame, err = decodeOperation(br, mark.Offset); err != nil {
		if err == topic.ErrDesyncDetected {
			// ErrDesyncDetected is returned by FixedFraming.Unmarshal (and not Unpack, meaning the reader
			// is still in a good state). This frame is garbage, but playback can continue.
			log.WithField("mark", mark).Warn("detected de-synchronization")
			err = nil
		}
		// Other errors are unexpected & unrecoverable.
		return
	}

	// Attempt to transition the FSM by the operation, and if applies, reenact the local filesystem action.
	if applied, err = applyOperation(op, frame, fsm); err != nil {
		// We expect FSM errors to be rare, but they can happen under normal operation (eg, during
		// hand-off, or if multiple Recorders briefly run concurrently, creating branched log histories).
		log.WithFields(log.Fields{"mark": mark, "err": err, "op": op}).Info("did not apply FSM operation")
		err = nil
	} else if applied {
		err = reenactOperation(op, fsm, br, dir, files)
	}
	return
}

func stagedPath(dir string, fnode Fnode) string {
	var fname = strconv.FormatInt(int64(fnode), 10)
	return filepath.Join(dir, fnodeStagingDir, fname)
}

func create(dir string, fnode Fnode, files fnodeFileMap) error {
	var file, err = os.OpenFile(stagedPath(dir, fnode),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666) // Expect file to not exist.

	if err == nil {
		files[fnode] = file
	}
	return err
}

func unlink(dir string, fnode Fnode, fsm *FSM, files fnodeFileMap) error {
	if _, isLive := fsm.LiveNodes[fnode]; isLive {
		// Live links remain for |fnode|. Take no action.
		return nil
	}
	var file = files[fnode]

	// Close and remove the local backing file.
	if err := file.Close(); err != nil {
		return err
	} else if err = os.Remove(stagedPath(dir, fnode)); err != nil {
		return err
	}
	delete(files, fnode)
	return nil
}

func write(op *RecordedOp_Write, r io.Reader, files fnodeFileMap) error {
	var file = files[Fnode(op.Fnode)]

	// Seek to the indicated offset.
	if _, err := file.Seek(op.Offset, 0); err != nil {
		return err
	}
	return copyFixed(file, r, op.Length)
}

// copyFixed is like io.CopyN, but treats an EOF prior to |length| as an
// ErrUnexpectedEOF. It also uses a |copyBuffers| pool to avoid a buffer
// allocation.
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

// makeLive links staged Fnode |files| into each of their hard link locations
// indicated by |fsm| under |dir|, and creates any property files of |fsm|.
// |files| must exactly match live nodes of |fsm| or makeLive panics.
func makeLive(dir string, fsm *FSM, files fnodeFileMap) error {
	if fsm.hasRemainingHints() {
		return fmt.Errorf("FSM has remaining unused hints: %+v", fsm)
	}
	for fnode, liveNode := range fsm.LiveNodes {
		var file = files[fnode]
		delete(files, fnode)

		// Link backing-file into target paths.
		for link := range liveNode.Links {
			targetPath := filepath.Join(dir, link)

			if err := os.MkdirAll(filepath.Dir(targetPath), 0777); err != nil {
				return err
			} else if err = os.Link(stagedPath(dir, fnode), targetPath); err != nil {
				return err
			}
			log.WithFields(log.Fields{"fnode": fnode, "target": targetPath}).Info("linked file")
		}
		// Close and removed the staged file.
		if err := file.Close(); err != nil {
			return err
		} else if err = os.Remove(stagedPath(dir, fnode)); err != nil {
			return err
		}
	}
	if len(files) != 0 {
		// Invariant: |FSM.LiveNodes| should fully describe all backing files.
		log.WithField("files", files).Panic("backing files not in FSM")
	}
	// Remove staging directory.
	if err := os.Remove(filepath.Join(dir, fnodeStagingDir)); err != nil {
		return err
	}

	// Write property files.
	for path, content := range fsm.Properties {
		var targetPath = filepath.Join(dir, path)

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

// Subdirectory into which Fnodes are played-back.
const fnodeStagingDir = ".fnodes"

var (
	copyBuffers = sync.Pool{
		New: func() interface{} {
			var b = make([]byte, 32*1024)
			return &b
		},
	}
)
