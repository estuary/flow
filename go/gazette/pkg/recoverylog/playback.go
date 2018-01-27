package recoverylog

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
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

type Player struct {
	hints     FSMHints
	dir       string
	tailingCh chan struct{}
	handoffCh chan Author
	exitCh    chan *FSM

	// TODO(johnny): Plumb through a Context, and remove this.
	cancelCh <-chan struct{}
}

// NewPlayer returns a new Player for recovering the log indicated by |hints|
// into |dir|. An error is returned if |hints| are invalid.
func NewPlayer(hints FSMHints, dir string) (*Player, error) {
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

// TODO(johnny): Plumb Context through Play, and remove.
// Deprecated. SetCancelChan arranges for a subsequent Play
// invocation to cancel playback upon |cancelCh| becoming select-able.
func (p *Player) SetCancelChan(cancelCh <-chan struct{}) {
	p.cancelCh = cancelCh
}

// Deprecated. Play using the prepared Player. Returns on the first encountered
// unrecoverable error, or upon a successful MakeLive or Handoff.
func (p *Player) Play(client journal.Client) error {
	// Start an adapter to close the playLog Context on cancelCh being selectable.
	// TODO(johnny): Remove after context is plumbed through Play.
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
// pending read operations due to a signal to begin exiting.
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
	if asyncAppend, err = client.Write(fsm.LogMark.Journal, nil); err != nil {
		return
	}
	<-asyncAppend.Ready

	// Offset we expect to have read through prior to exiting.
	var writeHead = asyncAppend.WriteHead

	var reader = newPlayerReader(ctx, fsm.LogMark, client)
	defer func() { reader.abort() }() // Defer must be wrapped, as |reader| may change.

	for {

		if s := fsm.hintedSegments; len(s) != 0 && s[0].FirstOffset > fsm.LogMark.Offset {
			// Use hinted offset to opportunistically skip through dead chunks of the log.
			// Note that FSM is responsible for updating |hintedSegments| as they're applied.
			fsm.LogMark.Offset = s[0].FirstOffset
		} else if state == playerStateBackfill && fsm.LogMark.Offset == writeHead {
			// We've completed back-fill and are now tailing the journal.
			state = playerStateTail
			close(tailingCh)
		} else if fsm.LogMark.Offset == -1 {
			// In the absence of hinted segments, read from the current log head.
			fsm.LogMark.Offset = writeHead
		}

		switch state {
		case playerStateBackfill, playerStateTail, playerStateReadHandoffBarrier:
			// Always perform blocking reads from these states.
			reader = prepareRead(ctx, reader, fsm.LogMark.Offset, true)
		case playerStateExitAtHead, playerStateInjectHandoffAtHead:
			// If we believe we're at the log head and want to do something once we confirm it
			// (exit, or inject a no-op), use non-blocking reads.
			reader = prepareRead(ctx, reader, fsm.LogMark.Offset, fsm.LogMark.Offset < writeHead)
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
			if fsm.LogMark.Offset < writeHead {
				// This error is returned only by a non-blocking reader, and we should have used
				// a non-blocking reader only if we were already at |writeHead|.
				log.WithFields(log.Fields{"mark": fsm.LogMark, "writeHead": writeHead}).
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

				if asyncAppend, err = client.Write(fsm.LogMark.Journal, noop); err != nil {
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
		fsm.LogMark = reader.rr.AdjustedMark(reader.br)

		if reader.rr.LastResult.WriteHead > writeHead {
			writeHead = reader.rr.LastResult.WriteHead
		}

		// Parse and apply the operation. Track the operation author, and whether it
		// was correctly sequenced by the FSM (or otherwise, was ignored).
		var opAuthor Author
		var opApplied bool

		if opAuthor, opApplied, err = playOperation(reader.br, dir, fsm, files); err != nil {
			return
		}

		// Update again to reflect the next operation offset we intend to apply.
		fsm.LogMark = reader.rr.AdjustedMark(reader.br)

		if handoff != 0 && opAuthor == handoff {
			if state != playerStateReadHandoffBarrier {
				log.WithField("state", state).Panic("unexpected state")
			}

			if opApplied {
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

// playOperation reads and applies a single RecordedOp from |br|. It returns
// the operation |author| and whether it was successfully |applied| to the FSM,
// and any other encountered playback |err|.
func playOperation(br *bufio.Reader, dir string, fsm *FSM, files fnodeFileMap) (author Author, applied bool, err error) {
	var b []byte
	var op RecordedOp

	if b, err = topic.FixedFraming.Unpack(br); err != nil {
		return
	} else if err = topic.FixedFraming.Unmarshal(b, &op); err == topic.ErrDesyncDetected {
		// Garbage frame. Treat as no-op operation, allowing playback to continue.
		err = nil
		log.WithField("mark", fsm.LogMark).Warn("detected de-synchronization")
		return
	} else if err != nil {
		return
	}

	author = op.Author

	// Run the operation through the FSM to verify validity.
	if fsmErr := fsm.Apply(&op, b[topic.FixedFrameHeaderLength:]); fsmErr != nil {
		// Log but otherwise ignore FSM errors: the Player is still in a consistent
		// state, and we may make further progress later in the log.
		if fsmErr == ErrFnodeNotTracked {
			// Fnode is deleted later in the log, and is no longer hinted.
		} else if fsmErr == ErrNotHintedAuthor {
			// The FSM has remaining playback hints, and this operation doesn't match
			// the next expected Author. This happens frequently during Recorder hand-off;
			// the operation is a dead branch of the log.
		} else if fsmErr == ErrWrongSeqNo && op.SeqNo < fsm.NextSeqNo {
			// |op| is prior to the next hinted SeqNo. We may have started reading
			// from a lower-bound offset, or it may be a duplicated write.
		} else {
			log.WithFields(log.Fields{"mark": fsm.LogMark, "op": op, "err": fsmErr}).Warn("playback FSM error")
		}

		// For bytestream consistency Write ops must still skip |op.Length| bytes.
		if op.Write != nil {
			err = copyFixed(ioutil.Discard, br, op.Write.Length)
		}
		return
	}

	applied = true

	// The operation is valid. Apply local playback actions.
	if op.Create != nil {
		err = create(dir, Fnode(op.SeqNo), files)
	} else if op.Unlink != nil {
		err = unlink(dir, op.Unlink.Fnode, fsm, files)
	} else if op.Write != nil {
		metrics.RecoveryLogRecoveredBytesTotal.Add(float64(op.Write.Length))
		err = write(op.Write, br, files)
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
