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

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// Player reads from a log to rebuild encoded file operations onto the local filesystem.
// Player is a future: on completion of a successful Play (or equivalently, upon
// Done selecting) |Resolved| may be inspected for recovered playback state.
// |Resolved| is not valid before Play completes, or if Play returns an error.
type Player struct {
	// Resolved outputs which may be read once the Play completes.
	Resolved struct {
		Log pb.Journal // Primary Log which was played back (i.e., FSMHints.Log).
		FSM *FSM       // FSM recovered at Play completion.
		Dir string     // Local directory into which the log is recovered.
	}

	handoffCh chan Author   // Coordinates Player completion (& hand-off to a new Recorder).
	tailingCh chan struct{} // Closed when Player reaches (and is tailing) the live log.
	doneCh    chan struct{} // Closed when Player.Play completes.
}

// NewPlayer returns a new Player for recovering a log.
func NewPlayer() *Player {
	return &Player{
		handoffCh: make(chan Author, 1),
		tailingCh: make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Play uses the prepared Player to play back the FSMHints. It returns on the
// first encountered unrecoverable error, including context cancellation, or
// upon a successful FinishAtWriteHead or InjectHandoff.
func (p *Player) Play(ctx context.Context, hints FSMHints, dir string, ajc client.AsyncJournalClient) error {
	defer close(p.doneCh)

	if fsm, err := playLog(ctx, hints, dir, ajc, p.tailingCh, p.handoffCh); err != nil {
		return err
	} else {
		p.Resolved.Log = hints.Log
		p.Resolved.FSM = fsm
		p.Resolved.Dir = dir
		return nil
	}
}

// FinishAtWriteHead requests that playback complete upon reaching the current
// write head. Only one invocation of FinishAtWriteHead or InjectHandoff may be
// made of a Player instance.
func (p *Player) FinishAtWriteHead() {
	p.handoffCh <- 0
	close(p.handoffCh)
}

// InjectHandoff requests that playback complete upon injecting a no-op handoff
// of the given |author| at the log head. Only one invocation of InjectHandoff
// or FinishAtWriteHead may be made of a Player instance. |author| must be non-
// zero or InjectHandoff panics.
func (p *Player) InjectHandoff(author Author) {
	if author == 0 {
		log.WithField("author", author).Panic("author must be non-zero")
	}
	p.handoffCh <- author
	close(p.handoffCh)
}

// Tailing returns a channel which selects when Play has reached the log
// write head, and is tailing new log operations as they arrive.
func (p *Player) Tailing() <-chan struct{} {
	return p.tailingCh
}

// Done returns a channel which selects when Play has completed. If Play
// returned no error, then Player.Resolved will hold the recovered playback
// state. Otherwise, Player.Resolved will be zero-valued.
func (p *Player) Done() <-chan struct{} {
	return p.doneCh
}

// playerReader is a buffered RetryReader which may be asynchronously Peeked.
// This is a requirement for the playback loop, which generally wants to use
// blocking reads, while retaining an ability to cancel a blocking read at or
// near the log write head where the player wishes to complete playback.
type playerReader struct {
	rr          *client.RetryReader
	br          *bufio.Reader   // Wraps |rr|.
	peekReqCh   chan<- struct{} // Signals to begin a new Peek.
	peekRespCh  <-chan error    // Signalled with the result of a Peek.
	pendingPeek bool            // Indicates that a Peek is underway.
	block       bool            // Block field of ReadRequest. Retained to avoid a data race.
}

func newPlayerReader(ctx context.Context, name pb.Journal, ajc client.AsyncJournalClient) *playerReader {
	var rr = client.NewRetryReader(ctx, ajc, pb.ReadRequest{
		Journal:    name,
		Block:      true,
		DoNotProxy: !ajc.IsNoopRouter(),
	})

	var reqCh = make(chan struct{}, 1)
	var respCh = make(chan error, 1)

	var pr = &playerReader{
		rr:         rr,
		br:         bufio.NewReaderSize(rr, 32*1024),
		peekReqCh:  reqCh,
		peekRespCh: respCh,
		block:      rr.Reader.Request.Block,
	}

	// Start a "peek pump", which allows us to request that a peek operation
	// happen in the background to prime for reading the next operation.
	go func(br *bufio.Reader, reqCh <-chan struct{}, respCh chan<- error) {
		for range reqCh {
			var _, err = br.Peek(message.FixedFrameHeaderLength)
			respCh <- err
		}
		close(respCh)
	}(pr.br, reqCh, respCh)

	return pr
}

// peek begins a background Peek, if one is not underway, and returns a channel
// which will select with its result.
func (pr *playerReader) peek() <-chan error {
	if !pr.pendingPeek {
		pr.pendingPeek = true
		pr.peekReqCh <- struct{}{}
	}
	return pr.peekRespCh
}

func (pr *playerReader) seek(name pb.Journal, offset int64) int64 {
	if pr.pendingPeek {
		panic("expected !pendingPeek") // Cannot concurrently access |pr.rr|.
	} else if name != pr.rr.Journal() {
		pr.rr.Cancel()
		pr.rr.Restart(pb.ReadRequest{
			Journal:    name,
			Offset:     offset,
			Block:      pr.block,
			DoNotProxy: pr.rr.Reader.Request.DoNotProxy,
		})
		pr.br.Reset(pr.rr)

		return offset
	} else if offset, err := pr.rr.AdjustedSeek(offset, io.SeekStart, pr.br); err != nil {
		panic(err) // The contract for RetryReader.Seek is that it never return an error.
	} else {
		return offset
	}
}

// setBlocking toggles the blocking mode of the reader.
func (pr *playerReader) setBlocking(block bool) {
	if block && !pr.block {
		if pr.pendingPeek {
			panic("expected !pendingPeek")
		}
		// We're switching to blocking mode. We can update the current reader to
		// start blocking with its next request, and it will retry on
		// ErrOffsetNotYetAvailable.
		pr.rr.Reader.Request.Block = true
		pr.block = true

	} else if !block && pr.block {
		// A blocking read is underway. We cancel and restart the reader as it may
		// otherwise block indefinitely, and we drain a |pendingPeek|.

		if pr.rr.Cancel(); pr.pendingPeek {
			<-pr.peekRespCh // Now selects, due to cancellation.
			pr.pendingPeek = false
		}
		pr.rr.Reader.Request.Block = false
		pr.block = false
		pr.rr.Restart(pr.rr.Reader.Request)
	}
}

func (pr *playerReader) close() {
	pr.rr.Cancel()
	close(pr.peekReqCh)
}

type fnodeFileMap map[Fnode]*os.File

// playerState models the recovery-log playback state machine.
type playerState int

const (
	// Player is reading historical log content.
	playerStateBackfill playerState = iota
	// Player is tailing new log content as it is written.
	playerStateTail
	// Player will exit after reading through the current log head.
	playerStateExitAtHead
	// Player will inject a write barrier to attempt hand-off after reading through the log head.
	playerStateInjectHandoffAtHead
	// Player previously injected a write barrier, and is waiting to read it & determine if hand-off was successful.
	playerStateReadHandoffBarrier
	// Player has completed playback (terminal state).
	playerStateComplete
)

// playLog applies |hints| to play the log (indicated by |hints|) into local
// directory |dir|. It returns an encountered error (which aborts playback),
// and otherwise blocks indefinitely until signalled by |handoffCh|. If signaled
// with a zero-valued Author, playLog exits upon reaching the log head. Otherwise,
// playLog exits upon injecting a properly sequenced no-op RecordedOp which encodes
// the provided Author. The recovered FSM is returned on success.
func playLog(ctx context.Context, hints FSMHints, dir string, ajc client.AsyncJournalClient,
	tailingCh chan<- struct{}, handoffCh <-chan Author) (fsm *FSM, err error) {

	var state = playerStateBackfill
	var files = make(fnodeFileMap) // Live Fnodes backed by local files.
	var handoff Author             // Author we will hand-off to on exit.

	// Error checks in this function consistently use |err| prior to returning.
	defer func() {
		if err != nil {
			cleanupOnAbort(dir, files)
		} else if state != playerStateComplete {
			// |err| should be nil only on a successful playback completion.
			log.WithField("state", state).Panic("unexpected state on return")
		}
	}()

	if fsm, err = NewFSM(hints); err != nil {
		err = extendErr(err, "NewFSM")
		return
	} else if err = preparePlayback(dir); err != nil {
		err = extendErr(err, "preparePlayback(%v)", dir)
		return
	}

	// Issue write barriers to determine the transactional,
	// current write heads of every log named by hints.
	var barriers = make(map[pb.Journal]*client.AsyncAppend)
	var queueBarrier = func(log pb.Journal) {
		if _, ok := barriers[log]; ok {
			return
		}
		var aa = ajc.StartAppend(pb.AppendRequest{Journal: log}, nil)
		if err := aa.Release(); err != nil {
			panic(err) // client.AsyncAppend contract prohibits this.
		}
		barriers[log] = aa
	}

	queueBarrier(hints.Log)
	for _, segment := range fsm.hintedSegments {
		// Add other logs named by hinted segments (which could be empty).
		queueBarrier(segment.Log)
	}

	// Block until all barriers resolve.
	for log, aa := range barriers {
		if err = aa.Err(); err != nil {
			err = extendErr(err, "determining head of %q", log)
			return
		}
	}

	// Sanity-check: all hinted segment offsets should be less than |readThrough|.
	for _, segment := range fsm.hintedSegments {
		if e := barriers[segment.Log].Response().Commit.End; segment.FirstOffset >= e || segment.LastOffset > e {
			err = errors.Errorf(
				"max write-head of %v is %d, vs hinted segment %#v; possible data loss",
				segment.Log, e, segment)
			return
		}
	}

	var readLog pb.Journal // Log currently being read.
	var offset int64       // Next offset to read.
	var readThrough int64  // Write head of the current |log|.

	var reader = newPlayerReader(ctx, readLog, ajc)
	defer reader.close()

	for {

		if len(fsm.hintedSegments) != 0 {
			var segment = fsm.hintedSegments[0] // Next unread, hinted segment of the log.

			if readLog != segment.Log || offset < segment.FirstOffset {
				// Use hinted offset to opportunistically skip through dead chunks of the log.
				// Note that FSM is responsible for updating |hintedSegments| as they're applied.
				readLog = segment.Log
				offset = reader.seek(segment.Log, segment.FirstOffset)
				readThrough = barriers[segment.Log].Response().Commit.End
			} else if offset >= readThrough {
				// We've read through |readThrough|, but still have not read all hinted log segments.
				err = errors.Errorf("offset %v:%d >= readThrough %d, but FSM has unused hints; possible data loss",
					readLog, offset, readThrough)
				return
			}
		} else if readLog == "" {
			// There were no hinted segments. Read the log from byte zero.
			readLog = hints.Log
			offset = reader.seek(hints.Log, 0)
			readThrough = barriers[hints.Log].Response().Commit.End
		} else if readLog != hints.Log {
			// There were hinted segments, but the final segment read a different
			// log than the top-level log of the FSM hints.
			// FSMHints.LiveLogHints() checks for and errors against this case.
			panic(fmt.Sprintf("reading %s but expected to be reading %s after consuming all hints",
				readLog, hints.Log))
		}

		switch state {
		case playerStateTail, playerStateReadHandoffBarrier:
			reader.setBlocking(true)
		case playerStateBackfill, playerStateExitAtHead, playerStateInjectHandoffAtHead:
			// If we believe we're at the log head and want to do something once we confirm it
			// (exit, or inject a no-op, or signal we're now tailing), use non-blocking reads.
			reader.setBlocking(offset < readThrough)
		}

		// Begin a read of the next operation. Wait for it to error or for the
		// first byte to be ready, or to be signaled for hand-off.
		select {
		case err = <-reader.peek():
			reader.pendingPeek = false

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
				log.WithField("state", state).Panic("unexpected state on <-handoffCh")
			}
			handoffCh = nil // Do not select again.
			continue
		}

		// A reader.peek() completed with |err|.

		if err == client.ErrOffsetNotYetAvailable {
			if offset < readThrough {
				// This error is returned only by a non-blocking reader, and we should have used
				// a non-blocking reader only if we were already at |readThrough|.
				log.WithFields(log.Fields{"log": readLog, "offset": offset, "readThrough": readThrough}).
					Panic("unexpected ErrNotYetAvailable")
			} else if readLog != hints.Log {
				// Similarly, for hinted logs *other* than the primary hints.Log, we must have already
				// returned an error rather than continue to read beyond |readThrough|.
				log.WithFields(log.Fields{"log": readLog, "hints.Log": hints.Log}).
					Panic("offset >= readThrough, but read log != hints.Log")
			}

			if tailingCh != nil {
				// Signal that we've caught up to the approximate log write-head.
				close(tailingCh)
				tailingCh = nil
			}

			switch state {
			case playerStateBackfill:
				state = playerStateTail // Begin a blocking tail of the journal.
				continue

			case playerStateExitAtHead:
				state = playerStateComplete
				if err = makeLive(dir, fsm, files); err != nil {
					err = extendErr(err, "makeLive after reaching log head")
				}
				return

			case playerStateInjectHandoffAtHead:
				// No-op which is sequenced for the log as read so far.
				var noop []byte
				if noop, err = message.EncodeFixedProtoFrame(&RecordedOp{
					SeqNo:    fsm.NextSeqNo,
					Checksum: fsm.NextChecksum,
					Author:   handoff,
				}, nil); err != nil {
					panic(err) // Cannot fail to encode.
				}

				// Start an append of |noop| which also places a cooperative fence,
				// preventing further appends from an older "zombie" recorder.
				var txn = ajc.StartAppend(pb.AppendRequest{
					Journal:        hints.Log,
					UnionRegisters: handoff.Fence(),
				}, nil)
				_, _ = txn.Writer().Write(noop)

				if err = txn.Release(); err == nil {
					err = txn.Err()
				}
				if err != nil {
					err = extendErr(err, "injecting no-op")
					return
				}
				// We next must read through the op we just wrote.
				state, readThrough = playerStateReadHandoffBarrier, txn.Response().Commit.End
				continue

			default:
				log.WithField("state", state).Panic("invalid state")
			}

		} else if err == client.ErrOffsetJump {
			// ErrOffsetJump indicates the next byte of available content is at an
			// offset larger than the one requested. This can happen if a range of
			// content was deleted from the log.
			var jumpTo = reader.rr.Offset()

			// Did the offset jump over a hinted portion of the log? We cannot recover from this error.
			if s := fsm.hintedSegments; len(s) != 0 && s[0].LastOffset != 0 && s[0].LastOffset < jumpTo {
				err = errors.Errorf("offset jumps over hinted segment of %v (from: %d, to: %d, hinted range: %d-%d); possible data loss",
					readLog, offset, jumpTo, s[0].FirstOffset, s[0].LastOffset)
				return
			}
			// Otherwise, we can continue playing the log from the jumped-to offset.
			log.WithFields(log.Fields{"log": readLog, "from": offset, "to": jumpTo}).
				Warn("recoverylog offset jump")

			offset = jumpTo
			continue

		} else if err != nil {
			// Any other Peek error aborts playback.
			err = extendErr(err, "playerReader.peek")
			return
		}

		// A Peek has completed (err == nil), and the next RecordedOp is available to read.

		// Sanity check: the requested offset and adjusted offsets must match.
		if ao := reader.rr.AdjustedOffset(reader.br); offset != ao {
			log.WithFields(log.Fields{"ao": ao, "offset": offset, "log": readLog}).
				Panic("unexpected AdjustedOffset")
		}

		var op RecordedOp
		var applied bool

		if op, applied, err = playOperation(reader.br, readLog, offset, fsm, dir, files); err != nil {
			err = extendErr(err, "playOperation(%q, %d)", readLog, offset)
			return // playOperation returns only unrecoverable errors.
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
				if err = makeLive(dir, fsm, files); err != nil {
					err = extendErr(err, "makeLive after reading handoff barrier")
				}
				return
			} else {
				// We lost the race to inject our write operation, and must try again.
				state = playerStateInjectHandoffAtHead
			}
		}

		offset = op.LastOffset
	}
}

func preparePlayback(dir string) error {
	// File nodes are staged into a directory within |dir| during playback.
	var fileNodesDir = filepath.Join(dir, fnodeStagingDir)

	// Remove all prior content under |dir|.
	if err := os.RemoveAll(dir); err != nil {
		return err
	} else if err = os.MkdirAll(fileNodesDir, 0777); err != nil {
		return err
	}
	return nil
}

func cleanupOnAbort(dir string, files fnodeFileMap) {
	for _, fnode := range files {
		if err := fnode.Close(); err != nil {
			log.WithField("err", err).Warn("failed to close fnode during abort")
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		log.WithFields(log.Fields{"err": err, "dir": dir}).
			Warn("failed to remove local directory during abort")
	}
}

// decodeOperation unpacks, unmarshals, and sets offsets of a RecordedOp from Reader |br| at |offset|.
func decodeOperation(br *bufio.Reader, readLog pb.Journal, offset int64) (op RecordedOp, frame []byte, err error) {
	if frame, err = message.UnpackFixedFrame(br); err == nil {
		err = op.Unmarshal(frame[message.FixedFrameHeaderLength:])
	}
	// First and last offsets are meta-fields never populated by Recorder, and known only upon playback.
	op.FirstOffset = offset
	op.LastOffset = offset + int64(len(frame))
	op.Log = readLog

	if op.Write != nil {
		op.LastOffset += op.Write.Length
	}
	return
}

// applyOperation attempts to transition |fsm| with operation |op|. It returns
// whether a state transition was applied, and logs unexpected FSM errors.
// Common and expected FSM errors are squelched.
func applyOperation(op RecordedOp, frame []byte, fsm *FSM) bool {
	if err := fsm.Apply(&op, frame[message.FixedFrameHeaderLength:]); err == nil {
		return true
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
		// We expect remaining FSM errors to be rare, but they can happen under
		// normal operation (eg, during hand-off, or if multiple Recorders briefly
		// run concurrently, creating branched log histories).
		log.WithFields(log.Fields{"log": op.Log, "err": err, "op": op.String()}).
			Info("did not apply FSM operation")
	}
	return false
}

// reenactOperation replays local file actions represented by RecordedOp |op|, which has been applied to |fsm|.
func reenactOperation(op RecordedOp, fsm *FSM, br *bufio.Reader, dir string, files fnodeFileMap) error {
	if op.Create != nil {
		return create(dir, Fnode(op.SeqNo), files)
	} else if op.Unlink != nil {
		return unlink(dir, op.Unlink.Fnode, fsm, files)
	} else if op.Write != nil {
		recoveredBytesTotal.Add(float64(op.Write.Length))
		return write(op.Write, br, files)
	}
	// op.Link and op.Property have no local reenactment, beyond application to the FSM.
	return nil
}

// playOperation composes operation decode, application, and re-enactment. It logs warnings on recoverable
// errors, and surfaces only those which should abort playback.
func playOperation(br *bufio.Reader, readLog pb.Journal, offset int64, fsm *FSM,
	dir string, files fnodeFileMap) (op RecordedOp, applied bool, err error) {

	// Unpack the next frame and its unmarshaled RecordedOp.
	var frame []byte
	if op, frame, err = decodeOperation(br, readLog, offset); err != nil {
		if err == message.ErrDesyncDetected {
			// ErrDesyncDetected is returned by UnpackFixedFrame.
			// This is pretty bad. It means a *significant* data corruption occurred,
			// such as a loss of Etcd consistency *plus* an operational recovery error
			// that resulted in a double-write of a journal offset. Or, a bug in
			// recordering, or some other means by which journal data was scribbled.
			//
			// Likely we're not going to be able to apply further operations beyond
			// this point, but we *can* still attempt further reads of the log,
			// and at least recover to a consistent albeit older state.
			log.WithFields(log.Fields{"offset": offset, "log": readLog}).
				Warn("detected de-synchronization")
			err = nil
		} else if err != nil {
			// Other errors are unrecoverable.
			err = extendErr(err, "decodeOperation")
		}
		return
	}

	// Attempt to transition the FSM by the operation, and if it applies,
	// reenact the local filesystem action.
	if applied = applyOperation(op, frame, fsm); applied {
		if err = reenactOperation(op, fsm, br, dir, files); err != nil {
			err = extendErr(err, "reenactOperation(%s)", op.String())
		}
	} else if op.Write != nil {
		// We must discard the indicated length for bytestream consistency.
		if err = copyFixed(ioutil.Discard, br, op.Write.Length); err != nil {
			err = extendErr(err, "copyFixed(%d)", op.Write.Length)
		}
	}
	return
}

func stagedPath(dir string, fnode Fnode) string {
	var fname = strconv.FormatInt(int64(fnode), 10)
	return filepath.Join(filepath.FromSlash(dir), fnodeStagingDir, fname)
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

func write(op *RecordedOp_Write, br *bufio.Reader, files fnodeFileMap) error {
	var file = files[Fnode(op.Fnode)]

	// Seek to the indicated offset.
	if _, err := file.Seek(op.Offset, 0); err != nil {
		return err
	}
	return copyFixed(file, br, op.Length)
}

// copyFixed is like io.CopyN, but minimizes copies by re-using the
// bufio.Reader buffer.
func copyFixed(w io.Writer, br *bufio.Reader, length int64) error {
	for length != 0 {
		// Ask |br| to fill its buffer, if empty.
		if _, err := br.Peek(1); err != nil {
			return extendErr(err, "Peek")
		}

		var n = br.Buffered()
		if int64(n) > length {
			n = int(length)
		}
		b, err := br.Peek(n)
		if err != nil {
			panic(err.Error()) // Cannot fail as n <= br.Buffered().
		}

		n, err = w.Write(b)
		br.Discard(n)
		length -= int64(n)

		if err != nil {
			return err
		}
	}
	return nil
}

// makeLive links staged Fnode |files| into each of their hard link locations
// indicated by |fsm| under |dir|, and creates any property files of |fsm|.
// |files| must exactly match live nodes of |fsm| or makeLive panics.
func makeLive(dir string, fsm *FSM, files fnodeFileMap) error {
	if fsm.hasRemainingHints() {
		panic("fsm.hasRemainingHints")
	}

	log.WithFields(log.Fields{
		"dir":          dir,
		"files":        len(files),
		"nextSeqNo":    fsm.NextSeqNo,
		"nextChecksum": fsm.NextChecksum,
		"lastLog":      fsm.LastLog,
	}).Info("completed playback")

	for fnode, liveNode := range fsm.LiveNodes {
		var file = files[fnode]
		delete(files, fnode)

		// Link backing-file into target paths.
		for link := range liveNode.Links {
			var targetPath = filepath.Join(dir, link)

			if err := os.MkdirAll(filepath.Dir(targetPath), 0777); err != nil {
				return err
			} else if err = os.Link(stagedPath(dir, fnode), targetPath); err != nil {
				return err
			} else if s, err := os.Stat(targetPath); err != nil {
				return err
			} else {
				log.WithFields(log.Fields{
					"fnode":  fnode,
					"target": targetPath,
					"size":   s.Size(),
				}).Info("linked file")
			}
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

func extendErr(err error, mFmt string, args ...interface{}) error {
	if err == nil {
		panic("expected error")
	} else if _, ok := err.(interface{ StackTrace() errors.StackTrace }); ok {
		// Avoid attaching another errors.StackTrace if one is already present.
		return errors.WithMessage(err, fmt.Sprintf(mFmt, args...))
	} else {
		// Use Wrapf to simultaneously attach |mFmt| and the current stack trace.
		return errors.Wrapf(err, mFmt, args...)
	}
}

// Subdirectory into which Fnodes are played-back.
const fnodeStagingDir = ".fnodes"
