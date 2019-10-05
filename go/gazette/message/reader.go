package message

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/labels"
)

// Iterator iterates over message Envelopes.
type Iterator interface {
	// Next returns the next message Envelopes in the sequence. It returns EOF
	// if none remain, or any other encountered error.
	Next() (Envelope, error)
}

// NewReadUncommittedIter returns a ReadUncommittedIter over message Envelopes
// read from the RetryReader. The reader's journal must have an appropriate
// labels.ContentType label, which is used to determine the message Framing.
func NewReadUncommittedIter(rr *client.RetryReader, newMsg NewMessageFunc) *ReadUncommittedIter {
	return &ReadUncommittedIter{
		rr:     rr,
		br:     bufio.NewReader(rr),
		newMsg: newMsg,
	}
}

// ReadUncommittedIter is an Iterator over read-uncommitted messages.
type ReadUncommittedIter struct {
	rr      *client.RetryReader
	br      *bufio.Reader
	newMsg  NewMessageFunc
	spec    *pb.JournalSpec
	framing Framing
}

// Next reads and returns the next Envelope, or an unrecoverable error.
// Several recoverable errors are handled and logged, but not returned:
//
// * A framing Unmarshal() error (as opposed to Unpack). This is an error with
//   respect to a specific framed message in the journal, and not an error
//   of the underlying Journal stream. It can happen if improperly framed
//   or simply malformed data is written to a journal. Typically we want to
//   keep reading in this case as the error already happened (when the frame
//   was written) and we still care about Envelopes occurring afterwards.
//
// * ErrOffsetJump indicates the next byte of available content is at an
//   offset larger than the one requested. This can happen if a range of
//   content was deleted from the journal. Next will log a warning but continue
//   reading at the jumped offset, which is reflected in the returned Envelope.
func (it *ReadUncommittedIter) Next() (Envelope, error) {
	if it.spec != nil {
	} else if err := it.init(); err != nil {
		return Envelope{}, err
	}

	for {
		var begin = it.rr.AdjustedOffset(it.br)
		var frame, err = it.framing.Unpack(it.br)

		switch {
		case err == nil:
			break

		case errors.Cause(err) == io.ErrNoProgress:
			// Swallow ErrNoProgress from our bufio.Reader. client.Reader returns
			// an empty read to allow for inspection of the ReadResponse message,
			// and client.RetryReader also surface these empty reads. A journal
			// with no active appends can eventually cause our bufio.Reader to
			// give up, though no error has occurred.
			continue

		case errors.Cause(err) == client.ErrOffsetJump:
			log.WithFields(log.Fields{
				"journal": it.spec.Name,
				"from":    begin,
				"to":      it.rr.Offset(),
			}).Warn("source journal offset jump")
			continue

		case err == io.EOF: // Don't wrap io.EOF.
			return Envelope{}, err

		default:
			return Envelope{}, errors.WithMessagef(err, "framing.Unpack(offset %d)", begin)
		}

		var msg Message
		if msg, err = it.newMsg(it.spec); err != nil {
			return Envelope{}, errors.WithMessage(err, "newMessage")
		}
		if err = it.framing.Unmarshal(frame, msg); err != nil {
			log.WithFields(log.Fields{
				"journal": it.rr.Journal(),
				"begin":   begin,
				"end":     it.rr.AdjustedOffset(it.br),
				"err":     err,
			}).Error("failed to unmarshal message")
			continue
		}

		return Envelope{
			Journal: it.spec,
			Begin:   begin,
			End:     it.rr.AdjustedOffset(it.br),
			Message: msg,
		}, nil
	}
}

func (it *ReadUncommittedIter) init() error {
	var err error
	if it.spec, err = client.GetJournal(it.rr.Context, it.rr.Client, it.rr.Journal()); err != nil {
		return errors.WithMessage(err,  "fetching journal spec")
	} else if it.framing, err = FramingByContentType(it.spec.LabelSet.ValueOf(labels.ContentType)); err != nil {
		return errors.WithMessage(err, "determining framing")
	}
	return nil
}

// ReadCommittedIter is an Iterator over read-committed messages.
type ReadCommittedIter struct {
	rui ReadUncommittedIter
	seq *Sequencer
}

// NewReadCommittedIter returns a ReadCommittedIter over message Envelopes read
// from the RetryReader. The provided Sequencer is used to sequence committed
// messages. The reader's journal must have an appropriate labels.ContentType
// label, which is used to determine the message Framing.
func NewReadCommittedIter(rr *client.RetryReader, newMsg NewMessageFunc, seq *Sequencer) *ReadCommittedIter {
	return &ReadCommittedIter{rui: *NewReadUncommittedIter(rr, newMsg), seq: seq}
}

// Next returns the next read-committed message Envelope in the sequence.
// It returns EOF if none remain, or any other encountered error.
func (it *ReadCommittedIter) Next() (Envelope, error) {
	for {
		switch env, err := it.seq.DequeCommitted(); err {
		case nil:
			return env, nil

		case ErrMustStartReplay:
			var req = it.rui.rr.Reader.Request
			req.Offset, req.EndOffset = it.seq.ReplayRange()

			var rr = client.NewRetryReader(it.rui.rr.Context, it.rui.rr.Client, req)
			it.seq.StartReplay(NewReadUncommittedIter(rr, it.rui.newMsg))

		case io.EOF:
			switch env, err = it.rui.Next(); err {
			case nil:
				it.seq.QueueUncommitted(env)
			case io.EOF:
				return Envelope{}, io.EOF // Don't wrap io.EOF.
			default:
				return Envelope{}, errors.WithMessage(err, "reading next uncommitted message")
			}

		default:
			return Envelope{}, errors.WithMessage(err, "deque of committed message")
		}
	}
}
