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

// Iterator iterates over message Envelopes. It's implemented by ReadUncommittedIter
// and ReadCommittedIter.
type Iterator interface {
	// Next returns the next message Envelopes in the sequence. It returns EOF
	// if none remain, or any other encountered error.
	Next() (Envelope, error)
}

// IteratorFunc adapts a function to an Iterator.
type IteratorFunc func() (Envelope, error)

// Next invokes the IteratorFunc and returns its result.
func (ifn IteratorFunc) Next() (Envelope, error) { return ifn() }

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
	rr        *client.RetryReader
	br        *bufio.Reader
	newMsg    NewMessageFunc
	spec      *pb.JournalSpec
	unmarshal UnmarshalFunc
}

// Next reads and returns the next Envelope or error.
func (it *ReadUncommittedIter) Next() (Envelope, error) {
	if it.spec != nil {
	} else if err := it.init(); err != nil {
		return Envelope{}, err
	}

	for {
		var begin = it.rr.AdjustedOffset(it.br)
		var msg, err = it.newMsg(it.spec)
		if err != nil {
			return Envelope{}, errors.WithMessage(err, "newMessage")
		}

		switch err = it.unmarshal(msg); errors.Cause(err) {
		case nil:
			return Envelope{
				Journal: it.spec,
				Begin:   begin,
				End:     it.rr.AdjustedOffset(it.br),
				Message: msg,
			}, nil

		case io.EOF:
			return Envelope{}, err // Don't wrap io.EOF.

		case io.ErrNoProgress:
			// Swallow ErrNoProgress from our bufio.Reader. client.Reader returns
			// an empty read to allow for inspection of the ReadResponse message,
			// and client.RetryReader also surface these empty reads. A journal
			// with many restarted reads but no active appends can eventually
			// cause our bufio.Reader to give up, though no error has occurred.
			continue

		case client.ErrOffsetJump:
			// ErrOffsetJump indicates the next byte of available content is at an
			// offset larger than the one requested. This can happen if a range of
			// content was deleted from the journal. Log a warning but continue
			// reading at the jumped offset, which will be reflected in the next
			// returned Envelope.
			log.WithFields(log.Fields{
				"journal": it.spec.Name,
				"from":    begin,
				"to":      it.rr.Offset(),
			}).Warn("source journal offset jump")
			continue

		default:
			return Envelope{}, errors.WithMessagef(err, "framing.Unmarshal(offset %d)", begin)
		}
	}
}

func (it *ReadUncommittedIter) init() error {
	var err error
	var framing Framing

	if it.spec, err = client.GetJournal(it.rr.Context, it.rr.Client, it.rr.Journal()); err != nil {
		return errors.WithMessage(err, "fetching journal spec")
	} else if framing, err = FramingByContentType(it.spec.LabelSet.ValueOf(labels.ContentType)); err != nil {
		return errors.WithMessage(err, "determining framing")
	}
	// The returned |it.spec|, as a JournalSpec, has no query component in the
	// journal name. Re-add it (noting this technically invalidates the JournalSpec),
	// so that the caller can disambiguate Envelopes returned from different queried
	// reads of the same Journal.
	it.spec.Name = it.rr.Journal()
	it.unmarshal = framing.NewUnmarshalFunc(it.br)
	return nil
}

// ReadCommittedIter is an Iterator over read-committed messages. It's little
// more than the composition of a provided Sequencer with an underlying
// ReadUncommittedIter.
//
// If a dequeue of the Sequencer returns ErrMustStartReplay, then ReadCommittedIter
// will automatically start the appropriate replay in order to continue its iteration.
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
