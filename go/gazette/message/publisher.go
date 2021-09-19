package message

import (
	"bufio"
	"bytes"
	"time"

	"github.com/pkg/errors"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
)

// Publisher maps, sequences, and asynchronously appends messages to Journals.
// It supports two modes of publishing: PublishCommitted and PublishUncommitted.
// Committed messages are immediately read-able by a read-committed reader.
// Uncommitted messages are immediately read-able by a read-uncommitted reader,
// but not by a read-committed reader until a future "acknowledgement" (ACK)
// message marks them as committed -- an ACK which may not ever come.
//
// To publish as a transaction, the client first issues a number of
// PublishUncommitted calls. Once all pending messages have been published,
// BuildAckIntents returns []AckIntents which will inform readers that published
// messages have committed and should be processed. To ensure atomicity of the
// published transaction, []AckIntents must be written to stable storage *before*
// being applied, and must be re-tried on fault.
//
// As a rule of thumb, API servers or other pure "producers" of events in Gazette
// should use PublishCommitted. Gazette consumers should use PublishUncommitted
// to achieve end-to-end exactly once semantics: upon commit, each consumer
// transaction will automatically acknowledge all such messages published over
// the course of the transaction.
//
// Consumers *may* instead use PublishCommitted, which may improve latency
// slightly (as read-committed readers need not wait for the consumer transaction
// to commit), but must be aware that its use weakens the effective processing
// guarantee to at-least-once.
type Publisher struct {
	ajc        client.AsyncJournalClient
	clock      *Clock
	autoUpdate bool
	producer   ProducerID
	intents    []AckIntent
	intentIdx  map[pb.Journal]int
}

// NewPublisher returns a new Publisher using the given AsyncJournalClient
// and optional *Clock. If *Clock is nil, then an internal Clock is allocated
// and is updated with time.Now on each message published. If a non-nil *Clock
// is provided, it should be updated by the caller at a convenient time
// resolution, which can greatly reduce the frequency of time system calls.
func NewPublisher(ajc client.AsyncJournalClient, clock *Clock) *Publisher {
	var autoUpdate bool
	if clock == nil {
		clock, autoUpdate = new(Clock), true
	}

	var p = &Publisher{
		ajc:        ajc,
		clock:      clock,
		autoUpdate: autoUpdate,
		producer:   NewProducerID(),
		intentIdx:  make(map[pb.Journal]int),
	}
	return p
}

// ProducerID returns the ProducerID of this Publisher.
func (p *Publisher) ProducerID() ProducerID { return p.producer }

// PublishCommitted maps the Message to a Journal and begins an AsyncAppend of
// its marshaled content, with a UUID sequenced for immediate consumption.
// An error is returned if:
//
//  * The Message implements Validator, and it returns an error.
//  * The MappingFunc returns an error while mapping the Message to a journal.
//  * The journal's Framing returns an error while marshaling the Message,
//    or an os.PathError occurs while spooling the frame to a temporary file
//    (eg, because local disk is full).
//
// A particular MappingFunc error to be aware of is ErrEmptyListResponse,
// returned by mapping routines of this package when there are no journals
// that currently match the mapping's selector. The caller may wish to retry at
// a later time in the case of ErrEmptyListResponse or os.PathError.
//
// Note that the message UUID will not yet be set when Validator or MappingFunc
// is invoked. This is because generation of UUIDs must be synchronized
// over the journal to which the Message is written to preserve ordering, and this
// cannot be known until mapping has been done.
//
// If desired, the caller may select on Done of the returned *AsyncAppend to be
// notified as soon as this particular Message has committed to the journal.
// This might be appropriate when publishing as part of an HTTP request, where
// status is to reported to the client.
//
// Callers are also free to altogether ignore the returned *AsyncAppend, perhaps
// within a non-blocking "fire and forget" of collected logs or metrics.
//
// Another option is to issue a periodic "write barrier", where the caller uses
// PendingExcept of the underlying AsyncJournalClient and waits over the returned
// OpFutures. At that time the caller is assured that all prior publishes have
// committed, without having to track or wait for them individually.
//
// PublishCommitted is safe for concurrent use.
func (p *Publisher) PublishCommitted(mapping MappingFunc, msg Message) (*client.AsyncAppend, error) {
	var _, _, aa, err = p.publish(mapping, msg, Flag_OUTSIDE_TXN)
	return aa, err
}

// PublishUncommitted is like PublishCommitted but sequences the Message
// as part of an open transaction. The Message must later be acknowledged before it
// will be visible to read-committed readers. The Journal is tracked and included
// in the results of the next BuildAckIntents.
// PublishUncommitted is *not* safe for concurrent use.
func (p *Publisher) PublishUncommitted(mapping MappingFunc, msg Message) (*client.AsyncAppend, error) {
	var journal, framing, aa, err = p.publish(mapping, msg, Flag_CONTINUE_TXN)
	if err != nil {
		return aa, err
	}
	// Is this the first publish to this journal since our last commit?
	if _, ok := p.intentIdx[journal]; !ok {
		p.intentIdx[journal] = len(p.intents)
		p.intents = append(p.intents, AckIntent{
			Journal: journal,
			msg:     msg.NewAcknowledgement(journal),
			framing: framing,
		})
	}
	return aa, nil
}

// BuildAckIntents returns the []AckIntents which acknowledge all pending
// Messages published since its last invocation. It's the caller's job to
// actually append the intents to their respective journals, and only *after*
// checkpoint-ing the intents to a stable store so that they may be re-played
// in their entirety should a fault occur. Without doing this, in the presence
// of faults it's impossible to ensure that ACKs are written to _all_ journals,
// and not just some of them (or none).
//
// Applications running as Gazette consumers *must not* call BuildAckIntents
// themselves. This is done on the application's behalf, as part of building
// the checkpoints which are committed with consumer transactions.
//
// Uses of PublishUncommitted outside of consumer applications, however, *are*
// responsible for building, committing, and writing []AckIntents themselves.
func (p *Publisher) BuildAckIntents() ([]AckIntent, error) {
	var b bytes.Buffer
	var bw = bufio.NewWriter(&b)
	var out []AckIntent

	for _, i := range p.intents {
		i.msg.SetUUID(BuildUUID(p.producer, p.clock.Tick(), Flag_ACK_TXN))

		var n = b.Len()
		if err := i.framing.Marshal(i.msg, bw); err != nil {
			return nil, errors.WithMessagef(err, "marshaling message %+v", i.msg)
		}
		_ = bw.Flush()

		out = append(out, AckIntent{
			Journal: i.Journal,
			// It's not valid to reference into Bytes while writing into the
			// Buffer, but right now we just need to know the size.
			Intent: b.Bytes()[n:b.Len()],
		})
		delete(p.intentIdx, i.Journal)
	}
	p.intents = p.intents[:0]

	// We're done writing into |b|; now fix-up []bytes to
	// reference their proper slices in the final buffer.
	var n int
	for i := range out {
		out[i].Intent = b.Bytes()[n : n+len(out[i].Intent)]
		n += len(out[i].Intent)
	}
	return out, nil
}

// AckIntent is framed "intent" message and its journal which, when appended to
// the journal, will acknowledge a set of pending messages previously written
// to that journal via PublishUncommitted.
type AckIntent struct {
	Journal pb.Journal // Journal to be acknowledged.
	Intent  []byte     // Framed Message payload.

	msg     Message // Zero-valued instance of the correct type for the journal.
	framing Framing // Framing of the journal.
}

func (p *Publisher) publish(mapping MappingFunc, msg Message, flags Flags) (journal pb.Journal, framing Framing, aa *client.AsyncAppend, err error) {
	if p.autoUpdate {
		p.clock.Update(time.Now())
	}

	if v, ok := msg.(Validator); ok {
		if err = v.Validate(); err != nil {
			return
		}
	}

	journal, ct, err := mapping(msg)
	if err != nil {
		return
	} else if framing, err = FramingByContentType(ct); err != nil {
		return
	}

	aa = p.ajc.StartAppend(pb.AppendRequest{Journal: journal}, nil)
	// StartAppend strictly orders all writes to this |journal| done through
	// this journal client (and Publisher).
	//
	// It's important that we build the UUID *after* entering the StartAppend
	// block, as concurrent PublishCommitted calls could otherwise race such
	// that a UUID with an earlier Clock is written after one with a later one.
	msg.SetUUID(BuildUUID(p.producer, p.clock.Tick(), flags))
	aa.Require(framing.Marshal(msg, aa.Writer()))
	err = aa.Release()
	return
}
