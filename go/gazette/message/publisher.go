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
// and is updated with time.Now() on each message published. If a non-nil *Clock
// is provided, it should be updated by the caller at a convenient time
// resolution, which can greatly reduce the frequency of time() system calls.
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

// PublishCommitted sequences the Message for immediate consumption, maps it
// to a Journal, and begins an AsyncAppend of its marshaled content.
// It is safe for concurrent use.
func (p *Publisher) PublishCommitted(mapping MappingFunc, msg Message) (*client.AsyncAppend, error) {
	var _, _, aa, err = p.publish(mapping, msg, Flag_OUTSIDE_TXN)
	return aa, err
}

// PublishUncommitted sequences the Message as uncommitted, maps it to a Journal,
// and begins an AsyncAppend of its marshaled content. The Journal is tracked
// and included in the results of a future []AckIntents.
// PublishUncommitted is *not* safe for concurrent use.
func (p *Publisher) PublishUncommitted(mapping MappingFunc, msg Message) error {
	var journal, framing, _, err = p.publish(mapping, msg, Flag_CONTINUE_TXN)
	if err != nil {
		return err
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
	return nil
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

// AckIntent is framed Intent and Journal which, when written, will acknowledge
// a set of pending Messages previously written via PublishUncommitted.
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
	msg.SetUUID(BuildUUID(p.producer, p.clock.Tick(), flags))

	if v, ok := msg.(Validator); ok {
		if err = v.Validate(); err != nil {
			return
		}
	}
	if journal, framing, err = mapping(msg); err != nil {
		return
	}
	aa = p.ajc.StartAppend(pb.AppendRequest{Journal: journal}, nil)
	aa.Require(framing.Marshal(msg, aa.Writer()))
	err = aa.Release()
	return
}
