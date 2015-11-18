package journal

import (
	"time"

	log "github.com/Sirupsen/logrus"
)

const ReadOpBufferSize = 10

type Tail struct {
	journal   Name
	fragments FragmentSet

	readOps   chan ReadOp
	updates   <-chan Fragment
	endOffset chan int64

	// Reads which can't (yet) be satisfied by a fragment in |fragments|.
	blockedReads []ReadOp
	deadline     struct {
		next  time.Time
		timer *time.Timer
	}

	stop chan struct{}
}

func NewTail(journal Name, updates <-chan Fragment) *Tail {
	t := &Tail{
		journal:   journal,
		updates:   updates,
		readOps:   make(chan ReadOp, ReadOpBufferSize),
		endOffset: make(chan int64),
		stop:      make(chan struct{}),
	}
	t.deadline.timer = time.NewTimer(0)
	return t
}

func (t *Tail) StartServingOps() *Tail {
	go t.loop()
	return t
}

func (t *Tail) Read(op ReadOp) {
	t.readOps <- op
}

func (t *Tail) Stop() {
	close(t.readOps)
	<-t.stop // Blocks until loop() exits.
}

func (t *Tail) EndOffset() int64 {
	return <-t.endOffset
}

func (t *Tail) loop() {
	for t.updates != nil || t.readOps != nil {
		// Consume available fragment updates prior to serving reads.
		select {
		case fragment, ok := <-t.updates:
			if ok {
				t.onUpdate(fragment)
				continue
			}
		default:
		}

		// Wait for a read or fragment update to arrive, or for a request
		// for the current tail end.
		select {
		case fragment, ok := <-t.updates:
			if !ok {
				t.updates = nil
				// Any remaining blocked reads will now fail.
				t.wakeBlockedReads(time.Time{})
			} else {
				t.onUpdate(fragment)
			}
		case read, ok := <-t.readOps:
			if !ok {
				t.readOps = nil
			} else {
				t.onRead(read)
			}
		case done := <-t.deadline.timer.C:
			// A zero value t.deadline.next indicates the timer is not in use
			t.deadline.next = time.Time{}
			t.wakeBlockedReads(done)
		case t.endOffset <- t.fragments.EndOffset():
		}
	}
	close(t.endOffset) // After close(), EndOffset() will thereafter return 0.
	log.WithField("journal", t.journal).Info("tail loop exiting")
	close(t.stop)
}

func (t *Tail) onUpdate(fragment Fragment) {
	if fragment.Journal != t.journal {
		log.WithFields(log.Fields{"fragment.Journal": fragment.Journal,
			"tail.journal": t.journal}).Error("unexpected fragment journal")
		return
	}
	t.fragments.Add(fragment)
	t.wakeBlockedReads(time.Time{})
}

func (t *Tail) onRead(op ReadOp) {
	if op.Journal != t.journal {
		op.Result <- ReadResult{Error: ErrWrongJournal}
		return
	}
	// Special handling for offsets 0 and -1.
	if op.Offset == 0 {
		// Skip |Offset| forward to the first available offset.
		op.Offset = t.fragments.BeginOffset()
	} else if op.Offset == -1 {
		// Set |Offset| to the current tail end.
		op.Offset = t.fragments.EndOffset()
	}

	ind := t.fragments.LongestOverlappingFragment(op.Offset)
	if ind == len(t.fragments) || t.fragments[ind].Begin > op.Offset {
		// A fragment covering op.Offset isn't available (yet).
		if t.updates != nil && op.Blocking {
			// If a deadline is specified, manage the timer appropriately
			if !op.Deadline.IsZero() {
				if t.deadline.next.IsZero() || t.deadline.next.After(op.Deadline) {
					t.deadline.next = op.Deadline
					t.deadline.timer.Reset(op.Deadline.Sub(time.Now()))
				}
			}
			t.blockedReads = append(t.blockedReads, op)
		} else {
			op.Result <- ReadResult{
				Error:     ErrNotYetAvailable,
				Offset:    op.Offset,
				WriteHead: t.fragments.EndOffset(),
			}
		}
	} else {
		// A covering fragment was found.
		op.Result <- ReadResult{
			Offset:    op.Offset,
			WriteHead: t.fragments.EndOffset(),
			Fragment:  t.fragments[ind],
		}
	}
}

func (t *Tail) wakeBlockedReads(when time.Time) {
	woken := t.blockedReads
	t.blockedReads = nil

	for _, op := range woken {
		// If the deadline for a particular read has passed, it should no longer
		// be considered blocking.
		if op.Deadline.Before(when) && op.Deadline.After(time.Time{}) {
			op.Blocking = false
			op.Deadline = time.Time{}
		}
		t.onRead(op)
	}
}
