package gazette

import (
	log "github.com/Sirupsen/logrus"
)

const ReadOpBufferSize = 10

type Tail struct {
	journal   string
	fragments FragmentSet

	readOps   chan ReadOp
	updates   <-chan Fragment
	endOffset chan int64

	// Reads which can't (yet) be satisfied by a fragment in |fragments|.
	blockedReads []ReadOp

	stop chan struct{}
}

func NewTail(journal string, updates <-chan Fragment) *Tail {
	t := &Tail{
		journal:   journal,
		updates:   updates,
		readOps:   make(chan ReadOp, ReadOpBufferSize),
		endOffset: make(chan int64),
		stop:      make(chan struct{}),
	}
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

		select {
		case fragment, ok := <-t.updates:
			if !ok {
				t.updates = nil
				t.wakeBlockedReads()
			} else {
				t.onUpdate(fragment)
			}
		case read, ok := <-t.readOps:
			if !ok {
				t.readOps = nil
			} else {
				t.onRead(read)
			}
		case t.endOffset <- t.fragments.EndOffset():
		}
	}
	close(t.endOffset) // EndOffset() immediately returns 0.
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
	t.wakeBlockedReads()
}

func (t *Tail) onRead(read ReadOp) {
	if read.Journal != t.journal {
		read.Result <- ReadResult{Error: ErrWrongJournal}
		return
	}
	ind := t.fragments.LongestOverlappingFragment(read.Offset)
	if ind == len(t.fragments) || t.fragments[ind].Begin > read.Offset {
		// A fragment covering |read.Offset| isn't available (yet).

		if t.updates != nil && read.Blocking {
			t.blockedReads = append(t.blockedReads, read)
		} else {
			read.Result <- ReadResult{Error: ErrNotYetAvailable}
		}
		return
	}
	read.Result <- ReadResult{Fragment: t.fragments[ind]}
}

func (t *Tail) wakeBlockedReads() {
	woken := t.blockedReads
	t.blockedReads = nil

	for _, read := range woken {
		t.onRead(read)
	}
}
