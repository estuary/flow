package gazette

import (
	log "github.com/Sirupsen/logrus"
)

const ReplicateOpBufferSize = 10

// Accepts completed spool fragments for persisting.
type fragmentPersister interface {
	Persist(Fragment)
}

type Head struct {
	journal   string
	directory string

	replicateOps chan ReplicateOp
	committed    chan struct{}

	// Journal offset at which the next write will occur.
	writeHead int64
	// In-progress spool.
	spool     *Spool
	persister fragmentPersister
	// Notification channel on which committed fragments are sent.
	updates chan<- Fragment

	stop chan struct{}
}

func NewHead(journal, directory string, persister fragmentPersister,
	updates chan<- Fragment) *Head {

	h := &Head{
		journal:      journal,
		directory:    directory,
		replicateOps: make(chan ReplicateOp, ReplicateOpBufferSize),
		committed:    make(chan struct{}),
		persister:    persister,
		updates:      updates,
		stop:         make(chan struct{}),
	}
	return h
}

func (h *Head) StartServingOps(writeHead int64) *Head {
	h.writeHead = writeHead
	go h.loop()
	return h
}

func (h *Head) Replicate(op ReplicateOp) {
	h.replicateOps <- op
}

func (h *Head) Stop() {
	close(h.replicateOps)
	<-h.stop // Blocks until loop() exits.
}

func (h *Head) loop() {
	for {
		op, ok := <-h.replicateOps
		if !ok {
			break
		}
		result := h.onWrite(op)
		op.Result <- result

		if result.Error == nil {
			// Block until transaction completes (Close() is called).
			<-h.committed
		}
	}
	if h.spool != nil {
		h.persister.Persist(h.spool.Fragment)
	}
	log.WithField("journal", h.journal).Info("head loop exiting")
	close(h.stop)
}

func (h *Head) onWrite(write ReplicateOp) ReplicateResult {
	if write.Journal != h.journal {
		return ReplicateResult{Error: ErrWrongJournal}
	}
	// Fail if the operation uses a write head behind ours.
	// Skip forward if it uses a future one.
	if write.WriteHead < h.writeHead {
		return ReplicateResult{
			Error:          ErrWrongWriteHead,
			ErrorWriteHead: h.writeHead,
		}
	} else if write.WriteHead > h.writeHead {
		h.writeHead = write.WriteHead
	}
	// Evaluate conditions under which we'll roll a new spool.
	if h.spool == nil ||
		h.spool.err != nil ||
		h.spool.End != h.writeHead ||
		(write.NewSpool && h.spool.Size() != 0) {

		if h.spool != nil {
			if h.spool.End != h.writeHead {
				log.WithFields(log.Fields{"end": h.spool.End, "head": h.writeHead,
					"journal": h.journal}).
					Warn("rolling spool because of write-head increase")
			}
			h.persister.Persist(h.spool.Fragment)
		}

		spool, err := NewSpool(h.directory, h.journal, h.writeHead)
		if err != nil {
			return ReplicateResult{Error: err}
		}
		h.spool = spool
	}
	return ReplicateResult{Writer: headTransaction{h}}
}

// Implements the WriteCommitter interface.
type headTransaction struct{ *Head }

func (t headTransaction) Write(buf []byte) (n int, err error) {
	return t.spool.Write(buf)
}

func (t headTransaction) Commit(delta int64) error {
	err := t.spool.Commit(delta)
	t.writeHead = t.spool.End
	t.updates <- t.spool.Fragment
	t.committed <- struct{}{}
	return err
}
