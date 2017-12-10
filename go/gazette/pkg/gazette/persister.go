package gazette

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	etcd "github.com/coreos/etcd/client"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/async"
	"github.com/LiveRamp/gazette/pkg/cloudstore"
	"github.com/LiveRamp/gazette/pkg/journal"
)

const (
	PersisterLocksPrefix = "persister_locks/"
	PersisterLocksRoot   = ServiceRoot + "/" + PersisterLocksPrefix

	kPersisterConvergeInterval = time.Minute
	kPersisterLockTTL          = 10 * time.Minute
)

type Persister struct {
	directory string
	cfs       cloudstore.FileSystem
	keysAPI   etcd.KeysAPI
	routeKey  string

	queue        map[string]journal.Fragment
	shuttingDown uint32
	loopExited   chan struct{}
	mu           sync.Mutex

	// Effective constants, which are swappable for testing.
	osRemove         func(path string) error
	persisterLockTTL time.Duration
}

func NewPersister(directory string, cfs cloudstore.FileSystem,
	keysAPI etcd.KeysAPI, routeKey string) *Persister {
	p := &Persister{
		cfs:              cfs,
		directory:        directory,
		keysAPI:          keysAPI,
		osRemove:         os.Remove,
		persisterLockTTL: kPersisterLockTTL,
		queue:            make(map[string]journal.Fragment),
		loopExited:       make(chan struct{}),
		routeKey:         routeKey,
	}
	// Make the state of the persister queue available to expvar.
	gazetteMap.Set("persister", p)

	return p
}

// Note: This String() implementation is primarily for the benefit of expvar,
// which expects the string to be a serialized JSON object.
func (p *Persister) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Build a view of the queue that shows the list of offsets being uploaded
	// for a given journal, which is sort of the inverse of how we store it in
	// |p.queue|
	ret := make(map[journal.Name][]string)
	for offsetRange, entry := range p.queue {
		ret[entry.Journal] = append(ret[entry.Journal], offsetRange)
	}

	if msg, err := json.Marshal(ret); err != nil {
		return fmt.Sprintf("%q", err.Error())
	} else {
		return string(msg)
	}
}

func (p *Persister) IsShuttingDown() bool {
	return atomic.LoadUint32(&p.shuttingDown) == 1
}

func (p *Persister) Stop() {
	atomic.StoreUint32(&p.shuttingDown, 1)
	<-p.loopExited
}

func (p *Persister) StartPersisting() *Persister {
	go func() {
		interval := time.Tick(kPersisterConvergeInterval)
		for {
			<-interval

			// Attempt to converge all items in the queue.
			p.converge()

			// If the queue has become empty and we are shutting down, bail.
			p.mu.Lock()
			if p.IsShuttingDown() && len(p.queue) == 0 {
				p.mu.Unlock()
				break
			}
			p.mu.Unlock()
		}
		close(p.loopExited)
	}()
	return p
}

func (p *Persister) Persist(fragment journal.Fragment) {
	// If we are shutting down, warn loudly on new Persist() requests -- we
	// handle them to a degree, but it shouldn't happen.
	if p.IsShuttingDown() {
		log.WithField("fragment", fragment).Warn("Persist() called during shutdown")
	}
	// If the fragment is empty, immediately delete it.
	if fragment.Size() == 0 {
		p.removeLocal(fragment)
		return
	}
	p.mu.Lock()
	p.queue[fragment.ContentName()] = fragment
	p.mu.Unlock()
}

func (p *Persister) converge() {
	p.mu.Lock()
	for name, fragment := range p.queue {
		p.mu.Unlock()
		success := p.convergeOne(fragment)
		p.mu.Lock()

		if success {
			delete(p.queue, name)
		}
	}
	p.mu.Unlock()
}

func (p *Persister) convergeOne(fragment journal.Fragment) bool {
	var lockPath = PersisterLocksRoot + fragment.ContentName()
	var lockIndex uint64

	// Attempt to lock this fragment for upload.
	if response, err := p.keysAPI.Set(context.Background(), lockPath, p.routeKey,
		&etcd.SetOptions{
			PrevExist: etcd.PrevNoExist,
			TTL:       p.persisterLockTTL,
		},
	); err != nil {
		// Log if this is not an Etcd "Key already exists" error.
		if etcdErr, _ := err.(etcd.Error); etcdErr.Code != etcd.ErrorCodeNodeExist {
			log.WithFields(log.Fields{"err": err, "path": fragment.ContentName}).
				Warn("failed to lock fragment for persisting")
		}
		return false
	} else {
		lockIndex = response.Index
	}

	// Arrange to remove the lock on exit.
	defer func() {
		if _, err := p.keysAPI.Delete(context.Background(), lockPath,
			&etcd.DeleteOptions{PrevIndex: lockIndex}); err != nil {
			log.WithFields(log.Fields{"err": err, "path": fragment.ContentName}).
				Error("failed to delete fragment persister lock")
		}
	}()

	done := make(async.Promise)

	// Perform the actual transfer in a goroutine which resolves |done|. Capture
	// whether the transfer completed successfully.
	var success bool
	go func(success *bool) {
		defer done.Resolve()
		*success = transferFragmentToGCS(p.cfs, fragment)
	}(&success)

	// Wait for |done|, periodically refreshing the held lock.
	done.WaitWithPeriodicTask(p.persisterLockTTL/2, func() {
		if resp, err := p.keysAPI.Set(context.Background(), lockPath, p.routeKey,
			&etcd.SetOptions{
				PrevExist: etcd.PrevExist,
				PrevIndex: lockIndex,
				TTL:       p.persisterLockTTL,
			}); err != nil {
			log.WithFields(log.Fields{"err": err, "path": fragment.ContentName}).
				Error("failed to update held fragment lock for persisting")
		} else {
			lockIndex = resp.Index
		}
	})

	if success {
		p.removeLocal(fragment)
	}
	return success
}

func transferFragmentToGCS(cfs cloudstore.FileSystem, fragment journal.Fragment) bool {
	// Create the journal's fragment directory, if not already present.
	if err := cfs.MkdirAll(fragment.Journal.String(), 0750); err != nil {
		log.WithFields(log.Fields{"err": err, "path": fragment.Journal}).
			Warn("failed to make fragment directory")
		return false
	}

	var w, err = cfs.OpenFile(fragment.ContentPath(),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)

	if os.IsExist(err) {
		// Already present on target file system. No need to re-upload.
		return true
	} else if err != nil {
		log.WithFields(log.Fields{"err": err, "path": fragment.ContentPath()}).
			Warn("failed to open fragment for writing")
		return false
	}
	var r = io.NewSectionReader(fragment.File, 0, fragment.End-fragment.Begin)

	if _, err := cfs.CopyAtomic(w, r); err != nil {
		log.WithFields(log.Fields{"err": err, "path": fragment.ContentPath()}).
			Warn("failed to copy fragment")
		return false
	} else {
		return true
	}
}

func (p *Persister) removeLocal(fragment journal.Fragment) {
	localPath := filepath.Join(p.directory, fragment.ContentPath())

	if rmErr := p.osRemove(localPath); rmErr != nil {
		log.WithFields(log.Fields{"err": rmErr, "path": localPath}).
			Error("failed to remove persisted spool")
	}
}
