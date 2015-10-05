package gazette

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/go-etcd/etcd"

	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/api-server/discovery"
	"github.com/pippio/gazette/async"
	"github.com/pippio/gazette/journal"
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
	etcd      discovery.EtcdService
	routeKey  string

	queue map[string]journal.Fragment
	mu    sync.Mutex

	// Effective constants, which are swappable for testing.
	osRemove         func(path string) error
	persisterLockTTL time.Duration
}

func NewPersister(directory string, cfs cloudstore.FileSystem,
	etcd discovery.EtcdService, routeKey string) *Persister {
	p := &Persister{
		cfs:              cfs,
		directory:        directory,
		etcd:             etcd,
		osRemove:         os.Remove,
		persisterLockTTL: kPersisterLockTTL,
		queue:            make(map[string]journal.Fragment),
		routeKey:         routeKey,
	}
	return p
}

func (p *Persister) StartPersisting() *Persister {
	go func() {
		interval := time.Tick(kPersisterConvergeInterval)
		for {
			<-interval
			p.converge()
		}
	}()
	return p
}

func (p *Persister) Persist(fragment journal.Fragment) {
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
	lockPath := PersisterLocksRoot + fragment.ContentName()

	// Attempt to lock this fragment for upload.
	if err := p.etcd.Create(lockPath, p.routeKey, p.persisterLockTTL); err != nil {
		// Log if this is not an Etcd "Key already exists" error.
		if etcdErr, ok := err.(*etcd.EtcdError); !ok || etcdErr.ErrorCode != 105 {
			log.WithFields(log.Fields{"err": err, "path": fragment.ContentName}).
				Warn("failed to lock fragment for persisting")
		}
		return false
	}
	// Arrange to remove the lock on exit.
	defer func() {
		if err := p.etcd.Delete(lockPath, false); err != nil {
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

		// Create the journal's fragment directory, if not already present.
		if err := p.cfs.MkdirAll(fragment.Journal.String(), 0750); err != nil {
			log.WithFields(log.Fields{"err": err, "path": fragment.Journal}).
				Error("failed to make fragment directory")
			return
		}

		w, err := p.cfs.OpenFile(fragment.ContentPath(),
			os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)

		if os.IsExist(err) {
			// Already present on target file system. No need to re-upload.
			*success = true
			return
		} else if err != nil {
			log.WithFields(log.Fields{"err": err, "path": fragment.ContentPath()}).
				Error("failed to open fragment for writing")
			return
		}
		r := journal.NewBoundedReaderAt(fragment.File, fragment.End-fragment.Begin, 0)

		if _, err := p.cfs.CopyAtomic(w, r); err != nil {
			log.WithFields(log.Fields{"err": err, "path": fragment.ContentPath()}).
				Error("failed to copy fragment")
		} else {
			*success = true
		}
	}(&success)

	// Wait for |done|, periodically refreshing the held lock.
	done.WaitWithPeriodicTask(p.persisterLockTTL/2, func() {
		if err := p.etcd.Update(lockPath, p.routeKey, p.persisterLockTTL); err != nil {
			log.WithFields(log.Fields{"err": err, "path": fragment.ContentName}).
				Error("failed to update held fragment lock for persisting")
		}
	})

	if success {
		p.removeLocal(fragment)
	}
	return success
}

func (p *Persister) removeLocal(fragment journal.Fragment) {
	localPath := filepath.Join(p.directory, fragment.ContentPath())

	if rmErr := p.osRemove(localPath); rmErr != nil {
		log.WithFields(log.Fields{"err": rmErr, "path": localPath}).
			Error("failed to remove persisted spool")
	}
}
