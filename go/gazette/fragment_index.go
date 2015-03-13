package gazette

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/services/storage-client"
	"google.golang.org/cloud/storage"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const kSpoolRollSize = 1 << 20

type FragmentIndex struct {
	LocalDirectory string
	Journal        string
	StorageContext *storageClient.GCSContext

	mu           sync.Mutex
	fragments    FragmentSet
	currentSpool *Spool

	// Rendezvous point for stalled reads which are waiting
	// for the committed log head to move.
	commitCond sync.Cond
}

func NewFragmentIndex(localDirectory, journal string,
	context *storageClient.GCSContext) *FragmentIndex {

	index := &FragmentIndex{
		LocalDirectory: localDirectory,
		Journal:        journal,
		StorageContext: context,
	}
	index.commitCond.L = &index.mu
	return index
}

func (i *FragmentIndex) ServerOpen() error {
	if err := os.MkdirAll(
		filepath.Join(i.LocalDirectory, i.Journal), 0700); err != nil {
		return err
	}
	if _, err := i.LoadFromContext(); err != nil {
		return err
	}
	i.RecoverLocalSpools()
	return nil
}

func (i *FragmentIndex) WriteOffset() int64 {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.fragments.EndOffset()
}

func (i *FragmentIndex) AddFragment(fragment Fragment) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.fragments.Add(fragment)
}

func (i *FragmentIndex) FinishCurrentSpool() {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.finishCurrentSpool()
}
func (i *FragmentIndex) finishCurrentSpool() {
	if i.currentSpool != nil {
		go i.persist(i.currentSpool)
		i.currentSpool = nil
	}
}

func (i *FragmentIndex) RouteRead(offset int64) (Fragment, *Spool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Is this read at the current log head?
	for i.fragments.EndOffset() == offset /* && !i.fragments.Shutdown */ {
		i.commitCond.Wait() // Wait for the committed head to move.
	}

	if offset > i.fragments.EndOffset() {
		return Fragment{}, nil
	}

	// Can the current spool satisfy the read?
	if i.currentSpool != nil &&
		i.currentSpool.Begin <= offset &&
		i.currentSpool.LastCommit >= offset {
		return i.currentSpool.Fragment(), i.currentSpool
	}

	ind := i.fragments.LongestOverlappingFragment(offset)
	return i.fragments[ind], nil
}

func (i *FragmentIndex) InvokeWithSpool(invoke func(*Spool)) {
	i.mu.Lock()
	defer i.mu.Unlock()

	writeOffset := i.fragments.EndOffset()

	if i.currentSpool != nil && (i.currentSpool.Error != nil ||
		i.currentSpool.LastCommit != writeOffset ||
		i.currentSpool.CommittedSize() > kSpoolRollSize) {
		i.finishCurrentSpool()
	}
	if i.currentSpool == nil {
		i.currentSpool = NewSpool(i.LocalDirectory, i.Journal, writeOffset)
		log.Info("created new spool ", i.currentSpool.ContentPath())
	}
	invoke(i.currentSpool)
	i.fragments.Add(i.currentSpool.Fragment())

	i.commitCond.Broadcast() // Wake waiting readers.
}

func (i *FragmentIndex) RecoverLocalSpools() {
	spools := RecoverSpools(i.LocalDirectory)

	for _, spool := range spools {
		log.WithField("path", spool.LocalPath()).Warning("recovering spool")
		i.AddFragment(spool.Fragment())
		go i.persist(spool)
	}
}

func (i *FragmentIndex) persist(spool *Spool) {
	for {
		if err := spool.Persist(i.StorageContext); err != nil {
			log.WithFields(log.Fields{"err": err, "path": spool.LocalPath()}).
				Error("failed to persist")

			time.Sleep(time.Minute) // Retry.
		} else {
			break
		}
	}
}

func (i *FragmentIndex) LoadFromContext() (cursor interface{}, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.fragments = i.fragments[:0] // Truncate entries.

	// Perform iterative incremental loads until no new fragments are available.
	for done := false; !done && err == nil; {
		done, cursor, err = i.incrementalRefresh(nil)
	}
	return cursor, err
}

func (i *FragmentIndex) IncrementalRefresh(cursor interface{}) (
	done bool, cursorOut interface{}, err error) {

	i.mu.Lock()
	defer i.mu.Unlock()

	return i.IncrementalRefresh(cursor)
}

func (i *FragmentIndex) incrementalRefresh(cursor interface{}) (
	done bool, cursorOut interface{}, err error) {

	auth, err := i.StorageContext.ObtainAuthContext()
	if err != nil {
		return false, cursor, err
	}
	// TODO(johnny): Move this to GCSContext.
	bucket, prefix := removeMeJournalToBucketAndPrefix(i.Journal)
	log.WithFields(log.Fields{"bucket": bucket, "prefix": prefix, "next": cursor}).
		Info("querying for stored fragments")

	query, _ := cursor.(*storage.Query)
	if query == nil {
		query = &storage.Query{Prefix: prefix}
	}
	objects, err := storage.ListObjects(auth, bucket, query)
	if err != nil {
		return false, cursor, err
	}

	for _, result := range objects.Results {
		log.WithField("name", result.Name).Info("got result")

		fragment, err := ParseFragment(result.Name[len(bucket)+1:])
		if err != nil {
			log.WithFields(log.Fields{"path": result.Name, "err": err}).
				Warning("failed to parse content-name")
		} else {
			i.fragments.Add(fragment)
		}
	}
	log.WithField("nextCursor", objects.Next).Info("finished incremental query")
	return objects.Next == nil, objects.Next, err
}

func removeMeJournalToBucketAndPrefix(journal string) (bucket, prefix string) {
	parts := strings.SplitN(journal, "/", 2)
	return parts[0], parts[1]
}
