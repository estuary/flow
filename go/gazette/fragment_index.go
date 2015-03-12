package gazette

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/logging"
	"google.golang.org/cloud/storage"
	"strings"
	"sync"
)

const kSpoolRollSize = 1 << 30

type FragmentIndex struct {
	LocalDirectory string
	Journal        string
	StorageContext *logging.GCSContext

	mu           sync.Mutex
	fragments    FragmentSet
	currentSpool *Spool
}

func NewFragmentIndex(localDirectory, journal string,
	context *logging.GCSContext) *FragmentIndex {

	return &FragmentIndex{
		LocalDirectory: localDirectory,
		Journal:        journal,
		StorageContext: context,
	}
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

	// Persist the spool.
	go func(spool *Spool) {
		if err := spool.Persist(i.StorageContext); err != nil {
			log.WithFields(log.Fields{"err": err, "path": spool.LocalPath()}).
				Error("failed to persist")
		}
	}(i.currentSpool)
	i.currentSpool = nil
}

func (i *FragmentIndex) InvokeWithSpool(invoke func(*Spool)) {
	i.mu.Lock()
	defer i.mu.Unlock()

	writeOffset := i.fragments.EndOffset()

	if i.currentSpool != nil && (i.currentSpool.LastCommit != writeOffset ||
		i.currentSpool.CommittedSize() > kSpoolRollSize) {
		i.FinishCurrentSpool()
	}
	if i.currentSpool == nil {
		i.currentSpool = NewSpool(i.LocalDirectory, i.Journal, writeOffset)
	}
	invoke(i.currentSpool)
	i.fragments.Add(i.currentSpool.Fragment())
}

func (i *FragmentIndex) LoadFromContext() (cursor interface{}, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.fragments = i.fragments[:0] // Truncate entries.

	// Perform iterative incremental loads until no new fragments are available.
	for done := false; !done && err == nil; {
		done, cursor, err = i.IncrementalRefresh(nil)
	}
	return cursor, err
}

func (i *FragmentIndex) IncrementalRefresh(cursor interface{}) (
	done bool, cursorOut interface{}, err error) {

	i.mu.Lock()
	defer i.mu.Unlock()

	auth, err := i.StorageContext.ObtainAuthContext()
	if err != nil {
		return false, cursor, err
	}
	bucket, prefix := JournalToBucketAndPrefix(i.Journal)

	query, _ := cursor.(*storage.Query)
	if query == nil {
		query = &storage.Query{Prefix: prefix}
	}
	objects, err := storage.ListObjects(auth, bucket, query)
	if err != nil {
		return false, cursor, err
	}

	for _, result := range objects.Results {
		begin, end, sum, err := ParseContentName(result.Name[len(prefix):])
		if err != nil {
			log.WithFields(log.Fields{"path": result.Name, "err": err}).
				Warning("failed to parse content-name")
		} else {
			i.fragments.Add(Fragment{begin, end, sum})
		}
	}
	return len(objects.Results) != 0, objects.Next, err
}

func JournalToBucketAndPrefix(journal string) (bucket, prefix string) {
	parts := strings.SplitN(journal, "/", 2)
	return parts[0], parts[1]
}
