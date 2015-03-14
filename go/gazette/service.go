package gazette

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	//"github.com/coreos/go-etcd/etcd"
	"github.com/pippio/services/storage-client"
	"google.golang.org/cloud/storage"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
)

type Service struct {
	LocalDirectory string
	GCSContext     *storageClient.GCSContext

	masters map[string]*JournalMaster
	//replicas map[string]*JournalReplica

	indexes map[string]*FragmentIndex
	mu      sync.Mutex
}

func NewService(localDirectory string,
	gcsContext *storageClient.GCSContext) *Service {

	service := &Service{
		LocalDirectory: localDirectory,
		GCSContext:     gcsContext,
		masters:        make(map[string]*JournalMaster),
		indexes:        make(map[string]*FragmentIndex),
	}

	for _, spool := range RecoverSpools(localDirectory) {
		log.WithField("path", spool.LocalPath()).Warning("recovering spool")
		service.obtainFragmentIndex(spool.Journal).AddFragment(spool.Fragment())
		go persistUntilDone(spool, gcsContext)
	}
	return service
}

func (s *Service) obtainFragmentIndex(journal string) *FragmentIndex {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, ok := s.indexes[journal]
	if !ok {
		index = NewFragmentIndex(s.LocalDirectory, journal, s.GCSContext)
		s.indexes[journal] = index
	}
	return index
}

func (s *Service) obtainJournalMaster(name string) (*JournalMaster, error) {
	index := s.obtainFragmentIndex(name)

	s.mu.Lock()
	defer s.mu.Unlock()

	journal, ok := s.masters[name]
	if !ok {
		journal = NewJournalMaster(name, index)
		s.masters[name] = journal
		go journal.Serve()

		log.WithField("journal", journal).Info("built journal master")
	}
	return journal, nil
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// "Route" |journal| to a |JournalMaster|.

	if r.Method == "GET" {
		s.serveRead(w, r)
	} else if r.Method == "PUT" {
		s.serveWrite(w, r)
	} else {
		http.Error(w, "unsupported method", http.StatusBadRequest)
	}
}

func (s *Service) serveWrite(w http.ResponseWriter, r *http.Request) {
	var journalName string = r.URL.Path[1:]
	var err error

	journal, err := s.obtainJournalMaster(journalName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	request := RequestPool.Get().(Request)
	request.Request = r

	journal.AppendRequests <- request
	err = <-request.Response

	if err == nil {
		http.Error(w, "OK", http.StatusOK)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	RequestPool.Put(request)
}

func (s *Service) serveRead(w http.ResponseWriter, r *http.Request) {
	var journalName string = r.URL.Path[1:]
	var offset int64
	var err error

	index := s.obtainFragmentIndex(journalName)

	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		offset = index.WriteOffset()
	}
	var wroteHeaders bool

	for {
		fragment, spool := index.RouteRead(offset)

		if !wroteHeaders && fragment.Begin > offset {
			// Skip the offset forward to the next available fragment.
			offset = fragment.Begin
		} else if fragment.Begin > offset {
			// Next fragment is discontiguous with the current partial response.
			// We must stop here, and let the client re-request.
			break
		}

		if !wroteHeaders {
			w.Header().Add("Content-Range",
				fmt.Sprintf("bytes %v-%v/%v", offset, math.MaxInt64, math.MaxInt64))
			w.WriteHeader(206)
			wroteHeaders = true
		}

		var delta int64
		if fragment.Size() == 0 {
			delta = 0
		} else if spool == nil {
			delta = s.serveRemoteFragmentRead(w, journalName, fragment, offset)
		} else {
			delta = s.serveSpoolRead(w, spool, offset)
		}

		if delta == 0 {
			break
		} else {
			offset += delta
		}
	}
}

var ReadBufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 1<<15) },
}

func (s *Service) serveSpoolRead(w io.Writer, spool *Spool, offset int64) int64 {
	buffer := ReadBufferPool.Get().([]byte)
	defer ReadBufferPool.Put(buffer)

	// Limit buffer to the committed portion of |spool|.
	var sizedBuffer []byte
	if avail := spool.LastCommit - offset; avail < int64(len(buffer)) {
		sizedBuffer = buffer[:avail]
	} else {
		sizedBuffer = buffer
	}
	_, err := spool.backingFile.File().ReadAt(sizedBuffer, offset-spool.Begin)
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err,
			"offset": offset,
			"path":   spool.LocalPath(),
		}).Error("error reading from spool")
		return 0
	}
	n, err := w.Write(sizedBuffer)

	if err != nil {
		log.WithFields(log.Fields{"err": err, "offset": offset}).
			Warn("failed to write to client")
	} else if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return int64(n)
}

func (s *Service) serveRemoteFragmentRead(w io.Writer, journal string,
	fragment Fragment, offset int64) int64 {

	auth, err := s.GCSContext.ObtainAuthContext()
	if err != nil {
		log.WithField("err", err).Error("failed to obtain auth context")
		return 0
	}

	path := filepath.Join(journal, fragment.ContentName())
	bucket, bucketPath := removeMeJournalToBucketAndPrefix(path)
	r, err := storage.NewReader(auth, bucket, bucketPath)
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err,
			"bucket": bucket,
			"path":   bucketPath,
		}).Error("failed to read fragment")
	}
	// Discard until |offset| is reached.
	io.CopyN(ioutil.Discard, r, offset-fragment.Begin)

	delta, err := io.Copy(w, r)
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err,
			"bucket": bucket,
			"path":   bucketPath,
		}).Error("error while reading fragment")
	} else {
		log.WithFields(log.Fields{
			"bucket": bucket,
			"path":   bucketPath,
		}).Info("served remote fragment")
	}
	return delta
}

/*
func (s *Service) OnEtcdResponse(response *etcd.Response, tree *etcd.Node) {
	journalModels := extractJournalNodes(tree)

	for i, model := range journalModels {
		// Add and drop journals as needed, until |s.Journals| matches
		// the form of |journalModels|.
		for i == len(s.Journals) || s.Journals[i].Name != model.Name {
			if i == len(s.Journals) {
				// Insert a new journal at the tail.
				s.Journals = append(s.Journals, &Journal{Name: model.Name})
				log.WithField("name", model.Name).Info("tracking journal")
			} else if s.Journals[i].Name < model.Name {
				log.WithField("name", s.Journals[i].Name).Info("dropping journal")

				copy(s.Journals[i:], s.Journals[i+1:])
				s.Journals = s.Journals[:len(s.Journals)-1]
			} else if s.Journals[i].Name > model.Name {
				// Splice in a new journal.
				s.Journals = append(s.Journals, nil)
				copy(s.Journals[i+1:], s.Journals[i:])

				s.Journals[i] = &Journal{Name: model.Name}
				log.WithField("name", model.Name).Info("tracking journal")
			}
		}
		//s.updateJournal(s.Journals[i], model.Node)
	}
	for len(s.Journals) > len(journalModels) {
		i := len(s.Journals) - 1
		log.WithField("name", s.Journals[i].Name).Info("dropping journal")
		s.Journals = s.Journals[:i]
	}
}

type journalNode struct {
	Name string
	Node *etcd.Node
}

// Identifies and returns (in sorted order) etcd Nodes which represent journals.
func extractJournalNodes(tree *etcd.Node) []journalNode {
	var result []journalNode

	// Walk the tree. Journals are represented in etcd as directory
	// nodes with no sub-directory children.
	stack := []etcd.Nodes{tree.Nodes}
	for i := len(stack); i != 0; i = len(stack) {
		if j := len(stack[i-1]); j == 0 {
			stack = stack[:i-1]
			continue
		}
		node := stack[i-1][0]
		stack[i-1] = stack[i-1][1:] // Pop.

		if len(node.Nodes) != 0 {
			stack = append(stack, node.Nodes)
		}
		var hasSubDir bool
		for _, sub := range node.Nodes {
			if sub.Dir {
				hasSubDir = true
				break
			}
		}
		if !node.Dir || hasSubDir {
			continue
		}
		result = append(result, journalNode{node.Key[len(tree.Key):], node})
	}
	return result
}
*/
