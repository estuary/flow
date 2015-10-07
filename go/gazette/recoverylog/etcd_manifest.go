package recoverylog

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/pippio/api-server/common"
	"github.com/pippio/api-server/discovery"
)

// EtcdManifest is an Etcd-backed implementation of Manifest. It represents
// files in Etcd is a single directory with zero or more |FileRecord|s.
type EtcdManifest struct {
	etcdPath string
	etcd     discovery.EtcdService

	// Maps paths of the filesystem to FileRecords.
	index map[string]*FileRecord
}

// Loads the manifest specified at |etcdPath| from |etcd|.
func NewEtcdManifest(etcdPath string, etcd discovery.EtcdService) (EtcdManifest, error) {
	resp, err := etcd.Get(etcdPath, false, true)
	if err != nil {
		return EtcdManifest{}, err
	}

	index := make(map[string]*FileRecord)

	for _, node := range resp.Node.Nodes {
		record := &FileRecord{}
		if err = json.Unmarshal([]byte(node.Value), record); err != nil {
			return EtcdManifest{}, err
		}

		for link := range record.Links {
			index[link] = record
		}
	}
	return EtcdManifest{
		etcdPath: etcdPath,
		etcd:     etcd,
		index:    index,
	}, nil
}

// Creates a new FileRecord linked to |fname| with FirstOffset |offset|.
// |fname| must not already be in use.
func (m EtcdManifest) CreateFile(fname string, offset int64) (*FileRecord, error) {
	if _, ok := m.index[fname]; ok {
		return nil, errors.New("create of known file")
	}
	record := &FileRecord{
		Id:          base64.URLEncoding.EncodeToString(common.GetRandomBytes(12)),
		FirstOffset: offset,
		Links:       map[string]struct{}{fname: {}},
	}
	m.index[fname] = record

	retryable("creating file record", func() error {
		return m.etcd.Create(m.etcdPath+"/"+record.Id, record, 0)
	})
	return record, nil
}

// Deletes a link to |fname| from its owning FileRecord. The link must exist,
// and the FileRecord is returned on success.
func (m EtcdManifest) DeleteFile(fname string) (*FileRecord, error) {
	record, ok := m.index[fname]
	if !ok {
		return nil, errors.New("delete of unknown file")
	}
	delete(record.Links, fname)
	delete(m.index, fname)

	if len(record.Links) == 0 {
		// No links remain: remove the FileRecord from Etcd.
		retryable("deleting file record", func() error {
			return m.etcd.Delete(m.etcdPath+"/"+record.Id, false)
		})
	} else {
		// File is still active. Update in Etcd.
		retryable("updating file record (delete)", func() error {
			return m.etcd.Update(m.etcdPath+"/"+record.Id, record, 0)
		})
	}

	return record, nil
}

// Moves a link |src| to instead link |target|. The |src| link must exist,
// the |target| link must not. The modified FileRecord is returned on success.
func (m EtcdManifest) RenameFile(src, target string) (*FileRecord, error) {
	record, ok := m.index[src]
	if !ok {
		return nil, errors.New("rename of unknown file")
	}
	if _, ok := m.index[target]; ok {
		return nil, errors.New("target file exists")
	}

	delete(record.Links, src)
	record.Links[target] = struct{}{}
	m.index[target] = record

	retryable("updating file record (rename)", func() error {
		return m.etcd.Update(m.etcdPath+"/"+record.Id, record, 0)
	})
	return record, nil
}

// Adds a link |target| for the file currently linked to |src|. The |src| link
// must exist, and |target| must not. The modified FileRecord is returned on
// success.
func (m EtcdManifest) LinkFile(src, target string) (*FileRecord, error) {
	record, ok := m.index[src]
	if !ok {
		return nil, errors.New("link of unknown file")
	}
	if _, ok := m.index[target]; ok {
		return nil, errors.New("target file exists")
	}

	record.Links[target] = struct{}{}
	m.index[target] = record

	retryable("updating file record (link)", func() error {
		return m.etcd.Update(m.etcdPath+"/"+record.Id, record, 0)
	})
	return record, nil
}
