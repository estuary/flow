package stores

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"
)

// MemoryStore is an in-memory implementation of Store for testing.
type MemoryStore struct {
	URL      *url.URL
	Content  map[string][]byte
	ModTimes map[string]time.Time
	mu       sync.RWMutex
}

func NewMemoryStore(ep *url.URL) *MemoryStore {
	var ms = &MemoryStore{
		URL:      ep,
		Content:  make(map[string][]byte),
		ModTimes: make(map[string]time.Time),
	}
	return ms
}

func (m *MemoryStore) SignGet(path string, d time.Duration) (string, error) {
	var u = m.URL.JoinPath(path)
	u.Scheme = "memory" // Use a custom scheme to indicate in-memory storage.
	return u.String(), nil
}

func (m *MemoryStore) Exists(ctx context.Context, path string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var _, exists = m.Content[path]
	return exists, nil
}

func (m *MemoryStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var content, exists = m.Content[path]
	if !exists {
		return nil, fmt.Errorf("path not found: %s", path)
	}

	return io.NopCloser(bytes.NewReader(content)), nil
}

func (m *MemoryStore) Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	// Read the entire content into memory
	var buf = make([]byte, contentLength)
	if _, err := content.ReadAt(buf, 0); err != nil {
		return fmt.Errorf("failed to read content: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.Content[path] = buf
	m.ModTimes[path] = time.Now()
	return nil
}

func (m *MemoryStore) List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for fullPath := range m.Content {
		if !strings.HasPrefix(fullPath, prefix) {
			continue
		}
		var err error
		var relPath = strings.TrimPrefix(fullPath, prefix)

		if modTime, ok := m.ModTimes[fullPath]; ok {
			err = callback(relPath, modTime)
		} else {
			err = callback(relPath, time.Unix(1652140800, 0))
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MemoryStore) Remove(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.Content, path)
	delete(m.ModTimes, path)
	return nil
}

func (m *MemoryStore) IsAuthError(err error) bool {
	return false
}
