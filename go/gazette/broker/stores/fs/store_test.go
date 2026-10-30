package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	defer func(r string) { FileSystemStoreRoot = r }(FileSystemStoreRoot)
	var tempDir = t.TempDir()
	FileSystemStoreRoot = tempDir

	// Create test files
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "file.txt"), []byte("content"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "sub", "nested.txt"), []byte("nested"), 0644))

	// Test New with invalid query parameter
	_, err := New(mustParseURL("file:///?invalid=param"))
	require.Error(t, err)

	// Create store with empty prefix for simpler testing
	s, err := New(mustParseURL("file:///"))
	require.NoError(t, err)

	// Test SignGet
	signed, err := s.SignGet("file.txt", time.Hour)
	require.NoError(t, err)
	require.Equal(t, "file:///file.txt", signed)

	// Test Exists
	exists, err := s.Exists(context.Background(), "file.txt")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = s.Exists(context.Background(), "missing.txt")
	require.NoError(t, err)
	require.False(t, exists)

	// Test Get
	reader, err := s.Get(context.Background(), "file.txt")
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	reader.Close()
	require.NoError(t, err)
	require.Equal(t, "content", string(content))

	_, err = s.Get(context.Background(), "missing.txt")
	require.Error(t, err)

	// Test Put
	err = s.Put(context.Background(), "new.txt", strings.NewReader("new"), 3, "")
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(tempDir, "new.txt"))

	// Test List
	var files []string
	err = s.List(context.Background(), "", func(path string, modTime time.Time) error {
		files = append(files, path)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"file.txt", "new.txt", "sub/nested.txt"}, files)

	// Test List with prefix
	files = nil
	err = s.List(context.Background(), "sub", func(path string, modTime time.Time) error {
		files = append(files, path)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"nested.txt"}, files)

	// Test List callback error
	err = s.List(context.Background(), "", func(path string, modTime time.Time) error {
		return errors.New("callback error")
	})
	require.Equal(t, "callback error", err.Error())

	// Test Remove
	err = s.Remove(context.Background(), "new.txt")
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(tempDir, "new.txt"))

	err = s.Remove(context.Background(), "missing.txt")
	require.Error(t, err)
}

func TestPutWithPrefix(t *testing.T) {
	defer func(r string) { FileSystemStoreRoot = r }(FileSystemStoreRoot)
	var tempDir = t.TempDir()
	FileSystemStoreRoot = tempDir

	// Create base directory
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "base"), 0755))

	// Test with prefix
	s, _ := New(mustParseURL("file:///base"))
	err := s.Put(context.Background(), "file.txt", strings.NewReader("data"), 4, "")
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(tempDir, "basefile.txt"))

	// Test with non-existent base directory
	s2, _ := New(mustParseURL("file:///nonexistent"))
	err = s2.Put(context.Background(), "file.txt", strings.NewReader("data"), 4, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), invalidFileStoreDirectory)
}

func TestIsAuthError(t *testing.T) {
	s, _ := New(mustParseURL("file:///"))

	tests := []struct {
		err      error
		expected bool
	}{
		{os.ErrPermission, true},
		{fmt.Errorf("wrapped: %w", os.ErrPermission), true},
		{fmt.Errorf("%s: test", invalidFileStoreDirectory), true},
		{os.ErrNotExist, false},
		{errors.New("other error"), false},
		{nil, false},
	}

	for _, tt := range tests {
		require.Equal(t, tt.expected, s.IsAuthError(tt.err), "err: %v", tt.err)
	}
}

func TestPathRewriting(t *testing.T) {
	defer func(r string) { FileSystemStoreRoot = r }(FileSystemStoreRoot)
	var tempDir = t.TempDir()
	FileSystemStoreRoot = tempDir

	// Create file at rewritten path
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "rewritten.txt"), []byte("rewritten"), 0644))

	// Create store with rewrite
	s, err := New(mustParseURL("file:///?find=original.txt&replace=rewritten.txt"))
	require.NoError(t, err)

	// Verify rewrite works
	exists, err := s.Exists(context.Background(), "original.txt")
	require.NoError(t, err)
	require.True(t, exists)

	reader, err := s.Get(context.Background(), "original.txt")
	require.NoError(t, err)
	content, _ := io.ReadAll(reader)
	reader.Close()
	require.Equal(t, "rewritten", string(content))
}

func TestPutAtomicOperations(t *testing.T) {
	defer func(r string) { FileSystemStoreRoot = r }(FileSystemStoreRoot)
	var tempDir = t.TempDir()
	FileSystemStoreRoot = tempDir

	s, _ := New(mustParseURL("file:///"))

	// Test temp file cleanup on error
	err := s.Put(context.Background(), "fail.txt", &failingReader{}, 100, "")
	require.Error(t, err)

	// Verify no temp files remain
	matches, _ := filepath.Glob(filepath.Join(tempDir, ".partial-*"))
	require.Empty(t, matches)

	// Test atomic rename
	err = s.Put(context.Background(), "atomic.txt", strings.NewReader("v1"), 2, "")
	require.NoError(t, err)

	// Open file handle
	reader, err := s.Get(context.Background(), "atomic.txt")
	require.NoError(t, err)
	defer reader.Close()

	// Overwrite file
	err = s.Put(context.Background(), "atomic.txt", strings.NewReader("v2"), 2, "")
	require.NoError(t, err)

	// Original handle should still read old content
	content, _ := io.ReadAll(reader)
	require.Equal(t, "v1", string(content))
}

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

type failingReader struct{}

func (f *failingReader) ReadAt(p []byte, off int64) (int, error) {
	return 0, errors.New("read error")
}
