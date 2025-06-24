package stores

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestActiveStore(t *testing.T) {
	var ctx = context.Background()
	var testErr = errors.New("operation failed")
	var callbackErr = errors.New("callback error")

	// Test all operations in a single comprehensive test
	var calls []string
	var mockStore = &CallbackStore{
		ListFunc: func(_ Store, ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
			calls = append(calls, "List")
			require.Equal(t, "test/prefix/", prefix)
			callback("file1", time.Now())
			return callback("file2", time.Now())
		},
		ExistsFunc: func(_ Store, ctx context.Context, path string) (bool, error) {
			calls = append(calls, "Exists")
			return true, nil
		},
		GetFunc: func(_ Store, ctx context.Context, path string) (io.ReadCloser, error) {
			calls = append(calls, "Get")
			return io.NopCloser(strings.NewReader("content")), nil
		},
		PutFunc: func(_ Store, ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
			calls = append(calls, "Put")
			require.Equal(t, int64(7), contentLength)
			require.Equal(t, "gzip", contentEncoding)
			return nil
		},
		RemoveFunc: func(_ Store, ctx context.Context, path string) error {
			calls = append(calls, "Remove")
			return nil
		},
		SignGetFunc: func(_ Store, path string, duration time.Duration) (string, error) {
			calls = append(calls, "SignGet")
			require.Equal(t, time.Hour, duration)
			return "https://signed.url", nil
		},
	}

	as := NewActiveStore(pb.FragmentStore("file:///test/"), mockStore, nil)
	require.Equal(t, pb.FragmentStore("file:///test/"), as.Key)

	var paths []string
	err := as.List(ctx, "test/prefix/", func(path string, modTime time.Time) error {
		paths = append(paths, path)
		if len(paths) > 1 {
			return callbackErr
		}
		return nil
	})
	require.Equal(t, callbackErr, err) // Test callback error propagation
	require.Len(t, paths, 2)

	exists, err := as.Exists(ctx, "test/file")
	require.NoError(t, err)
	require.True(t, exists)

	rc, err := as.Get(ctx, "test/file")
	require.NoError(t, err)
	content, _ := io.ReadAll(rc)
	require.Equal(t, "content", string(content))
	rc.Close()

	err = as.Put(ctx, "test/file", strings.NewReader("content"), 7, "gzip")
	require.NoError(t, err)

	err = as.Remove(ctx, "test/file")
	require.NoError(t, err)

	url, err := as.SignGet("test/file", time.Hour)
	require.NoError(t, err)
	require.Equal(t, "https://signed.url", url)

	// Verify all operations were called in the expected order
	require.Equal(t, []string{
		"List",
		"Exists",
		"Get",
		"Put",
		"Remove",
		"SignGet",
	}, calls)

	// Test error cases
	t.Run("Errors", func(t *testing.T) {
		// Test nil store initialization error
		asNil := NewActiveStore(pb.FragmentStore("file:///test/"), nil, errors.New("init failed"))

		// All operations should return init error
		err := asNil.List(ctx, "test", func(string, time.Time) error { return nil })
		require.Contains(t, err.Error(), "init failed")

		exists, err := asNil.Exists(ctx, "test/file")
		require.Error(t, err)
		require.False(t, exists)

		rc, err := asNil.Get(ctx, "test/file")
		require.Error(t, err)
		require.Nil(t, rc)

		// Test operation errors
		var errorStore = &CallbackStore{
			ListFunc: func(_ Store, ctx context.Context, prefix string, callback func(string, time.Time) error) error {
				return testErr
			},
			ExistsFunc: func(_ Store, ctx context.Context, path string) (bool, error) { return false, testErr },
			GetFunc:    func(_ Store, ctx context.Context, path string) (io.ReadCloser, error) { return nil, testErr },
			PutFunc: func(_ Store, ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
				return testErr
			},
			RemoveFunc:  func(_ Store, ctx context.Context, path string) error { return testErr },
			SignGetFunc: func(_ Store, path string, duration time.Duration) (string, error) { return "", testErr },
		}

		asErr := NewActiveStore(pb.FragmentStore("file:///test/"), errorStore, nil)

		// All operations should return testErr
		require.Equal(t, testErr, asErr.List(ctx, "test", func(string, time.Time) error { return nil }))
		_, err = asErr.Exists(ctx, "test/file")
		require.Equal(t, testErr, err)
		_, err = asErr.Get(ctx, "test/file")
		require.Equal(t, testErr, err)
		require.Equal(t, testErr, asErr.Put(ctx, "test/file", strings.NewReader(""), 0, ""))
		require.Equal(t, testErr, asErr.Remove(ctx, "test/file"))
		_, err = asErr.SignGet("test/file", time.Hour)
		require.Equal(t, testErr, err)
	})

	// Test context cancellation
	t.Run("ContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var ctxStore = &CallbackStore{
			ListFunc: func(_ Store, ctx context.Context, prefix string, callback func(string, time.Time) error) error {
				return ctx.Err()
			},
			GetFunc: func(_ Store, ctx context.Context, path string) (io.ReadCloser, error) {
				return nil, ctx.Err()
			},
		}

		asCtx := NewActiveStore(pb.FragmentStore("file:///test/"), ctxStore, nil)
		require.Equal(t, context.Canceled, asCtx.List(ctx, "test/", func(string, time.Time) error { return nil }))
		_, err := asCtx.Get(ctx, "test/file")
		require.Equal(t, context.Canceled, err)
	})
}
