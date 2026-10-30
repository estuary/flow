package stores

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

// errorReader is a reader that always returns an error
type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("read error")
}

func TestHealthChecks(t *testing.T) {
	const testContent = "health-check\n"

	// Channel to coordinate the "Failure Then Success" test
	var failureThenSuccessCh = make(chan struct{})

	tests := []struct {
		name          string
		expectedError string
		setupStore    func(*httptest.Server) Constructor
	}{
		{
			name: "Success",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						PutFunc: func(_ Store, ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							// Verify correct path and content
							require.Equal(t, ".test/health-check", path)
							require.Equal(t, int64(len(testContent)), contentLength)

							var buf = make([]byte, contentLength)
							_, err := content.ReadAt(buf, 0)
							require.NoError(t, err)
							require.Equal(t, testContent, string(buf))
							return nil
						},
						GetFunc: func(_ Store, ctx context.Context, path string) (io.ReadCloser, error) {
							require.Equal(t, ".test/health-check", path)
							return io.NopCloser(strings.NewReader(testContent)), nil
						},
						ListFunc: func(_ Store, ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
							require.Equal(t, ".test/", prefix)
							return callback("health-check", time.Now())
						},
						SignGetFunc: func(_ Store, path string, d time.Duration) (string, error) {
							require.Equal(t, ".test/health-check", path)
							return server.URL + "/signed/test", nil
						},
						IsAuthErrorFunc: func(_ Store, err error) bool {
							return false
						},
					}, nil
				}
			},
		},
		{
			name:          "GET Failure",
			expectedError: "health check GET failed",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						PutFunc: func(_ Store, ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(_ Store, ctx context.Context, path string) (io.ReadCloser, error) {
							return nil, errors.New("simulated GET failure")
						},
						IsAuthErrorFunc: func(_ Store, err error) bool {
							return false
						},
					}, nil
				}
			},
		},
		{
			name:          "Content Mismatch",
			expectedError: "health check content mismatch",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						PutFunc: func(_ Store, ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(_ Store, ctx context.Context, path string) (io.ReadCloser, error) {
							return io.NopCloser(strings.NewReader("wrong content")), nil
						},
						ListFunc: func(_ Store, ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
							return callback("health-check", time.Now())
						},
						SignGetFunc: func(_ Store, path string, d time.Duration) (string, error) {
							return server.URL + "/wrong", nil
						},
						IsAuthErrorFunc: func(_ Store, err error) bool {
							return false
						},
					}, nil
				}
			},
		},
		{
			name:          "GET Returns Nil Reader",
			expectedError: "health check GET returned nil reader",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						PutFunc: func(_ Store, ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(_ Store, ctx context.Context, path string) (io.ReadCloser, error) {
							return nil, nil // Return nil reader without error
						},
						IsAuthErrorFunc: func(_ Store, err error) bool {
							return false
						},
					}, nil
				}
			},
		},
		{
			name:          "Read Failure",
			expectedError: "health check read failed",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						PutFunc: func(_ Store, ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(_ Store, ctx context.Context, path string) (io.ReadCloser, error) {
							// Return a reader that fails on Read
							return io.NopCloser(errorReader{}), nil
						},
						IsAuthErrorFunc: func(_ Store, err error) bool {
							return false
						},
					}, nil
				}
			},
		},
		{
			name:          "Failure Then Success",
			expectedError: "health check GET failed", // Expect failure on first attempt
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					var attempt int
					return &CallbackStore{
						PutFunc: func(_ Store, ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(_ Store, ctx context.Context, path string) (io.ReadCloser, error) {
							attempt++
							if attempt == 1 {
								return nil, errors.New("simulated transient failure")
							}
							<-failureThenSuccessCh // Wait for the test to signal it's ready.
							return io.NopCloser(strings.NewReader(testContent)), nil
						},
						ListFunc: func(_ Store, ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
							return callback("health-check", time.Now())
						},
						SignGetFunc: func(_ Store, path string, d time.Duration) (string, error) {
							return server.URL + "/signed/test", nil
						},
						IsAuthErrorFunc: func(_ Store, err error) bool {
							return false
						},
					}, nil
				}
			},
		},
	}

	var server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/signed/test" {
			w.Write([]byte(testContent))
		} else {
			w.Write([]byte("wrong content"))
		}
	}))
	defer server.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RegisterProviders(map[string]Constructor{
				"s3": tt.setupStore(server),
			})

			// Get store - this starts health check
			store := Get(pb.FragmentStore("s3://test/"))
			require.NoError(t, store.initErr)

			// Wait for first health check if needed.
			done, err := store.HealthStatus()
			if err == ErrFirstHealthCheck {
				<-done
				done, err = store.HealthStatus()
			}

			// Check result of first health check
			if tt.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			}

			// For "Failure Then Success", wait for the retry to succeed
			if tt.name == "Failure Then Success" {
				close(failureThenSuccessCh)
				<-done // Wait for next health check.
				_, err = store.HealthStatus()
				require.NoError(t, err)
			}
		})
	}
}

func TestCheckDelete(t *testing.T) {
	const prefix = "recovery/"

	t.Run("Success", func(t *testing.T) {
		// The probe removes exactly the fixed-name object it wrote under the prefix.
		var putPath string
		var store = &CallbackStore{
			PutFunc: func(_ Store, _ context.Context, path string, _ io.ReaderAt, _ int64, _ string) error {
				require.Equal(t, prefix+".gazette-delete-probe", path)
				putPath = path
				return nil
			},
			RemoveFunc: func(_ Store, _ context.Context, path string) error {
				require.Equal(t, putPath, path)
				return nil
			},
		}
		require.NoError(t, NewActiveStore("s3://bucket/", store, nil).CheckDelete(context.Background(), prefix))
	})

	t.Run("Root Prefix", func(t *testing.T) {
		// An empty prefix probes the store root, so the probe object has no prefix.
		var store = &CallbackStore{
			PutFunc: func(_ Store, _ context.Context, path string, _ io.ReaderAt, _ int64, _ string) error {
				require.Equal(t, ".gazette-delete-probe", path)
				return nil
			},
			RemoveFunc: func(_ Store, _ context.Context, _ string) error { return nil },
		}
		require.NoError(t, NewActiveStore("s3://bucket/", store, nil).CheckDelete(context.Background(), ""))
	})

	t.Run("Delete Denied", func(t *testing.T) {
		var store = &CallbackStore{
			PutFunc:    func(_ Store, _ context.Context, _ string, _ io.ReaderAt, _ int64, _ string) error { return nil },
			RemoveFunc: func(_ Store, _ context.Context, _ string) error { return errors.New("access denied") },
		}
		var err = NewActiveStore("s3://bucket/", store, nil).CheckDelete(context.Background(), prefix)
		require.ErrorContains(t, err, "delete-probe DELETE failed")
		require.ErrorContains(t, err, "access denied")
	})

	t.Run("Put Failed", func(t *testing.T) {
		var store = &CallbackStore{
			PutFunc: func(_ Store, _ context.Context, _ string, _ io.ReaderAt, _ int64, _ string) error {
				return errors.New("no write")
			},
			RemoveFunc: func(_ Store, _ context.Context, _ string) error {
				require.Fail(t, "Remove should not be called when Put fails")
				return nil
			},
		}
		require.ErrorContains(t, NewActiveStore("s3://bucket/", store, nil).CheckDelete(context.Background(), prefix), "delete-probe PUT failed")
	})

	t.Run("Uninitialized Store", func(t *testing.T) {
		var initErr = errors.New("bad store")
		require.Equal(t, initErr, NewActiveStore("s3://bucket/", nil, initErr).CheckDelete(context.Background(), prefix))
	})
}
