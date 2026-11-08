package gcs

import (
	"errors"
	"net/http"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

// TestGCSStoreIsAuthError tests GCS store auth error classification
func TestGCSStoreIsAuthError(t *testing.T) {
	store := &store{}

	// Test cases for GCS auth errors
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ErrBucketNotExist should be auth error",
			err:      storage.ErrBucketNotExist,
			expected: true,
		},
		{
			name:     "403 Forbidden via googleapi.Error should be auth error",
			err:      &googleapi.Error{Code: http.StatusForbidden, Message: "Permission denied"},
			expected: true,
		},
		{
			name:     "404 Not Found with bucket in message should be auth error",
			err:      &googleapi.Error{Code: http.StatusNotFound, Message: "bucket does not exist"},
			expected: true,
		},
		{
			name:     "404 Not Found without bucket in message should not be auth error",
			err:      &googleapi.Error{Code: http.StatusNotFound, Message: "object not found"},
			expected: false,
		},
		{
			name:     "401 Unauthorized should not be auth error (AuthN failure)",
			err:      &googleapi.Error{Code: http.StatusUnauthorized, Message: "Invalid credentials"},
			expected: false,
		},
		{
			name:     "Generic error should not be auth error",
			err:      errors.New("network error"),
			expected: false,
		},
		{
			name:     "Nil error should not be auth error",
			err:      nil,
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := store.IsAuthError(test.err)
			require.Equal(t, test.expected, result)
		})
	}
}
