package s3

import (
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
)

// TestS3StoreIsAuthError tests S3 store auth error classification
func TestS3StoreIsAuthError(t *testing.T) {
	store := &store{}

	// Test cases for S3 auth errors
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "NoSuchBucket error should be auth error",
			err:      awserr.New(s3.ErrCodeNoSuchBucket, "The specified bucket does not exist", nil),
			expected: true,
		},
		{
			name:     "AccessDenied error should be auth error",
			err:      awserr.New(s3ErrCodeAccessDenied, "Access Denied", nil),
			expected: true,
		},
		{
			name:     "403 Forbidden via RequestFailure should be auth error",
			err:      awserr.NewRequestFailure(awserr.New("Forbidden", "Forbidden", nil), http.StatusForbidden, "request-id"),
			expected: true,
		},
		{
			name:     "InvalidAccessKeyId should not be auth error (AuthN failure)",
			err:      awserr.New("InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist", nil),
			expected: false,
		},
		{
			name:     "Generic error should not be auth error",
			err:      errors.New("connection timeout"),
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
