package fragment

import (
	"errors"
	"net/http"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

// TestS3BackendIsAuthError tests S3 backend auth error classification
func TestS3BackendIsAuthError(t *testing.T) {
	backend := sharedStores.s3

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
			result := backend.IsAuthError(test.err)
			require.Equal(t, test.expected, result)
		})
	}
}

// TestGCSBackendIsAuthError tests GCS backend auth error classification
func TestGCSBackendIsAuthError(t *testing.T) {
	backend := sharedStores.gcs

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
			result := backend.IsAuthError(test.err)
			require.Equal(t, test.expected, result)
		})
	}
}

// TestAzureBackendIsAuthError tests Azure backend auth error classification
func TestAzureBackendIsAuthError(t *testing.T) {
	backend := sharedStores.azure

	// Test cases for Azure auth errors
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ContainerNotFound should be auth error",
			err:      createAzureStorageError(azblob.ServiceCodeContainerNotFound, http.StatusNotFound),
			expected: true,
		},
		{
			name:     "ContainerDisabled should be auth error",
			err:      createAzureStorageError(azblob.ServiceCodeContainerDisabled, http.StatusForbidden),
			expected: true,
		},
		{
			name:     "AccountIsDisabled should be auth error",
			err:      createAzureStorageError(azblob.ServiceCodeAccountIsDisabled, http.StatusForbidden),
			expected: true,
		},
		{
			name:     "403 Forbidden via storage error should be auth error",
			err:      createAzureStorageError("OtherError", http.StatusForbidden),
			expected: true,
		},
		{
			name:     "401 Unauthorized should not be auth error (AuthN failure)",
			err:      createAzureStorageError("InvalidCredentials", http.StatusUnauthorized),
			expected: false,
		},
		{
			name:     "Generic error should not be auth error",
			err:      errors.New("timeout"),
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
			result := backend.IsAuthError(test.err)
			require.Equal(t, test.expected, result)
		})
	}
}

// TestFSBackendIsAuthError tests filesystem backend auth error classification
func TestFSBackendIsAuthError(t *testing.T) {
	backend := sharedStores.fs

	// Test cases for filesystem auth errors
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "Permission denied should be auth error",
			err:      &os.PathError{Op: "open", Path: "/protected/file", Err: os.ErrPermission},
			expected: true,
		},
		{
			name:     "File not exist (current directory) should not be auth error",
			err:      &os.PathError{Op: "open", Path: "file.txt", Err: os.ErrNotExist},
			expected: false,
		},
		{
			name:     "Generic error should not be auth error",
			err:      errors.New("disk full"),
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
			result := backend.IsAuthError(test.err)
			require.Equal(t, test.expected, result)
		})
	}
}

// mockAzureStorageError implements the azblob.StorageError interface for testing
type mockAzureStorageError struct {
	serviceCode azblob.ServiceCodeType
	statusCode  int
}

func (e *mockAzureStorageError) Error() string {
	return string(e.serviceCode)
}

func (e *mockAzureStorageError) ServiceCode() azblob.ServiceCodeType {
	return e.serviceCode
}

func (e *mockAzureStorageError) Response() *http.Response {
	return &http.Response{StatusCode: e.statusCode}
}

func (e *mockAzureStorageError) Temporary() bool { return false }
func (e *mockAzureStorageError) Timeout() bool   { return false }

// Helper function to create Azure storage errors for testing
func createAzureStorageError(serviceCode azblob.ServiceCodeType, statusCode int) azblob.StorageError {
	return &mockAzureStorageError{
		serviceCode: serviceCode,
		statusCode:  statusCode,
	}
}
