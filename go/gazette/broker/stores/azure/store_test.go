package azure

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/stretchr/testify/require"
)

// TestAzureStoreIsAuthError tests Azure store auth error classification
func TestAzureStoreIsAuthError(t *testing.T) {
	store := &storeBase{}

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
			result := store.IsAuthError(test.err)
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
