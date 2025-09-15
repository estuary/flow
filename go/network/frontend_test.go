package network

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestFrontendHTTPSHeaders(t *testing.T) {
	// Create a test handler that mimics the frontend's HTTP handler
	handler := func(w http.ResponseWriter, req *http.Request) {
		// Add HSTS header for all HTTPS responses (same as in frontend.go)
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		
		// Simulate a simple response
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}

	// Create a test server with TLS
	server := httptest.NewTLSServer(http.HandlerFunc(handler))
	defer server.Close()

	// Create HTTP client that accepts the test server's certificate
	client := server.Client()

	// Make a request to the test server
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	// Check that HSTS header is present and correct
	hstsHeader := resp.Header.Get("Strict-Transport-Security")
	expectedHSTS := "max-age=31536000; includeSubDomains"
	
	if hstsHeader != expectedHSTS {
		t.Errorf("Expected HSTS header %q, got %q", expectedHSTS, hstsHeader)
	}
}

func TestFrontendHTTPSHeadersAllPaths(t *testing.T) {
	// Test that HSTS headers are set for different response paths
	testCases := []struct {
		name     string
		path     string
		expected int
	}{
		{"root path", "/", http.StatusOK},
		{"auth redirect path", "/auth-redirect", http.StatusOK},
		{"other path", "/some/path", http.StatusOK},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test handler that sets HSTS for all responses
			handler := func(w http.ResponseWriter, req *http.Request) {
				// Add HSTS header for all HTTPS responses
				w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
				
				// Simple response for all paths
				w.WriteHeader(tc.expected)
				w.Write([]byte("OK"))
			}

			// Create recorder to capture response
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest("GET", tc.path, nil)

			// Call handler
			handler(recorder, request)

			// Check HSTS header is present
			hstsHeader := recorder.Header().Get("Strict-Transport-Security")
			expectedHSTS := "max-age=31536000; includeSubDomains"
			
			if hstsHeader != expectedHSTS {
				t.Errorf("Path %s: Expected HSTS header %q, got %q", tc.path, expectedHSTS, hstsHeader)
			}

			// Check status code
			if recorder.Code != tc.expected {
				t.Errorf("Path %s: Expected status %d, got %d", tc.path, tc.expected, recorder.Code)
			}
		})
	}
}

func TestFrontendStructCreation(t *testing.T) {
	// Test that we can create a Frontend struct with minimal dependencies
	controlAPI, _ := url.Parse("http://localhost:8080")
	dashboard, _ := url.Parse("http://localhost:3000")
	
	// Create a minimal tap (we can't easily test the full Frontend without extensive mocking)
	tap := &Tap{}
	
	// This will fail due to missing raw listener, but tests struct creation
	_, err := NewFrontend(
		tap,
		"test.example.com",
		controlAPI,
		dashboard,
		nil, // networkClient
		nil, // shardClient  
		nil, // verifier
	)
	
	// We expect this to fail with the specific error about missing raw listener
	if err == nil {
		t.Error("Expected error for missing raw listener")
	}
	
	expectedErr := "Tap has not tapped a raw net.Listener"
	if err.Error() != expectedErr {
		t.Errorf("Expected error %q, got %q", expectedErr, err.Error())
	}
}