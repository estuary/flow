package stores

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// checkLoop checks the health of a store and schedules the next check.
// It takes `stores` by argument so that left-over queued checks are effectively
// discarded by the `clearStores` test helper.
func checkLoop(stores map[pb.FragmentStore]*ActiveStore, fs pb.FragmentStore, attempt int) {
	storesMu.RLock()
	var active, ok = stores[fs]
	storesMu.RUnlock()

	if !ok {
		return // Removed while we were waiting. Stop checks.
	}

	// Require that health checks complete within a generous timeout.
	var ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
	var err = runCheck(ctx, active)
	cancel()

	// Determine next check interval.
	var checkInterval time.Duration

	if err == nil {
		checkInterval = 5 * time.Minute

		log.WithFields(log.Fields{
			"store":    fs,
			"attempt":  attempt,
			"interval": checkInterval,
		}).Info("store health check succeeded")
		attempt = 0
	} else {
		switch attempt {
		case 0:
			checkInterval = 0
		case 1:
			checkInterval = time.Second / 10
		case 2, 3:
			checkInterval = time.Second
		case 4, 5:
			checkInterval = time.Second * 10
		default:
			checkInterval = time.Second * 100
		}

		log.WithFields(log.Fields{
			"store":    fs,
			"error":    err,
			"attempt":  attempt,
			"interval": checkInterval,
		}).Warn("store health check failed")
		attempt += 1
	}

	// Schedule next check with +/- 5% jitter to prevent thundering herd
	checkInterval += time.Duration((rand.Float64() - 0.5) * 0.1 * float64(checkInterval))

	storesMu.RLock()
	if a, ok := stores[fs]; ok && a == active {
		active.UpdateHealth(err)
		_ = time.AfterFunc(checkInterval, func() { checkLoop(stores, fs, attempt) })
	} else {
		// Removed while we were checking. Stop checks.
		// We don't call UpdateHealth as Sweep already marked it with errLastHealthCheck.
	}
	storesMu.RUnlock()
}

// runCheck performs a health check of the store by verifying all critical operations.
// It's designed to be tolerant of concurrent execution by multiple Gazette brokers
// checking the same store simultaneously.
func runCheck(ctx context.Context, s *ActiveStore) error {
	const (
		testPath    = ".test/health-check"
		testContent = "health-check\n"
	)

	// 1. PUT test file
	var content = strings.NewReader(testContent)
	if err := s.Store.Put(ctx, testPath, content, int64(len(testContent)), ""); err != nil {
		return fmt.Errorf("health check PUT failed: %w", err)
	}

	// 2. GET and verify content
	var err error
	var rc io.ReadCloser
	if rc, err = s.Store.Get(ctx, testPath); err != nil {
		return fmt.Errorf("health check GET failed: %w", err)
	} else if rc == nil {
		return fmt.Errorf("health check GET returned nil reader")
	}
	defer rc.Close()

	var buf bytes.Buffer
	if _, err = io.Copy(&buf, rc); err != nil {
		return fmt.Errorf("health check read failed: %w", err)
	}
	rc.Close()

	if buf.String() != testContent {
		return fmt.Errorf("health check content mismatch: got %q, want %q", buf.String(), testContent)
	}

	// 3. LIST and verify file appears
	var found bool
	err = s.Store.List(ctx, ".test/", func(path string, modTime time.Time) error {
		if path == "health-check" {
			found = true
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("health check LIST failed: %w", err)
	}
	if !found {
		return fmt.Errorf("health check LIST did not find test file")
	}

	// 4. SignGet to generate URL
	var signedURL string
	if signedURL, err = s.Store.SignGet(testPath, 5*time.Minute); err != nil {
		return fmt.Errorf("health check SignGet failed: %w", err)
	}

	if !strings.HasPrefix(signedURL, "http") {
		return nil // Skip fetches of non-HTTPS (HTTP when testing) signed URLs.
	}

	// 5. Fetch signed URL and verify content
	var req *http.Request
	if req, err = http.NewRequestWithContext(ctx, "GET", signedURL, nil); err != nil {
		return fmt.Errorf("health check request creation failed: %w", err)
	}

	var resp *http.Response
	if resp, err = http.DefaultClient.Do(req); err != nil {
		return fmt.Errorf("health check fetch failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check fetch returned status %d", resp.StatusCode)
	}

	buf.Reset()
	if _, err = io.Copy(&buf, resp.Body); err != nil {
		return fmt.Errorf("health check fetch read failed: %w", err)
	}

	if buf.String() != testContent {
		return fmt.Errorf("health check fetch content mismatch: got %q, want %q", buf.String(), testContent)
	}

	return nil
}
