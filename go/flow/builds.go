package flow

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/estuary/flow/go/protocols/catalog"
	_ "github.com/mattn/go-sqlite3" // Import for registration side-effect.
	"google.golang.org/api/option"
)

// BuildService manages active catalog builds.
type BuildService struct {
	baseURL  *url.URL                // URL to which buildIDs are joined.
	builds   map[string]*sharedBuild // All active builds.
	gsClient *storage.Client         // Google storage client which is initialized on first use.
	mu       sync.Mutex
}

// Build is lightweight reference to a shared catalog build.
type Build struct {
	*sharedBuild
}

// sharedBuild is an shared representation of an active catalog build.
type sharedBuild struct {
	svc        *BuildService
	buildID    string
	references int

	db          *sql.DB
	dbLocalPath string
	dbTempfile  *os.File
	dbErr       error
	dbOnce      sync.Once
}

// NewBuildService returns a new *BuildService.
// The |baseURL| must be an absolute URL which ends in '/'.
func NewBuildService(baseURL string) (*BuildService, error) {
	var base, err = url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing base URL: %w", err)
	} else if !base.IsAbs() {
		return nil, fmt.Errorf("base URL %q is not absolute", baseURL)
	} else if l := len(base.Path); l == 0 || base.Path[l-1] != '/' {
		return nil, fmt.Errorf("base URL %q must end in '/'", baseURL)
	}

	return &BuildService{
		baseURL:  base,
		builds:   make(map[string]*sharedBuild),
		gsClient: nil, // Initialized lazily.
	}, nil
}

// Open an identified build.
// The returned *Build must be Closed when it's no longer needed.
// It will panic on garbage collection if Close has not been called.
func (s *BuildService) Open(buildID string) *Build {
	// Fetch or create a sharedBuild to back this |buildID|.
	s.mu.Lock()
	var shared, ok = s.builds[buildID]
	if ok {
		shared.references++
	} else {
		shared = &sharedBuild{
			svc:        s,
			buildID:    buildID,
			references: 1,
		}
		s.builds[buildID] = shared
	}
	s.mu.Unlock()

	var out = &Build{sharedBuild: shared}

	// Catch resource leaks by panic-ing if a *Build is
	// garbage collected without having first been closed.
	// (note Close clears the finalizer we set here).
	var _, file, line, _ = runtime.Caller(1)
	runtime.SetFinalizer(out, func(b *Build) {
		panic(fmt.Sprintf("garbage-collected catalog Build was not closed (%s:%d)", file, line))
	})

	return out
}

// BuildID returns the BuildID of this Catalog.
func (b *Build) BuildID() string { return b.buildID }

// Extract invokes the callback with the Build's database.
// It returns an error returned by the callback.
// The passed *sql.DB must not be retained beyond the callback.
func (b *Build) Extract(fn func(*sql.DB) error) error {
	b.dbOnce.Do(func() { _ = b.dbInit() })

	if b.dbErr != nil {
		return b.dbErr
	}
	return fn(b.db)
}

// Close the Build. If this is the last remaining reference,
// then all allocated resources are cleaned up.
func (b *Build) Close() error {
	if b.sharedBuild == nil {
		return nil
	}
	var shared = b.sharedBuild
	b.sharedBuild = nil

	shared.svc.mu.Lock()
	shared.references--
	var done = shared.references == 0
	if done {
		delete(shared.svc.builds, shared.buildID)
	}
	shared.svc.mu.Unlock()

	runtime.SetFinalizer(b, nil) // Clear finalizer.

	if !done {
		return nil
	}
	return shared.destroy()
}

func (b *Build) dbInit() (err error) {
	defer func() { b.dbErr = err }()

	var resource = b.svc.baseURL.ResolveReference(&url.URL{Path: b.buildID})
	b.dbLocalPath, b.dbTempfile, err = fetchResource(b.svc, resource)
	if err != nil {
		return fmt.Errorf("fetching DB: %w", err)
	}

	b.db, err = sql.Open("sqlite3", fmt.Sprintf("file://%s?mode=ro", b.dbLocalPath))
	if err != nil {
		return fmt.Errorf("opening DB: %w", err)
	}

	// Sanity-check that the persisted build ID matches the ID used to retrieve this DB.
	config, err := catalog.LoadBuildConfig(b.db)
	if err != nil {
		return fmt.Errorf("loading build config from %s: %w", b.dbLocalPath, err)
	} else if config.BuildId != b.buildID {
		return fmt.Errorf(
			"ID %q, used to retrieve this catalog DB, differs from its configured ID %q",
			b.buildID, config.BuildId)
	}

	// Sanity-check that no build errors occurred.
	errors, err := catalog.LoadAllErrors(b.db)
	if err != nil {
		return fmt.Errorf("loading build errors: %w", err)
	} else if len(errors) != 0 {
		return fmt.Errorf("catalog has %d errors: %v", len(errors), errors)
	}

	return nil
}

func (b *sharedBuild) destroy() error {
	if b.db == nil {
		// Nothing to close.
	} else if err := b.db.Close(); err != nil {
		return fmt.Errorf("closing DB: %w", err)
	}

	if b.dbTempfile == nil {
		// Nothing to remove.
	} else if err := b.dbTempfile.Close(); err != nil {
		return fmt.Errorf("closing DB tempfile: %w", err)
	} else if err = os.Remove(b.dbLocalPath); err != nil {
		return fmt.Errorf("removing DB tempfile: %w", err)
	}

	return nil
}

func fetchResource(svc *BuildService, resource *url.URL) (path string, tempfile *os.File, err error) {
	var ctx = context.Background()

	switch resource.Scheme {
	case "file":
		return resource.Path, nil, nil
	case "gs":
		// Building the client will fail if application default credentials aren't located.
		// https://developers.google.com/accounts/docs/application-default-credentials
		svc.mu.Lock()
		if svc.gsClient == nil {
			svc.gsClient, err = storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
		}
		svc.mu.Unlock()

		if err != nil {
			return "", nil, fmt.Errorf("building google storage client: %w", err)
		}

		var r *storage.Reader
		if r, err = svc.gsClient.Bucket(resource.Host).Object(resource.Path[1:]).NewReader(ctx); err != nil {
			return "", nil, err
		}
		defer r.Close()

		if tempfile, err = os.CreateTemp("", "build"); err != nil {
			return "", nil, err
		}
		if _, err = io.Copy(tempfile, r); err != nil {
			_ = os.Remove(tempfile.Name())
			return "", nil, err
		}

		return tempfile.Name(), tempfile, nil

	default:
		return "", nil, fmt.Errorf("unsupported scheme: %s", resource.Scheme)
	}
}
