# broker/stores

This module provides a unified abstraction over cloud storage backends for persisting Gazette journal fragments.

## Overview

The stores package manages access to fragment storage across multiple cloud providers (S3, GCS, Azure) and local filesystem. It provides:

- Unified `Store` interface for all storage operations
- Continuous health monitoring for storage backends, with exponential retries
- Prometheus metrics for observability
- Lifecycle management through mark-and-sweep garbage collection

## Architecture

### Core Interface

```go
type Store interface {
    SignGet(path string, d time.Duration) (string, error)  // Pre-signed URLs to BLOBs
    Exists(ctx context.Context, path string) (bool, error)
    Get(ctx context.Context, path string) (io.ReadCloser, error)
    Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error
    List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error
    Remove(ctx context.Context, path string) error
    IsAuthError(error) bool
}
```

### Key Components

- **ActiveStore**: Wraps a Store with health checking, metrics, and GC support
- **FragmentStore**: Strongly-typed string representing a storage URL (e.g., `s3://bucket/path/`)
- **Registry**: Maps URL schemes to Store constructors
- **Index**: Maps unique FragmentStore values to associated ActiveStore

### Storage Backends

- `s3/`: Amazon S3 and compatible object stores
- `gcs/`: Google Cloud Storage
- `azure/`: Azure Blob Storage (AD and account-based auth)
- `fs/`: Local filesystem and NFS

Storage backends support various URL query parameters for configuration

## Usage

```go
// Register store providers at startup
stores.RegisterProviders(map[string]stores.Constructor{
    "s3":    s3.New,
    "file":  fs.New,
    // ... etc
})

// Get or create a store for a fragment URL
var fs pb.FragmentStore = "s3://my-bucket/journals/"
var store = stores.Get(fs)

// Mark the store as in-use.
store.Mark.Store(true)

// Check the health of the store.
var nextCh, err = store.HealthStatus()

// Await its very first health check.
if err == ErrFirstHealthCheck {
    <-nextCh
    nextCh, err = store.HealthStatus()
}

// Or, await checks until the store is healthy.
for err != nil {
    <-nextCh
    nextCh, err = store.HealthStatus()
}

// Use the store.
reader, err := store.Get(ctx, "path/to/fragment")

// Periodically Sweep unmarked stores, stopping health checks.
var swept int
swept = stores.Sweep()
```

## Integration

The broker uses stores for:

1. **Fragment Persistence**: Writing completed fragments to durable storage
2. **Fragment Serving**: Reading fragments for client requests
3. **Health Monitoring**: Ensuring storage is available before accepting writes
4. **Journal Resolution**: Pre-fetching stores for active journals
