# e2e-support

Test harness for end-to-end integration testing of data-plane
and control-plane components. Provides a hermetic, self-contained
environment with managed etcd and Gazette broker processes,
all communicating over Unix domain sockets in a temporary directory.

## Key types

- `DataPlane` — top-level handle that owns an etcd instance,
  a Gazette broker cluster, and an authenticated journal client.
  Start one per test suite and run sub-tests against it.
- `DataPlaneArgs` — configuration (e.g. broker count).
- `EtcdInstance` (`etcd.rs`) — manages a child etcd process with UDS transport.
- `GazetteCluster` (`gazette.rs`) — manages N broker processes
  and provides HMAC-authenticated `gazette::journal::Client` construction.

## Lifecycle

1. `DataPlane::start()` launches etcd, then brokers, then builds a journal client.
2. Tests use the `journal_client` (and later, reactor/control-plane handles)
   to exercise the system.
3. `DataPlane::reset()` tears down all journals, returning to a clean state
   between sub-tests.
4. `DataPlane::graceful_stop()` resets, then SIGTERMs children and awaits exit.
   If simply dropped, children are SIGKILL'd.

## Prerequisites

Tests using this crate require `etcd` and `gazette` on PATH
(`~/go/bin/gazette` by default). See `tests/hello_world.rs` for
a working example.
