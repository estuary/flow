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
  It also `signal`s every broker (SIGSTOP/SIGCONT simulate an outage).

## Journal helpers

`journals.rs` holds the operations which are particular to a test, as free
functions over a `gazette::journal::Client` — as `reset_journals` is — so a
fixture which holds a pre-cloned client can use them without borrowing its
`DataPlane` across an await point:

- `probe` — a journal's registers and write head, read with a zero-byte append.
  Its `Suspend` mode decides what the probe does to suspension: `Resume` wakes a
  suspended journal as any append does, while a mode which suspends reports that
  it took effect by answering `None`.
- `head` — the broker-confirmed write head, which bounds a read.
- `register` — the value of one of a journal's registers, such as the `author` a
  fence installs.
- `create` — one journal, unless a journal of that name exists.
- `update` — a read-modify-write of one journal's spec, conditioned on the
  revision it listed.

Listing one journal is `Client::get_journal` of the `gazette` crate. Creating and
updating one are its raw `Client::apply`, conditioned on an expected revision,
which `create` and `update` here wrap for tests alone — an activation and the
publisher build that same request inline.

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
