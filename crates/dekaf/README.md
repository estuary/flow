# Dekaf

Dekaf serves Estuary collections to Kafka consumers: it speaks enough of the
Kafka wire protocol, and enough of the Confluent Schema Registry HTTP API,
that an off-the-shelf consumer reads a Flow collection as a Kafka topic.

It runs as a data-plane process rather than as a shard. A Dekaf
materialization has no runtime task of its own -- no shards are published for
it -- so Dekaf synthesizes the shard identity it needs (see
`dekaf_shard_template_id`) and asks the control-plane's `/authorize/dekaf` for
everything else: the built `MaterializationSpec`, a control-plane token, and
the names of its ops journals.

The Kafka topic a consumer sees is a collection binding; a topic partition is
one of that collection's journals, in a stable order; and a Kafka offset is
the journal offset of a document's *final* byte. Records are Avro-encoded
against schemas registered in the control plane, which the embedded schema
registry serves.

## Layers

```
main.rs        CLI, listeners, and the process-wide task::Registry
  |
  +- session.rs   One Kafka connection. Dispatches each API request,
  |               owns pending Reads, and proxies group management
  |               to an upstream Kafka broker (api_client.rs).
  |
  +- registry.rs  The schema-registry HTTP surface (axum).
  |
  +- task.rs      The Task model -- Bindings and the Sources they read,
  |               compiled once per build -- and the Handle through
  |               which sessions read its refreshing state.
  |                 |
  |                 +- topology.rs  A Binding assembled for one request
  |                 |               as a Collection: the topic's partitions.
  |                 +- read.rs      One journal read, transcoded into
  |                                 Kafka RecordBatches.
  |
  +- log_appender.rs  Session logs and stats, appended to ops journals.
     logging.rs       Routes a session's tracing output to that appender.
```

## The Task model

`task.rs` compiles a built `MaterializationSpec` into a `Task` of `Binding`s
and `Source`s, following the taxonomy of the `runtime-next` and `shuffle`
crates: a Binding holds what's derived from a materialization binding (its
topic, partition selector, leader epoch, Avro value schema and extractors),
a Source holds what's derived from the collection it reads (partition
template, key pointers, Avro key schema), and bindings reference their Source
by index. Sources group on the spec's declared `collection_index`, never on
collection name. Compilation happens once per build, not once per request,
and a spec which fails to compile is `InvalidArgument` for that build.

## Refresh, and how staleness is noticed

Everything a session needs is held by [`task::Handle`] as a tree of `tokens`
Watches, and a session reads the current value at each access rather than
being handed a snapshot. `task.rs` documents the tree; the two properties
worth knowing here:

**The endpoint config is resolved on the build ID, not on a timer.** The
sealed config changes only through a publication, which moves the build, so
there's nothing else to key on. The upside is that an established session is
never exposed to a transient failure of `sops`.

**A session which sees something stale asks for a re-fetch.** An unknown
topic, a rejected password, a journal that has vanished, a rejected
control-plane token, a document which fails schema validation -- each of
these is explained by a spec we haven't seen yet, so the session cancels the
authorization's revocation handle and the watch re-fetches.
A 20-second cool-off inside `TaskDekafAuth` bounds what many sessions can ask for.
`--spec-ttl` upper-bounds the cadence for changes
which produce no error at all, such as a removed binding.

## Non-obvious details

- **Offsets address a document's last byte**, so a fetch offset may land
  mid-document. `read.rs` probes the preceding byte for a newline and reads
  back up to 64MB only when it must. See `OFFSET_READBACK`.
- **A binding's journals are listed on demand.** The Task names every
  binding, but a Source's client costs a refreshing `/authorize/task` and a
  Binding's listing a held broker list-watch, so `task::Topic` starts each
  on the first request for its topic and keeps them for the life of the task.
  This is a deliberate difference from `shuffle`, whose shards read every
  binding and so list eagerly.
- **Partition order must be stable**: journals are ordered by
  `(create_revision, name)`, and suspended journals are *kept* in the listing
  -- dropping one would renumber every partition after it.
- **The leader epoch is the binding's backfill counter, plus one.** Consumers
  don't run their truncation-detection logic on a 0 -> 1 transition, so the
  counter starts at 1. It's also mixed into upstream topic names, which
  isolates a backfilled binding's group state.
- **Topic names are encrypted before reaching the upstream broker** used for
  group management, so that customer collection names don't leave the
  data-plane. The encryption is deterministic (AES-SIV with a fixed nonce)
  because the same topic must map to the same upstream name every time.
- **A migrated task redirects rather than serving.** `/authorize/dekaf`
  answers with the target data-plane's addresses, which Metadata and
  FindCoordinator advertise as the only broker, and which the schema registry
  proxies to directly. The redirect is served *before* the password is
  checked: a redirected task's config is never resolved here, so this plane
  has nothing to compare against. A client
  which names an existing task therefore learns where it moved to, and its
  collection names, without authenticating -- accepted, because
  `dekaf.estuary.dev` exists to serve exactly this discovery, and because the
  data itself stays behind the target plane's own authentication.
- **`strict_topic_names` and `deletions` are read from the sealed config**, as
  plaintext, with no `sops` decryption and no overlay. Only `/token` is
  `secret: true` in the config schema, so nothing else is ever encrypted --
  and that's what lets a redirect reproduce the Metadata a client would see
  from the target plane. A config which encrypts one of them fails to
  authorize with `InvalidArgument`, in its home plane too.

## Tests

`tests/e2e/` drives a real consumer against a local two-plane stack; run it
with `mise run ci:dekaf-e2e-run`. `tests/field_extraction_tests.rs` is a
snapshot test of Avro schema and extractor derivation, and needs no stack.
