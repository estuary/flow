
master (unreleased)
--------------------

v0.86.1
--------

- Update RocksDB to 6.7.3, and Go to 1.14.2, along with many other package dependencies.
- The ``etcdtest`` package no longer embeds an Etcd server. Instead, an ``etcd``
  binary *must* be available on the PATH and is invoked as a sub-process,
  using Unix domain sockets. Users who use ``etcdtest`` themselves, or are running
  Gazette tests outside of the hermetic Docker build environment, must provide a
  reasonably recent version of ``etcd``.
- Relatedly, the gazette client_ package no longer depends on Etcd (including the Etcd client).
- JSON-framed messages are now able to use custom marshal/demarshal routines.
- ``gazctl journals/shards apply`` now interprets revision ``-1`` (previously
  disallowed) to mean "don't care", allowing specs to be applied which will always
  overwrite what's in Etcd.
  This makes sense for specs managed in another source-of-truth (eg, git).
- Mitigations and improved logging for issue GH-248_.
- Remove explicit TCP keep-alive management, as this is now Go default.
- Various doc improvements & cleanups.

.. _GH-248: GH-248
.. _client: https://godoc.org/go.gazette.dev/core/broker/client

v0.85.2
--------

- Fix: ``as-ci`` target downloads go modules before invoking inner make,
  to allow external repos to include Makefiles of the gazette/core repo
  which are dynamically determined using the ``go mod`` tool.
- Update to Go 1.13.4

v0.85.1 
---------

- Added ``MaxAppendRate`` JournalSpec field and global broker flag.
  Append RPCs now use a token-bucket flow control strategy, where RPC chunks
  are evaluated and potentially throttled or policed against maximum and minimum
  allowed flow-rates.
- Added ``PathPostfixTemplate`` JournalSpec field. Path postfixes are evaluated
  and applied to individual Fragments as they're persisted. A primary use case is
  to support Hive-compatible partitioning of Fragments based on their creation time.
  Journal names and labels may now include the '=' rune, to facilitate the layout of
  multiple journals as a Hive-partitioned table.
- Reworked almost all documentation into reStructuredText / Sphinx / ReadTheDocs format.
- Make-based build system is refactored to make it easier to integrate and reuse
  in external repositories and consumer application projects.
- Add ``DisableWaitForAck`` ShardSpec field, which toggles the consumer transaction
  behavior of waiting for ACKs of read pending messages. Most applications won't want
  to set this, but it can be helpful to avoid stalls in applications with cyclic
  message flows.

v0.84.2
-------

- Add ca-certificates to release images.

v0.84.1
-------

- SQLite is now a supported `consumer store`_!
- Instances of message.Framing may now be dynamically registered. Support for ``text/csv`` is added.
- Added the ``gazctl attach-uuids`` sub-command.
- A bike-share_ example and documentation have been added,
  along with new kustomize_ manifests for deploying existing examples.
- Automated partition crash-tests are re-enabled, after adding
  DaemonSet kustomize manifests to properly support them.
- The ListFragments RPC now properly respects fragment stores which use re-write rules.
- The journals of ShardSpecs are now verified by consumer servers and ``gazctl``, to actually
  exist and have appropriate content-types.
- Extensive Godoc and documentation improvements.
- Various minor logging improvements and bug fixes.

.. _`consumer store`: https://godoc.org/go.gazette.dev/core/consumer/store-sqlite
.. _bike-share: docs/examples_bike_share.md
.. _kustomize: kustomize/test/
.. _Urkel: https://github.com/jgraettinger/urkel

v0.83.2
-------

This release introduces `exactly-once processing semantics`_ to Gazette!

This is a breaking change to many of the ``consumer`` package interfaces, notably Shard, Application and Store, as well as the ``message`` interfaces. Updates to consumer applications will be required.

This release also introduces Kustomize manifests for deploying brokers, consumers, and dependencies. *Helm charts of the repo are deprecated and will be removed in a future release*.

**Rolling Upgrades**

A rolling upgrade from v0.82 => v0.83 is supported and tested, with the following caveats:

- Brokers must be fully migrated to v0.83 before any consumers may be migrated. This is required
  as v0.83 brokers introduce journal "registers" which v0.83 consumers rely on. The v0.83 broker
  is fully compatible with v0.82 consumers.
- v0.83 consumers will migrate the means of storing offsets within RocksDB from now-legacy
  keys/values to new consumer Checkpoints introduced with v0.83.
  **Legacy offsets are not removed, but are also not updated.**
  This means downgrading from v0.83 => v0.82 will re-process portions of source journals read
  by the v0.83 consumer. Similarly, a subsequent re-upgrade from v0.82 => v0.83
  *will not migrate offsets again* (and portions read by the downgraded v0.82 consumer will
  be re-processed).

.. _`exactly-once processing semantics`: https://github.com/gazette/core/blob/master/docs/exactly_once_semantics.md

v0.82.2
-------

Release v0.82.2 is a patch release of the v0.82 branch

It includes fixes cherry-picked from master since v0.82.1 was cut:

- 36a01b6 consumer: fix some spurious shard recovery errors
- ac3a329 broker: add more context cancellation checks for log supression
- 35632e1 broker: proxyAppend should take AppendRequest by value (not reference)
- 4c6fa33 client: RouteCache should account for empty Route
- ef7098e allocator: update some logging
