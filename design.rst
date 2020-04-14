
:flow-consumer: Go consumer.Application which drives the activities of each Shard.

:shuffle-svc: Stateless Rust service which performs merged reads of
    journals under a specific shuffle configuration, and proxies raw messages
    back to flow consumers.

:derive-svc: Processes transactional streams of source collection messages.
    Splits on source collection and proxies to corresponding lambda invocations.
    Provides a stateful K/V callback API with each stream.


Implementation Notes:
########################

* Each flow-consumer *member* spawns a shuffle-svc process.
* Each flow-consumer *primary shard* spawns a derive-svc process.
* Sub-processes can bind a UDS / port / both and emit it's address(s) to stdout.
    Parent spawns subprocess, blocks to read that path, then connects.

* shuffle-svc can be Go for now? Using existing Rust shuffle bindings?

* derive-svc:
    - Extract Recorder description and forward over stdin to subprocess.
    - Model subprocess as consumer.Store
    - Move BuildHints into Store interface, out of Recorder.
    - Shard no longer maintains a Recorder reference (allow it to exist entirely within the subprocess).

* *For now*, derive-svc has Go host that creates Recorder, store_sqlite / store_rocksdb instances,
  and hands off to Rust event loop. 

Transaction protocol:
    1) First ConsumeMessage of consumer transaction starts a /txn/phase1 stream to derive-svc.
        - It provides a base sequencing UUID which derive-svc can increment to sequence messages.
        - It starts reading immediately, publishing pending messages to brokers.
    2) derive-svc (NOT flow-consumer) ENSURES STATE is idle, SETS STATE to phase1,
        and starts / manages transactions in underlying stateful stores.
        It BEGINs a SQLite transaction, and prepares a K/V WriteBatchWithIndex.
    3) derive-svc pumps messages through lambdas, aggregates derived messages to publish,
        and streams back to flow-consumer.
    4) consumer.FinalizeTxn closes send side and waits to be signaled.
    5) derive-svc reads close, flushes any unsent accumulated derived aggregates in response, closes,
       and sets state to PHASE1_DONE.
    6) publish-pending read loop (begun with txn start) reads final pending messages to publish,
        then stream close, and itself signals FinalizeTxn & exits.
    7) FinalizeTxn is signaled and returns.
    8) Store.StartCommit is called with pending-publish |waitFor| future & checkpoint.
       - It POSTs to /txn/phase2 with the checkpoint & reads response.
       - It then go func()'s a task which blocks on |waitFor|, POSTS to /txn/phase3,
          then resolves the future returned by StartCommit with its response.
    9) On /txn/phase2, derive-svc VERIFIES next-state is "phase2" and a prior commit barrier has been cleaned up.
       - It begins an atomic Recorder block with a "waitFor" future which remains blocked.
       - It issues SQLite COMMIT / applies WriteBatchWithIndex / file writes / whatever else.
       - It captures atomic block barrier & releases it.
       - It sets state to "idle".
    10) On /txn/commit, VERIFY that a prepared commit future and barrier exist.
       - Signal the "waitFor" future created in /txn/prepare.
       - Wait for the barrier to resolve
       - Take both future & barrier.
       - Respond with barrier status.


    Gazette TODO:
     - Refactor Recorder to exclusively be held by Store.
       Move BuildHints into Store interface.
     - Refactor Recorder to hold an AsyncAppend open. Internal txnBegin becomes BeginAtomic ?.


     - Recorder bindings for:
        - Beginning an atomic batch, taking an arbitrary OpFuture we can signal from Rust.
        - Ending an atomic batch, obtaining its AsyncAppend as a future we can *read* from Rust.