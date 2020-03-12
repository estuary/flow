
Ingestion
----------

* Accumulate documents of a single request? **Yes.** Req EOF is signal to flush, immediately.
* Accumulate across requests? Adds latency... Not sure this can reasonably be overcome.
* Syslog / streams: accepter batches content RPCs of bounded max size, buffers in the mean-time as AppendService does.
    * Focus on efficient HTTP api...


Projections
-------------

- Parsed scalar types (only); bool, []byte, u64, i64, f64, etc.
- Also include binary! (use contentEncoding of projection definition)
- Allows for easy total ordering.
- Represent in Go with slice of tagged union ?

Consumer Flow
----------------

- Read line of []byte
- Project out [UUID, rest of shuffle key...]
    - (No need to validate yet...)
- If UUID is ACK
    - Send to all shards
    - Else, map to shard on shuffle key.

- *Problem* violation of exactly-once if:
    - reader doesn't sequence
    - shard count is changed
    - message is duplicated before and after change

- *Problem* want **reader** to control when a runner starts getting messages on paralellism change.
  Reader can make atomic cut-over for a given journal.


Sketch:
- Suppose M processors advertised w/in Etcd w/ endpoints (use allocator)
- Message parsing: represent raw []byte and skimmed [UUID, shuffle key...].
- Txn:
    - Consume:
        - Map to processor
        - Start a new RPC if needed
        - Dispatch down RPC.
    - FinishTxn:
        - Close / flush all RPCs.
        - Await each before allowing txn to commit.

    reasons this doesn't work:
        - on processor failure, some will have committed and some won't.
        - can't recall published downstream messages?
    what about exactly-once for downstream messages?

Shuffled reader:
    - Parse to raw []byte and skimmed [UUID, shuffle key...]
    - Send ACKs to all current processors.
    - Map on shuffle key to HRW processor having min_clock >= UUID.clock()

    problem: what if

Restated where every shard reads every journal:
    - Read every journal.
    - Parse to raw []byte and skimmed [UUID, shuffle key...]
    - Assemble shards having start_clock >= UUID.clock()
    - Select single shard to which shuffle-key maps.
    - Process message if that's me. Otherwise ignore.
    - Process message if shuffle key maps to me using HRW, and my shard.start_clock >= UUID.clock()