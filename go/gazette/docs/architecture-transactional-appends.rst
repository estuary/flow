Transactional Appends
======================

Overview
--------

Journal append RPCs have several requirements:

* An append that reports success must be durable to the configured replication
  factor and availability zones.
* Appends may be of arbitrary size, which may not be known ahead of time.
* Appends are all-or-nothing, and are frequently aborted mid-RPC
  (either explicitly by the client, or implicitly due to stream closure or timeout).
* A span of journal bytes assigned to a completed append must never again be
  re-assigned (i.e. a given journal offset is never written twice).
* Append RPCs may provide several expectations which must be verified before
  the append may proceed (much like a check-and-set).
* Append RPCs may be observed by other journal readers only after the append commits.
* Latency and round-trips of the replication protocol must be minimized.

Under the hood, many broker components work together to provide these guarantees.

Components 
----------

Etcd is the sole source of truth for a journal's current topology.
Every broker maintains a local KeySpace_ which mirrors keys & values of Etcd,
updated by a long-lived Etcd Watch. At times a broker learns of a future Etcd revision from a peer,
in which case the broker must WaitForRevision_ of the KeySpace before proceeding.

.. _KeySpace: https://godoc.org/go.gazette.dev/core/keyspace
.. _WaitForRevision: https://godoc.org/go.gazette.dev/core/keyspace#KeySpace.WaitForRevision
.. _Route:   https://godoc.org/go.gazette.dev/core/broker/protocol#Route
.. _resolver: https://github.com/gazette/core/blob/master/broker/resolver.go#L16
.. _allocator: https://godoc.org/go.gazette.dev/core/allocator
.. _Fragment: https://godoc.org/go.gazette.dev/core/broker/protocol#Fragment
.. _Spool:  https://godoc.org/go.gazette.dev/core/broker/fragment#Spool
.. _AppendRequest: https://godoc.org/go.gazette.dev/core/broker/protocol#AppendRequest
.. _ReplicateRequest: https://godoc.org/go.gazette.dev/core/broker/protocol#ReplicateRequest
.. _pipeline: https://github.com/gazette/core/blob/master/broker/pipeline.go
.. _append_fsm: https://github.com/gazette/core/blob/master/broker/append_fsm.go
.. _protocol pipelining: https://en.wikipedia.org/wiki/Protocol_pipelining

A current set of journal assignments in Etcd define its Route_ topology,
which captures the replica peer set and current primary of the journal.
Topology disagreements can always be resolved by comparing the associated
Etcd revision, which provides a total ordering over Route changes. A local
resolver_ layers upon the KeySpace to provide mappings between a journal and
its current Route and effective Etcd revision. 

Brokers coordinate through a distributed allocator_ to keep journal assignments
up to date. The allocator will not allow assignments to be removed if the current
Route_ is not consistent or if the journal would drop below the configured replication factor.
An implication is that (unless **N>=R** faults occur) there is always at least
one broker of a given Route_ that has participated in a previous journal
transaction.

Every span of journal content is defined by a content-addressed Fragment_
and each replica of a journal maintains a Spool_,
which is the replica's transactional "memory" of the journal. The Spool_
maintains a Fragment_ currently being built, as well was a set of
key/value pairs known as journal "registers" which may be operated on by an AppendRequest_.
At any given time, just one Append or Replicate RPC (or equivalently: goroutine)
"owns" the replica Spool, coordinated through Go channels. Other RPCs desiring
the Spool must block.

Spools are mutated by applying a stream of ReplicateRequest_,
where each request either proposes a chunk of content to be appended,
or proposes that a specific Fragment be adopted. A commit is conveyed by a
Fragment proposal that expands to cover content chunks proposed in earlier
requests, while a roll-back is a re-send of a prior adopted Fragment.

A Spool can be thought of as its own little state machine, with transitions
driven by ReplicateRequests. At each broker of the Route, a Spool instance
independently validates every transition. Content chunks are SHA1-summed and
offsets are tracked. Proposed Fragments are compared to a Fragment computed
from the prior adopted Fragment and subsequent content chunks. Any mismatch
is an invalid transition which aborts the replication stream.

Spools additionally support a callback observer interface, which is invoked
when a Fragment proposal is adopted. This callback is the means by which
ongoing read RPCs at each replica are made aware of new journal content.

Finally, a replication pipeline_ run by the journal's primary broker
manages a set of Replicate RPCs to each of the peers identified by a journal
Route_. Replicate RPCs are bi-directional request/response streams, with a
long lifetime that spans across many individual AppendRequest_ RPCs. The pipeline and
its replication streams amortizes much of the setup and synchronization cost
of the transaction protocol. Typically, a pipeline_ is torn down and restarted
only when necessary, i.e. because the journal Route has changed. A constructed
pipeline also has ownership over the replica's Spool_, and like the Spool, at a
given time just one Append RPC / goroutine "owns" the pipeline (and all others
must block). Pipeline ownership is further distinguished on "send" vs "receive"
ends, which allows a pipeline to be operated in full duplex mode, with multiple
ordered append RPCs in-flight to replicas concurrently (as in `protocol pipelining`_).


Append State Machine
--------------------

Every Append RPC traverses through an append_fsm_ state machine.
This section describes the states and transitions of that machine.
Transitions to **stateError** are omitted for brevity: for example,
all blocking FSM states monitor the request Context, and will abort the
FSM appropriately.

stateResolve
^^^^^^^^^^^^^^^^^^^^^^^^^
    Performs resolution (or re-resolution) of the AppendRequest. If
    the request specifies a future Etcd revision, first block until that
    revision has been applied to the local KeySpace. This state may be
    re-entered multiple times.

    - Transition to **stateAwaitDesiredReplicas** if the local broker is not the current primary.
    - Transition to **stateStartPipeline** if the FSM already owns the pipeline.
    - Common case: transition to **stateAcquirePipeline**.

stateAcquirePipeline
^^^^^^^^^^^^^^^^^^^^^^^^^
    Performs a blocking acquisition of the exclusively-owned replica pipeline.

    - Transition to **stateResolve** if the prior resolution is invalidated while waiting.
    - Common case: The pipeline is now owned. Transition to **stateStartPipeline**.
 
stateStartPipeline
^^^^^^^^^^^^^^^^^^^^^^^^^
    Builds a pipeline by acquiring the exclusively-owned replica Spool,
    and then constructing a new pipeline (which starts Replicate RPCs to each Route peer).
    If the current pipeline is in an initialized state but has an older effective
    Route, it's torn down and a new one started. If the current pipeline Route is correct,
    this state is a no-op.

    - Common case: The pipeline is already in a good state, with an effective Route that
      matches the FSM's resolution. This state is an effective no-op.
      Transition to **stateUpdateAssignments**.
    - Transition to **stateResolve** if the resolved Route is invalidated while awaiting the Spool.
    - Otherwise the pipeline is uninitialized or at an older Route and must be built.
      Transition to **stateSendPipelineSync**.

stateSendPipelineSync
^^^^^^^^^^^^^^^^^^^^^^^^^
    Sends a synchronizing ReplicateRequest proposal to all
    replication peers, which includes the current Route, effective Etcd
    revision, and the proposed current Fragment to be extended. Each peer
    verifies the proposal and headers, and may either agree or indicate a conflict.

    - Transition to **stateRecvPipelineSync**.

stateRecvPipelineSync
^^^^^^^^^^^^^^^^^^^^^^^^^
    Reads synchronization acknowledgements from all replication peers.

    - Transition to **stateResolve** if any peer is aware of a non-equivalent Route at
      a later Etcd revision.
    - Transition to **stateSendPipelineSync** if any peer is aware of a larger
      journal append offset, or is unable to continue a current Fragment, in
      which case the current Fragment is closed & persisted and a new Fragment
      is begun at the end offset of the old. An implication is that a current
      Fragment is always closed and persisted when a new broker joins the topology.
    - Transition to **stateUpdateAssignments** if all peers agree with the proposal,
      indicating the pipeline is now synchronized.

stateUpdateAssignments
^^^^^^^^^^^^^^^^^^^^^^^^^
    Verifies and, if required, updates Etcd assignments to
    advertise the consistency of the present Route, which has been now been
    synchronized. Etcd assignment consistency advertises to the allocator that
    all replicas are consistent, and allows it to now remove undesired journal
    assignments.

    - Common case: Values reflect current Route and this state is a no-op.
      Transition to **stateAwaitDesiredReplicas**.
    - Otherwise, effect Etcd value updates to reflect the present Route.
      Transition to **stateResolve** at the Etcd operation revision.

stateAwaitDesiredReplicas
^^^^^^^^^^^^^^^^^^^^^^^^^
    Ensures the Route has the desired number of journal replicas.
    If there are too many, then the allocator has over-subscribed the
    journal in preparation for removing some of the current members -- possibly
    even the primary. It's expected that the allocator's removal of member(s) is
    imminent, and we should wait for the route to update rather than sending this
    append to N > R members (if primary) or to an old primary (if proxying).

    - If there are too many replicas, transition to **stateResolve** at the next
      unread Etcd revision.
    - If there are too few, transition to **stateError** (INSUFFICIENT_JOURNAL_BROKERS).
    - If we are not the local primary, transition to **stateProxy**, indicating the
      RPC must proxy to the indicated primary peer.
    - Common case: The Route has the proper number of replicas.
      Transition to **stateValidatePreconditions**.

stateValidatePreconditions
^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Validates preconditions of the request. It ensures that current registers match
    the request's expectation, and if not it will fail the RPC with status ``REGISTER_MISMATCH``.
    
    It also validates the next offset to be written.
    Appended data must always be written at the furthest known journal extent.
    Usually this will be the offset of the pipeline's Spool. However if journal
    consistency was lost (due to too many broker or Etcd failures), a larger
    offset could exist in the fragment index.

    We don't attempt to automatically handle this scenario. There may be other
    brokers that were partitioned from Etcd, but which still have local
    fragments not yet persisted to the store. If we were to attempt automatic
    recovery, we risk double-writing an offset already committed by those brokers.

    Instead the operator is required to craft an AppendRequest which explicitly
    captures the new, maximum journal offset to use, as a confirmation that all
    previous brokers have exited or failed (see ``gazctl journals reset-head --help``).

    We do make an exception if the journal is not writable, in which case
    appendFSM can be used only for issuing zero-byte transaction barriers
    and there's no risk of double-writes to offsets. In particular this
    carve-out allows a journal to be a read-only view of a fragment store
    being written to by a separate & disconnected gazette cluster.
    
    Note that an AppendRequest offset may also be used outside of recovery,
    for example to implement at-most-once writes.

    - Transition to **stateResolve** if the Route changes while awaiting
      an initial fragment index refresh.
    - Transition to **stateError** if the request registers or offset don't match
      the request's expectation.
    - Common case: All precondition checks are successful.
      Transition to **stateStreamContent**.
 
stateStreamContent
^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Called with each received content message or error
    from the Append RPC client. On its first call, it may "roll" the present
    Fragment to a new and empty Fragment (for example, if the Fragment is
    at its target length, or if the compression codec changed). Each non-empty
    content chunk is forwarded to all peers of the FSM's pipeline. An error
    of the client causes a roll-back to be sent to all peers. A final empty
    content chunk followed by an io.EOF causes a commit proposal to be sent
    to each peer, which (if adopted) extends the current Fragment with the
    client's appended content.

    - Transitions to itself with every non-empty client content chunk.
    - Transitions to **stateReadAcknowledgements** after sending a commit proposal or rollback.

stateReadAcknowledgements
^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Releases ownership of the pipeline's send-side,
    enqueues itself for the pipeline's receive-side, and, upon its turn,
    reads responses from each replication peer.
    
    Recall that pipelines are full-duplex, and there may be other FSMs
    which completed stateStreamContent before we did, and which have not yet read
    their acknowledgements from peers. To account for this, a cooperative pipeline
    "barrier" is installed which is signaled upon our turn to read ordered
    peer acknowledgements, and which we in turn then signal having done so.

    - Transition to **stateFinished** once all peers acknowledge.

stateError
^^^^^^^^^^^^^^^^^^^^^^^^^^^
    *Terminal state* reached when an FSM transition fails.

stateProxy
^^^^^^^^^^^^^^^^^^^^^^^^^^^
    *Terminal state* reached when the FSM has resolved the append
    to a ready remote broker, to which the RPC is proxied.

stateFinished
^^^^^^^^^^^^^^^^^^^^^^^^^^^
    *Terminal state* reached when the append has fully committed.

Discussion
----------

*A synchronized pipeline held by an FSM is a distributed and exclusive lock over
the capability to append to a journal*. That's because:

 * An invoked Replicate RPC obtains exclusive access to the replica Spool.
 * While holding that lock, the RPC waits for the primary to close the stream (or fail).
   Other RPCs of the replica will block obtaining the Spool, until the primary does so.
 * The allocator will never voluntarily remove assignments which are not synchronized.
 
Completing synchronization of a pipeline is thus a confirmation that there are no
other ongoing mutating RPCs of the journal, nor can there be (unless consistency is lost).
Regular and proactive synchronization is important for cluster health: for one,
brokers cannot enter and leave a topology without first synchronizing.
As client Append RPCs may not arrive with enough regularity to drive this activity,
the journal's primary broker employs a "pulse" daemon_ to regularly and
proactively synchronize the pipeline.

*Append RPCs require just one internal round-trip* if the common case is taken on all
state transitions. This round-trip awaits an explicit acknowledgement from each
replica that the commit proposal was accepted. While an RPC waits, many other RPCs
may be evaluated concurrently as the pipeline is full-duplex. If a replication
pipeline must be re-built, more round trips are of course required.

*Append RPCs don't buffer client content*. Even very large appends
have minimal memory impacts on brokers, but this does mean that multiple queued
appends can exhibit head-of-line blocking while awaiting the exclusive replication
pipeline. For this reason, Append RPCs imposes a minimum flow rate on the client's
delivery of streamed chunks. RPCs of clients unable to sustain that flow rate are
aborted to protect overall quality of service. This policing is generous, but if
operating over very lossy networks or with untrusted clients, it likely makes
sense to have a buffering proxy that's closer to the broker.

*Sometimes sh!t happens*. A cross-zone outage occurs, Etcd quorum is lost,
or a bug / bad deploy is encountered. When journal consistency is lost Gazette is
designed to fail to safety, by first and foremost avoiding data loss. As an
operator, you'll experience this as many logged ``INDEX_HAS_GREATER_OFFSET``
errors, which indicate the broker's uncertainty in the face of discrepant offsets.
You'll want to diagnose the underlying fault, and then explicitly "tell" the cluster
that it's safe to accept appends again by using ``gazctl journals reset-head``.
In the meantime clients will need to buffer. They generally should be anyway,
in order to batch many smaller appends into fewer larger ones, and ideally
to a local disk as is done by AppendService_.

.. _daemon:        https://github.com/gazette/core/blob/9bba003c2bc8f096fbd2c95ffc60ed51ab58aa30/broker/replica.go#L94
.. _AppendService: https://godoc.org/go.gazette.dev/core/broker/client#AppendService
