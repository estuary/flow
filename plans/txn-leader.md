
Lead RPC is colocated with shuffle service

## Startup

* **Join**: Each member sends Requst.Join to current shard-zero assignment. 
  - Includes expected topology, Etcd revision, etc
* **Joined**: Leader waits for all members to join, then broadcasts Response.Joined

* **ResumeCheckpoint** All members stream their resume-checkpoint to leader
  * Leader reduces over checkpoints to determine max-progress frontier across members
  * This is the checkpoint that begins shuffled reads




## Discussion

### Roll-forward / back

A current scaled-out shard can transition to V2 Leaders:
 * Reduction of checkpoints means Leader starts with max journal/producer progress of any shard
And transition back:
 * All shards separately store the checkpoint and connector state in their recovery log

 
 - connector states are connector messages.
 - returned state updates are just message passing -- you can return multiple via newlines
 - the running session sees *all* sent messages individually
 - the runtime reduces all connector messages to a single message, used only at next session open
 - doesn't require a change to open input or state output sites
   - when reducing (only), !merge-patch is expressed as a leading null 
   - we add net-new fields for passing newline messages in

_Join a topology under a Leader_
Join / Joined
  - -> topology
  - -> build ID

_Tell the Leader where to start a shuffle Session_
Resume Checkpoint
  - All members stream Frontier; Leader reduces

_Coordinates starting connectors w/ startup state_
Start / Started (S0 sends Start; Leader broadcasts)
  - -> Open state (from S0)
  - -> map binding => max-key
  - <- Open state (broadcast to all)
  - <- map binding => max-key
  - _connectors are Open'd on Started_

_Extend the current transaction_
loop: Extend / Extended (scatter phase)
  - -> FrontierChunk
  - _derivation: no-op_
  - _materializations: scan and load into Combiner; send Load; read Loaded / Acknowledged_
  - <- combiner_usage_bytes

_Note either Extended or one Acknowledged can be sent to leader_
_client expects only Extended before sending FinishedCommit_

_Fetch states to persist before releasing writes_
Flush / Flushed
  - -> full Acknowledged messages
  - _derivation: no-op_
  - _materializations: await Acknowledged; Flush, then read Loaded and Flushed_
  - <- partial Flushed messages 
  - <- partial map[binding] => max-key update

_Accumulate and persist S0 state before releasing writes_
Persist / Persisted (S0 only)
  - -> full (Flushed or StartedCommit) messages
  - -> full ACK intents
  - -> full map[binding] => max-key update
  - _runtime: Frontier delta plus all Flushed messages written to S0 recovery log w/ max-key optimization_

_Release transaction effects and fetch state updates_
StartCommit / StartedCommit (scatter phase)
  - -> full Flushed messages
  - -> consumer.Checkpoint (legacy)
  - _derivation: scan and send to connector, Flush, read Published then Flushed,
                 send StartCommit, read StartedCommit_
  - _materialization: drain to Store, send StartCommit, read StartedCommit_
  - _capture: take combiner (building in a task), publish it out, and yield its checkpoint entries as StartedCommit messages_
  - <- partial StartedCommit messages
  - <- partial ACK intents
  - <- partial binding stats

Persist / Persisted (aagain; S0 only)

Acknowledge / Acknowledged (scatter phase)
  - -> full StartedCommit messages
  - _capture: send Acknowledge_
  - _materialization: send Acknowledge (does not await Acknowledged)_
  - _derivation: add Acknowledge???_
  - <- partial Acknowledged messages
