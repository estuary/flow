# Allocator

Distributed algorithm for assigning a set of "Items" across a pool of "Members" with replication and zone constraints.

## What it does

The allocator implements a distributed, fault-tolerant system for assigning Items to Members across multiple availability zones. Each Member runs an instance of the Allocator which coordinates through Etcd to:

- Assign Items to Members respecting desired replication counts
- Distribute replicas across failure zones for high availability  
- Balance load evenly across Members within capacity limits
- Maintain consistency constraints during reassignments
- Perform minimal incremental updates when topology changes

## How it fits into Gazette

The allocator is used by both the broker and consumer frameworks:

- **Brokers**: Assigns journal shards to broker instances across zones
- **Consumers**: Assigns consumer shards to consumer instances across zones

It provides the core scheduling intelligence that keeps Gazette services highly available and load-balanced.

## Essential Types

### Core Entities
- `Item`: Something to be assigned (journal, shard, etc.) with desired replication
- `Member`: An instance that can be assigned Items, with capacity limits  
- `Assignment`: A specific Item-to-Member assignment with slot ordering
- `State`: Extracted view of current allocation topology from Etcd KeySpace

### Key Interfaces
- `ItemValue`: User-defined Item with `DesiredReplication() int`
- `MemberValue`: User-defined Member with `ItemLimit() int` 
- `IsConsistentFn`: Determines if Item replicas are synchronized enough to allow reassignment
- `Decoder`: Decodes Etcd values into user-defined representations

### Flow Network
- `sparseFlowNetwork`: Models allocation as maximum flow problem
- Uses `sparse_push_relabel` algorithm to solve optimal assignments

## Brief Architecture

The allocator uses a **leader-follower** model where one Member acts as leader and makes allocation decisions:

1. **State Observation**: All Members observe shared Etcd KeySpace containing Items, Members, and current Assignments

2. **Leadership Election**: Leader is the Member with lowest `(CreateRevision, Key)` tuple

3. **Flow Network Solving**: Leader models allocation as maximum flow network and solves for optimal assignments using push/relabel algorithm

4. **Incremental Convergence**: Leader applies minimal changes through batched Etcd transactions, respecting consistency constraints

5. **Session Management**: Members announce themselves with Etcd leases and gracefully shutdown by zeroing their ItemLimit

The algorithm prioritizes:
- **Stability**: Minimal reassignments when topology changes
- **Balance**: Even distribution across Members and zones  
- **Availability**: Maintains replication during reassignments
- **Performance**: Incremental updates scale linearly with Items