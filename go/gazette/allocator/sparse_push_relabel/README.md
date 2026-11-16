# Sparse Push/Relabel

Greedy variant of the push/relabel maximum flow algorithm with virtual graph support for dynamic arc control.

## What it does

Implements a maximum flow solver that finds optimal flow through a network while providing mechanisms to influence the solution towards desired outcomes. Unlike standard push/relabel, this variant:

- Supports **virtual graphs** where arcs and capacities can be dynamically controlled
- Uses **arc presentation order** to coerce solutions towards previous/preferred states  
- Implements **height-based node prioritization** with discharge operations
- Provides **sparse network optimization** for networks with low arc utilization

## How it fits into the allocator

The allocator models assignment problems as flow networks and uses this solver to find optimal assignments:

- **Source** → **Items**: Desired replication capacity
- **Items** → **Zone-Items**: Zone distribution preferences  
- **Zone-Items** → **Members**: Current assignment preferences
- **Members** → **Sink**: Member capacity limits with fair-share scaling

The solver's ability to follow a "garden path" of preferred arcs allows the allocator to minimize reassignments while still achieving globally optimal load balancing.

## Essential Types

### Network Interface
- `Network`: Defines the flow network with `Nodes()`, `InitialHeight()`, and `Arcs()`
- `Arc`: Directed edge with capacity and presentation hints (`PushFront`)
- `NodeID`: Node identifier (Source=0, Sink=1, others≥2)

### Algorithm State  
- `MaxFlow`: Complete solver state including flows, node heights, and active node queue
- `Flow`: Utilized network edge with current flow rate
- `Height`: Node distance estimate from sink (shorter = higher priority)

### Virtual Graph Support
- `PageToken`: Enables paged arc enumeration for large/dynamic networks
- `PushFront`: Arc hint to prioritize certain flows (LIFO order in residual examination)

## Brief Architecture

The algorithm follows **push/relabel** principles with optimizations:

1. **Initialization**: Set source height to number of nodes, others to distance-from-sink estimates

2. **Discharge Loop**: While nodes have excess flow:
   - Pop highest-height active node  
   - Try to push flow along admissible arcs (current node height > target node height)
   - If no pushes possible, relabel node to minimum height allowing future pushes

3. **Virtual Graph Features**:
   - **Paged Arcs**: Networks can provide arcs in multiple pages, enabling dynamic capacity adjustments
   - **Arc Prioritization**: `PushFront` arcs examined last in LIFO order, expressing preferences
   - **Height-Based Capacity**: Arc capacities can depend on relative node heights (pressure)

4. **Gap Heuristic**: When relabeling creates height gaps, immediately relabel disconnected nodes to prevent unnecessary work

The solver guarantees finding maximum flow while allowing networks to guide the solution towards preferred configurations through strategic arc presentation and capacity tuning.