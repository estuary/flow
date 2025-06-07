# Automations

The `automations` crate provides a task execution framework for Flow's control plane automation jobs. It enables long-running, fault-tolerant background processes that can spawn sub-tasks, communicate via message passing, and persist their state across restarts.

## Core Concepts

### Tasks and Executors

- **Tasks** are persistent units of work identified by a unique `models::Id` and stored in the `internal.tasks` table
- **Executors** implement the `Executor` trait to define the logic for a specific `TaskType`
- Each task has serializable state, an inbox for receiving messages, and a heartbeat for liveness tracking

### Task Lifecycle

1. **Spawn**: Tasks are created with an initial message and assigned to a parent task
2. **Poll**: The executor's `poll()` method is called with the task's current state and inbox
3. **Outcome**: Poll returns an `Outcome` that encapsulates side effects to be applied transactionally
4. **Action**: The outcome produces an `Action` that determines the task's next state (sleep, suspend, spawn children, etc.)
5. **Persist**: State changes and actions are persisted atomically with heartbeat updates

### Message Passing

Tasks communicate through typed messages:
- **Send**: Send a message to any task by ID
- **Yield**: Send a message to the parent task
- **Inbox**: Receive messages from child tasks or external sources

## Key Types

### Executor Trait

```rust
pub trait Executor: Send + Sync + 'static {
    const TASK_TYPE: TaskType;
    type Receive: serde::de::DeserializeOwned + serde::Serialize + Send;
    type State: Default + serde::de::DeserializeOwned + serde::Serialize + Send;
    type Outcome: Outcome;

    fn poll(/* ... */) -> impl Future<Output = anyhow::Result<Self::Outcome>>;
}
```

### Actions

- `Action::Spawn(id, type, message)` - Create a new child task
- `Action::Send(id, message)` - Send message to existing task
- `Action::Yield(message)` - Send message to parent task
- `Action::Sleep(duration)` - Sleep then poll again
- `Action::Suspend` - Wait indefinitely for messages
- `Action::Done` - Complete and remove the task

### Task Types

Registered task types in `task_types` module:
- `DATA_PLANE_CONTROLLER` - Manages data plane infrastructure
- `LIVE_SPEC_CONTROLLER` - Handles live specification updates
- `PUBLICATIONS` - Processes catalog publications
- `DISCOVERS` - Runs connector discovery jobs
- `EVOLUTIONS` - Handles schema evolution tasks
- `APPLIED_DIRECTIVES` - Processes applied directives
- `CONNECTOR_TAGS` - Manages connector tag updates

## Server and Runtime

The `Server` struct:
- Registers multiple executors for different task types
- Dequeues ready tasks from the database using heartbeat-based liveness detection
- Runs tasks concurrently with configurable parallelism limits
- Handles task failures with automatic retry after heartbeat timeout

## Database Integration

Tasks are persisted in `internal.tasks` with:
- `task_id`: Unique identifier
- `task_type`: Maps to registered executor
- `parent_id`: Optional parent for hierarchical tasks
- `inner_state`: Serialized executor state
- `inbox`/`inbox_next`: Message queues
- `wake_at`: When task should next be polled
- `heartbeat`: Liveness timestamp

### SQL Procedures

In addition to `internal.tasks`, SQL migrations define procedures for task management:

**Creating Tasks:**
```sql
SELECT internal.create_task($1, $2, $3);
-- $1: task_id (models::Id)
-- $2: task_type (TaskType)
-- $3: parent_id (models::Id, optional)
```

**Sending Messages:**
```sql
SELECT internal.send_to_task($1, $2, $3::JSON);
-- $1: recipient_task_id (models::Id)
-- $2: sender_task_id (models::Id)
-- $3: message (JSON, NULL for EOF)
```

## Usage

```rust
// Define an executor
struct MyExecutor;

impl Executor for MyExecutor {
    const TASK_TYPE: TaskType = TaskType(42);
    type Receive = MyMessage;
    type State = MyState;
    type Outcome = Action;

    async fn poll(/* ... */) -> anyhow::Result<Action> {
        // Process inbox messages
        // Update state
        // Return action (spawn, send, sleep, etc.)
    }
}

// Register and serve
let server = Server::new().register(MyExecutor);
server.serve(permits, pool, interval, timeout, shutdown).await;
```

## Key Dependencies

- `coroutines` - Async task scheduling and coordination
- `models` - Flow catalog models and ID generation
- `sqlx` - Database operations and JSON serialization
- `tokio` - Async runtime and synchronization primitives

This crate is essential for Flow's control plane automation, enabling reliable execution of long-running operations like publication validation, schema evolution, and data plane management.
