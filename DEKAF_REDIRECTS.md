# Dekaf Redirects Design

## Problem Statement

By design, a Dekaf instance can only serve materializations hosted in the same dataplane that it's running in. When a Dekaf materialization is migrated to a new dataplane, existing Kafka consumers break because they continue connecting to the old dataplane's Dekaf instance, which no longer has access to the migrated task.

## Solution Overview

Implement a "redirect" mechanism in Dekaf that detects when a task has been migrated to a different dataplane and seamlessly redirects Kafka consumers to the correct Dekaf instance by advertising the target dataplane's broker address instead of its own.

## Key Components

### 1. Redirect Detection Logic

**Location**: `crates/dekaf/src/session.rs` in the `metadata()` and `find_coordinator()` methods

**Implementation**:

- During task authentication, check if the task's `data_plane_id` matches the current Dekaf instance's dataplane
- If different, determine the target dataplane's hostname using the pattern: `dekaf.{data_plane_fqdn}:{kafka_port}`
- Set a `redirected` flag on the session to track redirect state

### 2. Modified Broker Advertisement

**Current Behavior** (`session.rs:229-233`):

```rust
let brokers = vec![MetadataResponseBroker::default()
    .with_node_id(messages::BrokerId(1))
    .with_host(StrBytes::from_string(self.app.advertise_host.clone()))
    .with_port(self.app.advertise_kafka_port as i32)];
```

**New Behavior**:

```rust
let (broker_host, broker_port) = if let Some(target_dataplane) = self.get_target_dataplane().await? {
    self.redirected = true;
    (format!("dekaf.{}", target_dataplane.data_plane_fqdn), target_dataplane.kafka_port)
} else {
    (self.app.advertise_host.clone(), self.app.advertise_kafka_port)
};

let brokers = vec![MetadataResponseBroker::default()
    .with_node_id(messages::BrokerId(1))
    .with_host(StrBytes::from_string(broker_host))
    .with_port(broker_port as i32)];
```

### 3. Session State Management

**Add to Session struct**:

```rust
pub struct Session {
    // ... existing fields
    redirected: bool,
}
```

**Session Creation**:

```rust
impl Session {
    pub fn new(/* ... existing params */) -> Self {
        Self {
            // ... existing fields
            redirected: false,
        }
    }
}
```

### 4. Authorization Logic Modification for Cross-Dataplane Requests

**Problem**: The current authorization logic fails when a task has been migrated to a different dataplane because:

```rust
// This filter in authorize_dekaf.rs:154 will fail for migrated tasks
let Some(task) = snapshot
    .task_by_catalog_name(&task_name)
    .filter(|task| task.data_plane_id == task_data_plane.control_id)
```

**Solution**: Modify the authorization logic to detect cross-dataplane scenarios and return redirect information instead of failing.

**Enhanced DekafAuthResponse**:

```rust
// In models/authorizations.rs
pub struct DekafAuthResponse {
    pub token: String,
    pub ops_logs_journal: String,
    pub ops_stats_journal: String,
    pub task_spec: Option<models::RawValue>,
    pub retry_millis: u64,
    // Add new field for redirect information
    pub redirect_dataplane_fqdn: Option<String>,
}
```

**Modified Authorization Logic**:

```rust
// In authorize_dekaf.rs
fn evaluate_authorization(
    snapshot: &Snapshot,
    task_name: &str,
    shard_data_plane_fqdn: &str,
    token: &str,
) -> Result<(Option<chrono::DateTime<chrono::Utc>>, (String, String, Option<String>)), crate::api::ApiError> {
    // ... existing token verification logic ...

    // First, try to find task in the requesting dataplane (normal case)
    if let Some(task) = snapshot
        .task_by_catalog_name(&task_name)
        .filter(|task| task.data_plane_id == task_data_plane.control_id)
    {
        // Normal case: task is in requesting dataplane
        // ... existing validation and ops journal logic ...
        return Ok((
            snapshot.cordon_at(&task.task_name, task_data_plane),
            (ops_logs_journal, ops_stats_journal, None), // No redirect needed
        ));
    }

    // Task not found in requesting dataplane - check if it exists elsewhere
    if let Some(task) = snapshot.task_by_catalog_name(&task_name) {
        // Task exists but in different dataplane - return redirect info
        if task.spec_type != CatalogType::Materialization {
            return Err(anyhow::anyhow!(
                "task {task_name} must be a materialization, but is {:?} instead",
                task.spec_type
            )
            .with_status(StatusCode::PRECONDITION_FAILED));
        }

        // Find the target dataplane
        let target_dataplane = snapshot.data_planes.iter()
            .find(|dp| dp.control_id == task.data_plane_id)
            .ok_or_else(|| anyhow::anyhow!(
                "target dataplane for task {task_name} not found"
            ))?;

        // Return redirect information
        return Ok((
            None, // No expiration for redirect responses
            (String::new(), String::new(), Some(target_dataplane.data_plane_fqdn.clone()))
        ));
    }

    // Task not found anywhere
    Err(anyhow::anyhow!(
        "task {task_name} not found in any dataplane"
    )
    .with_status(StatusCode::PRECONDITION_FAILED))
}
```

**Updated Response Handling**:

```rust
// In authorize_dekaf.rs, modify the response construction
match evaluate_authorization(snapshot, task_name, shard_data_plane_fqdn, &token) {
    Ok((exp, (ops_logs_journal, ops_stats_journal, redirect_fqdn))) => {
        if let Some(redirect_fqdn) = redirect_fqdn {
            // Return redirect response
            Ok(axum::Json(Response {
                token: String::new(),
                ops_logs_journal: String::new(),
                ops_stats_journal: String::new(),
                task_spec: None,
                retry_millis: 0,
                redirect_dataplane_fqdn: Some(redirect_fqdn),
            }))
        } else {
            // Normal authorization response
            // ... existing token generation and response logic ...
        }
    }
    // ... existing error handling ...
}
```

### 5. Integration Points

**Session Authentication Flow with Redirect Detection**:

1. Client connects and authenticates with task credentials
2. Dekaf calls `/authorize/dekaf` endpoint with task token
3. Authorization endpoint checks if task is in different dataplane
4. If redirect needed, returns `redirect_dataplane_fqdn` instead of authorization
5. Dekaf stores redirect target and sets `redirected = true` on session

**Metadata Request Flow**:

1. Client sends `MetadataRequest`
2. If session has redirect target, advertise target dataplane's broker address
3. Otherwise, advertise current dataplane's broker address
4. Return appropriate broker information to client

**FindCoordinator Request Flow**:

1. Client sends `FindCoordinatorRequest`
2. If session has redirect target, return target dataplane's coordinator address
3. Otherwise, return current dataplane's coordinator address

## Implementation Details

### File Changes Required

1. **`crates/models/src/authorizations.rs`**:

   - Add `redirect_dataplane_fqdn: Option<String>` field to `DekafAuthResponse`

2. **`crates/agent/src/api/authorize_dekaf.rs`**:

   - Modify `evaluate_authorization()` function to handle cross-dataplane tasks
   - Update response construction to include redirect information
   - Change return type to include redirect FQDN

3. **`crates/dekaf/src/session.rs`**:

   - Add `redirected: bool` and `redirect_target_fqdn: Option<String>` fields to `Session` struct
   - Modify `metadata()` method to advertise redirect target when present
   - Modify `find_coordinator()` method to return redirect target when present

4. **`crates/dekaf/src/lib.rs`**:
   - Update authentication flow to handle redirect responses from `/authorize/dekaf`
   - Store redirect target in session when returned by authorization endpoint

### Key Methods to Implement

```rust
impl Session {
    // Updated Session struct
    pub struct Session {
        // ... existing fields
        redirected: bool,
        redirect_target_fqdn: Option<String>,
    }

    // Modified broker advertisement logic
    fn get_broker_address(&self) -> (String, u16) {
        if let Some(ref target_fqdn) = self.redirect_target_fqdn {
            (format!("dekaf.{}", target_fqdn), self.app.advertise_kafka_port)
        } else {
            (self.app.advertise_host.clone(), self.app.advertise_kafka_port)
        }
    }
}

// Updated authentication flow in lib.rs
impl App {
    async fn authenticate(&self, authcid: &str, password: &str) -> Result<SessionAuthentication, DekafError> {
        // ... existing token creation logic ...

        let response = self.client_base
            .call_api::<models::authorizations::DekafAuthResponse>("/authorize/dekaf", request)
            .await?;

        if let Some(redirect_fqdn) = response.redirect_dataplane_fqdn {
            // Return special redirect authentication containing target FQDN
            Ok(SessionAuthentication::Redirect(redirect_fqdn))
        } else {
            // Normal authentication flow
            // ... existing logic ...
        }
    }
}
```

## Client Experience

1. **Initial Connection**: Client connects to old dataplane's Dekaf instance
2. **Authentication**: Client authenticates with task credentials
3. **Metadata Request**: Client requests broker metadata
4. **Redirect Response**: Dekaf returns target dataplane's broker address
5. **Reconnection**: Client automatically reconnects to target dataplane
6. **Normal Operation**: Client continues normal Kafka operations on new dataplane

## Operational Considerations

### Monitoring

Add metrics to track redirect behavior:

- `dekaf_redirects_total{source_dataplane, target_dataplane}` - Count of redirects
- `dekaf_redirected_sessions_total` - Count of sessions that have been redirected

### Logging

Add structured logging for redirect events:

```rust
tracing::info!(
    task_name = %task_name,
    source_dataplane = %self.app.data_plane_fqdn,
    target_dataplane = %target_dataplane.data_plane_fqdn,
    "Redirecting client to target dataplane"
);
```

## Security Considerations

- **Authentication**: Redirects only work with authenticated sessions
- **Authorization**: Task tokens must be valid on target dataplane
