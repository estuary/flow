# Abandoned Task Detection and Disablement

## Part 1: Relevant System Context

### Controller Architecture

Every `live_specs` row has a 1:1 `LiveSpecControllerExecutor` automation task (TaskType 2) that manages its lifecycle. Controllers run a pipeline of stages, dispatched by spec type:

```text
capture:        auto_discover -> config_update -> dependencies -> republish -> periodic -> activation -> update_observed_pub_id
materialization:                  config_update -> dependencies -> source_capture -> republish -> periodic -> activation -> update_observed_pub_id
collection:                                       maybe_publish (dependencies + republish + periodic + inferred_schema) -> activation -> update_observed_pub_id
test:                                             dependencies -> republish -> periodic -> update_observed_pub_id
```

Each stage returns `Option<NextRun>`. The earliest across all stages determines when the controller next wakes. If no stage requests a future run, the controller suspends until a message arrives via `internal.send_to_task()`.

Controller state persists in `controller_jobs.status` as spec-type-specific JSON (`CaptureStatus`, `MaterializationStatus`, etc.), each containing sub-statuses for publications, activation, alerts, and type-specific fields.

### Shard Status and Health Monitoring

The **activation stage** (`crates/agent/src/controllers/activation.rs`) is responsible for both activating built specs into data planes and monitoring shard health.

Gazette shards have a `ReplicaStatus::Code` enum: `Idle(0) -> Backfill(100) -> Standby(200) -> Primary(300) -> Failed(400)`. A shard is healthy when at least one replica is `Primary`. The activation controller periodically calls `control_plane.list_task_shards()` (a `consumer::ListRequest` RPC to the data plane's reactor) and reduces individual replica statuses into:

```rust
enum ShardsStatus { Ok, Pending, Failed }
```

- `Ok`: all shards have a Primary replica
- `Pending`: some shards are Idle/Backfill/Standby, none Failed
- `Failed`: any shard has Failed without a Primary

Health check frequency varies with progressive backoff. When `Ok`: 3min up to 2hrs. When not `Ok`: 30s up to 1min, 3min, then 60min after 20+ consecutive checks. The result is stored in `ActivationStatus.shard_status`:

```rust
pub struct ShardStatusCheck {
    pub count: u32,            // consecutive checks with this status
    pub last_ts: DateTime<Utc>,
    pub first_ts: DateTime<Utc>, // when this status streak began
    pub status: ShardsStatus,
}
```

The activation controller also tracks `recent_failure_count` (failures within the `SHARD_FAILURE_RETENTION` window, default 8 hours) and fires a `ShardFailed` alert after 3+ failures. It resolves the alert after 2 hours of continuous `Ok` status.

`ShardStatusCheck` only tracks the *current streak*. When the status changes (e.g., `Ok` -> `Failed`), `first_ts` resets to the new status's start time. There is no persistent record of "when was the task last Primary."

### Alert System

Alerts follow two evaluation paths that converge on the same `evaluate_alert_actions()` diff function and `apply_alert_actions()` for persisting to `alert_history`:

**Controller-evaluated** (`view_name() = None`): `ShardFailed`, `AutoDiscoverFailed`, `BackgroundPublicationFailed`. These are set/resolved directly by controller code during the pipeline run, stored in the `Alerts` BTreeMap within `ControllerStatus`. At the end of each controller run, `evaluate_controller_alerts()` converts the BTreeMap to `Vec<AlertViewRow>` via `to_alert_view()`, fetches open alerts from `alert_history` for this catalog name, diffs them, and applies the resulting actions within the same transaction as the controller status update.

**DB-view-evaluated** (`view_name() = Some(...)`): `DataMovementStalled`, `FreeTrial*`, `MissingPaymentMethod`. These are computed by SQL views (`internal.alert_data_movement_stalled`, `internal.tenant_alerts`) and evaluated by singleton `AlertEvaluator` automation tasks (TaskTypes 10, 11) on a configurable interval. The `AlertEvaluator` is generic over a trait called `AlertView`, whose `query()` method returns `Vec<AlertViewRow>`.

Both paths converge on the same `evaluate_alert_actions()` pure function and `apply_alert_actions()` for writing to `alert_history` and creating `AlertNotifications` tasks (TaskType 9) that send emails.

Subscriptions live in `alert_subscriptions`, mapping `catalog_prefix` + `email` + `include_alert_types[]`. Alerts with no matching subscriptions still appear in `alert_history` but trigger no notification emails.

### Alert System History and the SQL View Problem

The alert system has gone through three generations:

1. **2023**: PL/pgSQL cron job (`evaluate_alert_events()`) querying SQL views, with HTTP-trigger-based email sending via Supabase edge functions. Emails were lost on HTTP failures. Evaluation was delayed by cron interval.

2. **Mid-2025**: Controller-evaluated alerts introduced (`ShardFailed`, `AutoDiscoverFailed`). These bypass SQL views entirely, setting alert state directly in the controller's status JSON. A serialization bug (PascalCase vs lowercase `AlertState`) silently prevented all controller alerts from firing for months because the `controller_alerts` SQL view expected a specific casing.

3. **Late 2025**: "Alerts redux" moved evaluation into Rust `AlertEvaluator` tasks and replaced the edge function with a Rust notification system. The PL/pgSQL cron job and HTTP triggers were disabled. The SQL views were kept but the evaluator code explicitly marks them as legacy:

   ```rust
   // This queries the `internal.tenant_alerts` view for historical
   // reasons. If we ever need to change that view, we should consider
   // dropping the view in favor of a regular sql query, which is easier
   // to manage.
   ```

   And from the module documentation:

   ```rust
   //! - The `tenant_alerts` and `alert_data_movement_stalled` views: These are
   //!   holdovers from the old alerting system, which expose firing alerts as
   //!   database views.
   ```

The `alert_all` view (union of all alert views) and the `evaluate_alert_events()` function are now dead code. The `controller_alerts` view exists but is not consumed by any Rust code; controller alerts are read directly from the status JSON.

The `AlertView` trait's `query()` method returns `Vec<AlertViewRow>` and has no structural dependency on SQL views. The two existing implementations happen to query views, but any code producing `Vec<AlertViewRow>` would work. The controller-evaluated path already demonstrates this: `to_alert_view()` converts in-memory `ControllerAlert` structs to `Vec<AlertViewRow>` without touching SQL.

### Task Disabling

A task is disabled by publishing its spec with `shards.disable = true`. The field path varies:

- Capture: `capture.shards.disable`
- Materialization: `materialization.shards.disable`
- Derivation: `collection.derive.shards.disable`

Currently, no controller automatically disables tasks. The only automated disabling happens during validation when `onIncompatibleSchemaChange: disableTask` is triggered. Controllers check `is_enabled_task()` to skip periodic republication and certain stages for disabled tasks.

---

## Part 2: Design

### What "Abandoned" Means

A task is abandoned when it meets all of these conditions:

1. **Enabled**: `shards.disable` is not `true`. Disabled tasks don't consume data-plane resources.

2. **No sustained PRIMARY**: No shard has held *sustained* PRIMARY status (3+ consecutive Ok health checks, ~6-9 minutes minimum) for a configurable duration (e.g., 14 days). When `last_primary_ts` is `None` (sustained PRIMARY has never been observed), `created_at` is used as the fallback, so newly-created tasks aren't flagged until they've existed for the full threshold duration. This covers:
   - Shards stuck in FAILED (crash-looping, bad config, unreachable source)
   - Shards stuck in IDLE/PENDING (activation failures, data-plane issues)
   - Shards that were never successfully created
   - Shards that briefly reach PRIMARY on each restart but immediately hit a terminal runtime error (e.g., a bad query that can't be caught during Validate)

3. **No connector status**: The connector hasn't emitted a `ConnectorStatus` event within the detection window. `ConnectorStatus` is the connector's self-reported health signal, emitted as a log event after startup. This is stronger than just observing PRIMARY, since a task could be assigned PRIMARY but the connector could be hung or crashing before it reports status.

This definition excludes "running but idle" tasks (PRIMARY shards, zero throughput). Those are a separate cost-optimization concern.

### Approach: Controller-Evaluated (no SQL view)

The controller-evaluated alert path is the newer pattern in this codebase (used by `ShardFailed`, `AutoDiscoverFailed`, `BackgroundPublicationFailed`). The abandoned-task alert fits naturally here because:

- The per-spec controller already does shard health checks during the activation stage, so the primary detection signal (`last_primary_ts`) is available locally.
- Controller-evaluated alerts fire/resolve immediately within the controller's wake cycle, with no separate evaluation interval.
- No SQL view to maintain. The detection logic lives in Rust, where it's testable and doesn't risk SQL/Rust serialization mismatches (the casing bug that silently broke controller alerts for months).
- The existing `controller_alerts` SQL view extracts all alerts from `controller_jobs.status` JSON, including any new alert types, so `TaskAbandoned` shows up there automatically.

The one piece missing from the controller's current state is the connector status timestamp. The `connector_status` table (one row per task, PK on `catalog_name`) is populated by a materialization from the ops events pipeline, not by the controller. Since the controller doesn't observe connector status events, this requires a join in `fetch_controller_job()` to read it.

### Detection Signals

#### `last_primary_ts` (new field in `ActivationStatus`)

```rust
pub struct ActivationStatus {
    // ... existing fields ...
    /// Last time shards were observed with sustained PRIMARY status (3+
    /// consecutive Ok health checks). Updated during periodic shard health
    /// checks. None if sustained PRIMARY has never been observed.
    #[serde(default)]
    pub last_primary_ts: Option<DateTime<Utc>>,
}
```

Updated in `update_shard_health()` when shards have been Ok for multiple consecutive health checks:

```rust
// Only update last_primary_ts when shards have been Ok for 3+ consecutive
// checks, filtering out tasks that briefly reach PRIMARY then crash.
// With Ok health check intervals starting at 3 minutes (activation.rs:587),
// this means shards were healthy for at least ~6-9 minutes.
if aggregate == ShardsStatus::Ok
    && status.shard_status.as_ref()
        .is_some_and(|s| s.status == ShardsStatus::Ok && s.count >= 3)
{
    status.last_primary_ts = Some(control_plane.current_time());
}
```

The "sustained" requirement addresses tasks that reach PRIMARY briefly on each restart (long enough for the connector to send `Opened`) but then immediately hit a terminal runtime error. These tasks alternate between Ok (count=1) and Failed (count=1) on each health check, never accumulating 3 consecutive Ok checks, so `last_primary_ts` doesn't update and they're correctly flagged as abandoned.

No additional RPCs. Piggy-backs on existing health checks.

#### `last_connector_status_ts` (new field in `ControllerState`)

```rust
pub struct ControllerState {
    // ... existing fields ...
    /// Timestamp from the most recent ConnectorStatus event for this task.
    /// Populated by joining the connector_status table in fetch_controller_job().
    /// None if the connector has never emitted a status event.
    pub last_connector_status_ts: Option<DateTime<Utc>>,
}
```

Populated by extending the `fetch_controller_job()` SQL query:

```sql
left join connector_status cs on cs.catalog_name = ls.catalog_name
```

The `ts` field is extracted from `cs.flow_document->>'ts'`. The `connector_status` table has exactly one row per task (PK on `catalog_name`), materialized from the ops events pipeline. Connectors emit `connectorStatus` log events after startup when they're functioning. The `ts` indicates when the connector last self-reported.

This is a cheap PK lookup on each controller fetch. Unlike `last_primary_ts` (which the controller maintains in its persisted status based on shard health checks), the connector status arrives through the ops log pipeline and isn't something the controller observes directly, so a join is the appropriate way to read it.

### Alert Evaluation

A new check runs in the activation stage (or as a small dedicated stage immediately after activation), using the controller-evaluated alert path:

```rust
fn evaluate_abandoned(
    alerts: &mut Alerts,
    activation: &ActivationStatus,
    state: &ControllerState,
    now: DateTime<Utc>,
) {
    let cutoff_ts = now - ABANDONED_TASK_THRESHOLD;

    // When last_primary_ts is None (never observed sustained PRIMARY),
    // fall back to created_at so that new tasks aren't immediately flagged.
    let last_primary = activation.last_primary_ts.unwrap_or(state.created_at);

    let is_stale = |ts: Option<DateTime<Utc>>| ts.map_or(true, |t| t < cutoff_ts);

    let is_abandoned = has_task_shards(state)
        && last_primary < cutoff_ts
        && is_stale(state.last_connector_status_ts);

    if is_abandoned {
        set_alert_firing(
            alerts,
            AlertType::TaskAbandoned,
            state,
            &format!(
                "task has had no sustained PRIMARY shard since {}",
                last_primary,
            ),
        );
    } else {
        resolve_alert(alerts, AlertType::TaskAbandoned);
    }
}
```

The check uses `has_task_shards()` rather than `is_enabled_task()` as the guard. `is_enabled_task()` returns `true` for Dekaf captures/materializations, but `has_task_shards()` returns `false` for them (they don't have gazette shards, so `last_primary_ts` would never be set and they'd always look abandoned).

This runs within the existing controller pipeline. At the end of the controller run, `evaluate_controller_alerts()` diffs the alerts BTreeMap against `alert_history` and fires/resolves via `apply_alert_actions()`, exactly as it does for `ShardFailed` today. No new evaluator task, no new SQL view, no new task type.

**Data plane outage consideration**: If a data plane is down for longer than the threshold and nobody publishes any of its tasks, all tasks in that data plane would be flagged simultaneously. The 14-day default threshold makes this scenario unlikely in practice, and the phased rollout (detection-only first) provides a safety net. A possible future refinement would be to check whether the data plane itself is healthy before flagging.

### Operator Visibility

The existing `internal.controller_alerts` SQL view extracts alerts from `controller_jobs.status->'alerts'` JSON. Any `TaskAbandoned` alert set by the controller automatically appears in this view. Operators can query:

```sql
select catalog_name, value->>'error' as reason, value->>'first_ts' as since
from controller_jobs cj
join live_specs ls on ls.id = cj.live_spec_id,
lateral json_each(json_extract_path(cj.status, 'alerts')) as alerts(key, value)
where alerts.key = 'TaskAbandoned';
```

Or more simply, via `alert_history`:

```sql
select catalog_name, fired_at, arguments
from alert_history
where alert_type = 'task_abandoned'
  and resolved_at is null;
```

No new view is required for either query.

### Phase 1: Detection Only

Implementation steps:

1. **Add `last_primary_ts` to `ActivationStatus`** (`crates/models/src/status/activation.rs`). New `Option<DateTime<Utc>>` field with `#[serde(default)]`. Update `update_shard_health()` in `activation.rs` to set it when status is `Ok`.

2. **Add `last_connector_status_ts` to `ControllerState`** (`crates/agent/src/controllers/mod.rs`). Extend `fetch_controller_job()` in `crates/control-plane-api/src/controllers.rs` with a `left join connector_status` to read the `ts` field from the connector's status document.

3. **Add `task_abandoned` to the `alert_type` PostgreSQL enum** (new migration: `ALTER TYPE public.alert_type ADD VALUE 'task_abandoned'`).

4. **Add `TaskAbandoned` to the Rust `AlertType` enum** with `view_name() = None` (controller-evaluated).

5. **Implement `evaluate_abandoned()`** in a new module `crates/agent/src/controllers/abandon.rs`. Wire it into the capture, materialization, and collection `update()` functions, running after the activation stage. Derivations are handled by the collection pipeline; the `has_task_shards()` guard excludes plain collections.

6. **Write notification templates** (`crates/notifications/src/`). The fired template should describe what happened and list actions (fix the task, disable it, or contact support). These can be minimal since nobody will be subscribed initially.

7. **Do not add `task_abandoned` to default `include_alert_types`** in `alert_subscriptions`. Alerts appear in `alert_history` for operator inspection but trigger no emails.

No controller version bump is needed. The `last_primary_ts` field starts as `None` for all existing tasks and accumulates on the natural health-check cadence (within 2 hours for healthy tasks). The detection threshold of 14+ days means the warm-up period is inconsequential.

### Phase 2: Notification

Once detection is validated:

1. **Add `task_abandoned` to default `include_alert_types`** in `alert_subscriptions` (migration to update column default, optional backfill for existing subscriptions).

2. **Refine email templates** based on Phase 1 experience.

3. **Consider a CLI command** like `flowctl catalog list-abandoned` querying `alert_history`.

### Phase 3: Automatic Disablement

When the `TaskAbandoned` alert has been firing for longer than `ABANDONED_TASK_DISABLE_AFTER`, the controller disables the task by creating a background publication with `shards.disable = true`.

The `evaluate_abandoned()` function already has access to the alert's `first_ts` (when it started firing). The disable logic extends it:

```rust
if let Some(alert) = alerts.get(&AlertType::TaskAbandoned) {
    if alert.state == AlertState::Firing
        && alert.first_ts + ABANDONED_TASK_DISABLE_AFTER < now
    {
        // Create PendingPublication setting shards.disable = true
    }
}
```

On successful publish, the task becomes disabled. On the next controller run, `is_enabled_task()` returns false, `evaluate_abandoned()` resolves the alert, and the controller stops scheduling health checks.

On failed publish, the alert remains firing. On the next controller wake (scheduled by the activation stage's retry backoff), `evaluate_abandoned()` re-evaluates and retries. No separate evaluator or message needed.

### Configuration

All thresholds are environment-variable-configurable:

| Variable                       | Default | Description                                  |
| ------------------------------ | ------- | -------------------------------------------- |
| `ABANDONED_TASK_THRESHOLD`     | `14d`   | Duration without PRIMARY before flagging     |
| `ABANDONED_TASK_DISABLE_AFTER` | `7d`    | Days after alert fires before auto-disabling |

No evaluation interval config is needed since the check runs within the controller's natural wake cycle.

### Database Migration

A single migration:

```sql
ALTER TYPE public.alert_type ADD VALUE 'task_abandoned';
```

No views to create, modify, or maintain.

### Rollout Strategy

1. **Deploy Phase 1 with generous thresholds** (e.g., 30-day detection window). Monitor `alert_history` for `task_abandoned` entries. Manually review flagged tasks.

2. **Tune thresholds** based on observed data. Healthy tasks populate `last_primary_ts` within 2 hours; unhealthy tasks remain `None`, treated as "never had PRIMARY" and flagged after the grace period.

3. **Enable notifications (Phase 2)** once confident in signal quality.

4. **Enable auto-disablement (Phase 3)** with the disable-after period giving notified users time to react.

### On Migrating Away from SQL Alert Views Entirely

The existing DB-view-evaluated alerts (`DataMovementStalled`, `FreeTrial*`, `MissingPaymentMethod`) could be migrated to the controller-evaluated path or to plain Rust-side SQL queries, eliminating the SQL views. The code already acknowledges this direction.

**`DataMovementStalled`**: Could move into the per-spec controller's activation stage, since the controller already has catalog name and could query `catalog_stats` directly. Or the `AlertEvaluator` could replace the view query with an inline SQL statement in its `query()` method, since the `AlertView` trait only requires producing `Vec<AlertViewRow>`, not querying a named view.

**`FreeTrial*` / `MissingPaymentMethod`**: These are tenant-level alerts, not task-level. They don't map to individual live specs, so the controller-evaluated path doesn't apply. The `AlertEvaluator` for these would keep its singleton pattern but replace the `internal.tenant_alerts` view with a direct SQL query in Rust. The `AlertView::query()` method already accepts a `PgPool` and returns `Vec<AlertViewRow>`, so the change is mechanical: move the SQL from the view definition into the Rust `query()` implementation.

**Cleanup**: Once all views are replaced, `internal.alert_all`, `internal.controller_alerts`, and the disabled `evaluate_alert_events()` function can be dropped.

This migration is independent of the abandoned-task feature and can happen incrementally. The abandoned-task implementation avoids creating new views, so it doesn't add to the migration surface.

### Future: catalog_stats as an additional signal

Checking `catalog_stats` for zero data movement over a window would provide a third independent signal. This is deferred for now due to concerns about query load on the stats tables, but could be added as a final confirmation step gated behind the other signals (only query stats for tasks already flagged by the first two signals).

### Alternatives Considered

**DB-view-evaluated with a new SQL view**: The original design used a SQL view (`internal.alert_abandoned_tasks`) with a `LATERAL JOIN` to `publication_specs` and JSON extraction from `controller_jobs.status`. This would work but adds another SQL view to maintain, continues a pattern the codebase is moving away from, and risks the SQL/Rust serialization mismatches that have caused bugs before.

**User publication timestamp as a signal**: Tracking `last_user_pub_at` in `PublicationStatus` by observing when `last_pub_id` advances past `max_observed_pub_id` in `update_observed_pub_id()`. Removed in favor of connector status: the connector status timestamp is a stronger signal (the connector was actually alive and functioning) and doesn't require inferring user vs. background publications.

**Singleton evaluator without SQL view**: A new automation task (TaskType 12) that queries tables directly in Rust. Viable but adds a new task type, a new executor registration, and evaluation-interval configuration, when the per-spec controller already wakes periodically and has all the needed data.

**Piggy-backing on existing `DataMovementStalled`**: Expanding its scope to cover all tasks. Conflates two different concerns (configured monitoring vs. abandonment detection) and would require changing its opt-in model.
