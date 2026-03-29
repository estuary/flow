# Diagnosis Report: Task Stuck in "Inferred Schema Updating"

## Executive Summary

When a task shows as stuck in "inferred schema updating," it means the **collection controller** has detected that the inferred schema in the database (`inferred_schemas` table) has a different MD5 than the one it last published, and is attempting (or waiting) to re-publish the collection to incorporate the new schema. Several conditions can cause this process to take longer than expected or get stuck entirely.

---

## 1. What "Inferred Schema Updating" Means

### The Inferred Schema System

Collections that use **schema inference** (their `readSchema` contains a `$ref` to `flow://inferred-schema`) have their schemas automatically derived from observed data. Here's the data flow:

1. **Runtime** (capture or derivation) observes documents and maintains an internal `doc::Shape`
2. At each transaction commit, if the shape changed, runtime emits a structured log: `"inferred schema updated"`
3. **stats_loader** (a materialization) reads these logs and writes to the `public.inferred_schemas` table
4. A **database trigger** (`internal.on_inferred_schema_update()`) sends an `InferredSchemaUpdated` message to the collection's controller task
5. The **collection controller** wakes up, fetches the new inferred schema, and publishes the collection

### Key Files
- `crates/agent/src/controllers/collection.rs` — Controller logic (lines 214-301: `update_inferred_schema()`)
- `crates/models/src/status/collection.rs` — Status types (`InferredSchemaStatus`)
- `crates/validation/src/collection.rs` — Schema resolution during build (lines 122-172)
- `supabase/migrations/01_compacted.sql` — DB trigger `on_inferred_schema_update()` (lines 1774-1808)

### Status Fields

The `InferredSchemaStatus` (visible via API and dashboard) contains:

| Field | Meaning |
|-------|---------|
| `schema_md5` | MD5 of the inferred schema that was **last successfully published** |
| `next_md5` | MD5 of the inferred schema **pending publication** (waiting on cooldown or retrying) |
| `schema_last_updated` | Timestamp of the last successful inferred schema publication |

Additionally, `publications.next_after` indicates the next time the controller will attempt to publish.

---

## 2. Normal Lifecycle of an Inferred Schema Update

```
┌──────────────────────────────────────────────────────────────┐
│ 1. RUNTIME: Capture processes documents                       │
│    → Observes new fields/types in data                        │
│    → Emits "inferred schema updated" structured log           │
└─────────────────────┬────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│ 2. STATS_LOADER: Materializes log into inferred_schemas table │
│    → INSERT/UPDATE with new schema JSON                       │
│    → md5 column auto-generated from schema content            │
└─────────────────────┬────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│ 3. DB TRIGGER: on_inferred_schema_update()                    │
│    → Fires on INSERT or UPDATE WHERE old.md5 != new.md5       │
│    → Calls send_to_task() → wakes controller (wake_at = NOW)  │
└─────────────────────┬────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│ 4. CONTROLLER: collection::update_inferred_schema()           │
│    → Fetches inferred schema from DB                          │
│    → Compares MD5 with status.schema_md5                      │
│    → If different: sets next_md5, checks cooldown             │
│    → Creates PendingPublication with detail                   │
│      "updating inferred schema"                               │
│    → Publishes (builds, validates, commits)                   │
└─────────────────────┬────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│ 5. PUBLICATION SUCCESS                                        │
│    → collection_published_successfully() called               │
│    → Clears next_md5                                          │
│    → Updates schema_md5 to new value                          │
│    → Updates schema_last_updated timestamp                    │
│    → Notifies dependent tasks                                 │
└──────────────────────────────────────────────────────────────┘
```

**Normal timing:** An inferred schema update typically completes within seconds to a few minutes, depending on publication cooldown configuration.

---

## 3. What Causes It to Take Longer Than Expected

### 3.1 Publication Cooldown (Most Common — Minutes)

**Mechanism:** `check_can_publish()` in `publication_status.rs:75-85` enforces a minimum time between publications. If a recent publication occurred (for any reason — dependency update, periodic, user-initiated), the controller must wait.

**Symptoms:**
- `next_md5` is set (pending update detected)
- `publications.next_after` is set to a future timestamp
- Controller error: `"waiting on publication cooldown"`
- No actual build errors

**Duration:** Depends on `controller_publication_cooldown()` configuration (typically 5 minutes).

**Why it could be extended:** If other publications keep happening (e.g., rapid dependency changes from upstream captures), each one resets the cooldown window.

### 3.2 Build Failure / Validation Errors (Common — Minutes to Hours)

**Mechanism:** The publication attempts to build the collection with the new inferred schema. If the build fails (e.g., the new schema is incompatible with downstream materializations, key violations, etc.), the publication fails and retries with exponential backoff.

**Backoff schedule** (`backoff_publication_failure()` in `mod.rs:443-454`):
| Attempt | Backoff |
|---------|---------|
| 1st failure | 1 minute |
| 2nd failure | 2 minutes |
| 3rd failure | 30 minutes |
| 4th failure | 60 minutes |
| 5th failure | 150 minutes |
| 6th+ failures | Up to 300 minutes (5 hours) max |

**Symptoms:**
- `next_md5` is set
- Controller has an error message (e.g., `"publication failed with status: BuildFailed"`)
- `publications.history` shows recent failed publications with errors
- After 3+ failures: `BackgroundPublicationFailed` alert fires

**Common build failure causes:**
- New inferred fields violate downstream materialization constraints
- Schema evolution introduces incompatible type changes
- Dependent specs have their own issues

### 3.3 Publication Superseded (Occasional — Minutes)

**Mechanism:** The controller creates a publication with `expect_pub_id` matching the last known pub_id. If another publication (user-initiated or from another controller action) commits between when the controller reads state and when it publishes, the publication fails with `PublicationSuperseded`.

**Symptoms:**
- Publication history shows `PublicationSuperseded` status
- Typically self-resolving on next retry (controller re-fetches state)

### 3.4 Generation ID Mismatch (After Collection Reset)

**Mechanism:** Inferred schemas carry an `x-collection-generation-id` annotation. After a collection is reset, the generation ID changes. The old inferred schema's generation ID no longer matches, so validation applies a **placeholder schema** instead (rejects all documents at read time).

**Code:** `crates/validation/src/collection.rs:126-155`

**Symptoms:**
- Collection was recently reset
- The inferred schema exists in DB but isn't being applied
- Validation logs: `"applied inferred schema placeholder (inferred schema is stale)"`
- Capture must produce new documents to generate a schema with the correct generation ID

**Duration:** Until the capture writes enough new documents to produce a complete inferred schema. This could take a long time if the capture is slow or if no data is flowing.

### 3.5 Database Trigger Not Firing (Rare — Up to 5 Hours)

**Mechanism:** The `on_inferred_schema_update()` trigger should fire on INSERT or UPDATE to `inferred_schemas`. If it doesn't (e.g., trigger disabled, transaction rollback), the controller won't be notified.

**Fallback:** The controller has a periodic check every **300 minutes (5 hours)** as a backup (`NextRun::after_minutes(300)` in `collection.rs:67`).

**Symptoms:**
- Inferred schema changed in DB (check `inferred_schemas` table)
- Controller task was not woken up (check `internal.tasks.wake_at`)
- Eventually resolves on the 5-hour periodic check

### 3.6 stats_loader Not Running (Systemic)

**Mechanism:** The `stats_loader` materialization writes to `inferred_schemas`. If it's not running, paused, or failing, inferred schemas never reach the database.

**Symptoms:**
- `inferred_schemas` table has stale/missing entries
- Runtime logs show `"inferred schema updated"` but DB doesn't reflect changes
- The ops-catalog derivation `inferred-schemas` isn't processing

### 3.7 No Data Flowing (User-Side)

**Mechanism:** Inferred schemas are derived from observed documents. If the capture isn't producing data, the schema can't be inferred or updated.

**Symptoms:**
- Capture has no recent transactions in stats
- `inferred_schemas` table has no entry or very old entry for the collection

### 3.8 EmptyDraft After Race with User Publication (Edge Case)

**Mechanism:** A subtle race condition can occur:
1. Inferred schema changes (MD5 A → B)
2. User publishes the collection (which picks up inferred schema B during build)
3. Controller runs, sees `schema_md5 = A` (stale), DB has MD5 = B
4. Controller creates publication to update inferred schema
5. During build, the inferred schema B is resolved — but it's already in the live built spec
6. If the built spec ends up as passthrough → `EmptyDraft` status
7. `error_for_status()` treats `EmptyDraft` as failure (see TODO comment at `publications/mod.rs:123`)
8. `collection_published_successfully()` never called → `schema_md5` stays at A
9. Loop repeats with exponential backoff

**Note:** There is a `TODO(phil)` comment in the code acknowledging this: `"consider returning Ok if status is EmptyDraft?"` at `crates/control-plane-api/src/publications/mod.rs:123`.

**Duration:** Exponential backoff up to 5 hours between attempts. Eventually resolves when another publication succeeds (periodic publish every 20 days, dependency change, or user action).

---

## 4. Normal Resolution Mechanisms

| Mechanism | How It Resolves | Timing |
|-----------|----------------|--------|
| **Cooldown expiry** | Controller re-runs after `publications.next_after` | Configured cooldown (typically 5 min) |
| **Retry with backoff** | Controller retries failed publication | 1min → 2min → 30min → 5h max |
| **User publication** | Any successful publication calls `collection_published_successfully()` which updates `schema_md5` | Immediate when user publishes |
| **Dependency update** | If a dependency changes, the dependency handler publishes and updates schema status | When upstream spec changes |
| **Periodic publish** | Background periodic rebuild every 20 days | Up to 20 days |
| **Periodic inferred check** | Fallback check every 300 minutes | Up to 5 hours |
| **Alert notification** | After 3+ failures, `BackgroundPublicationFailed` alert fires, notifying the user | After 3rd consecutive failure |

---

## 5. Diagnosis Steps

### Step 1: Check Controller Status

Query the collection's controller status:
```sql
SELECT
    cj.status,
    cj.error,
    cj.failures,
    cj.updated_at
FROM controller_jobs cj
JOIN live_specs ls ON ls.controller_task_id = cj.live_spec_id
WHERE ls.catalog_name = 'your/collection/name';
```

Look at:
- `status.inferred_schema.schema_md5` — last successfully published MD5
- `status.inferred_schema.next_md5` — pending MD5 (if set, update is pending)
- `status.publications.next_after` — when next publication will be attempted
- `status.publications.history` — recent publication attempts and outcomes
- `error` — current error message
- `failures` — consecutive failure count

### Step 2: Check Inferred Schema in Database

```sql
SELECT collection_name, md5, length(schema::text) as schema_size
FROM inferred_schemas
WHERE collection_name = 'your/collection/name';
```

Compare `md5` with `status.inferred_schema.schema_md5`. If different, the controller should be trying to update.

### Step 3: Check Controller Task State

```sql
SELECT
    t.task_id,
    t.wake_at,
    t.heartbeat,
    array_length(t.inbox, 1) as inbox_count,
    array_length(t.inbox_next, 1) as inbox_next_count
FROM internal.tasks t
JOIN live_specs ls ON ls.controller_task_id = t.task_id
WHERE ls.catalog_name = 'your/collection/name';
```

- `wake_at` NULL → task is suspended (shouldn't be if update is pending)
- `wake_at` in far future → waiting on backoff
- `heartbeat` recent → task is currently running
- `heartbeat = '0001-01-01'` → task is idle

### Step 4: Check Publication History

Look at the publications in the controller status JSON:
```sql
SELECT
    cj.status->'publications'->'history' as pub_history
FROM controller_jobs cj
JOIN live_specs ls ON ls.controller_task_id = cj.live_spec_id
WHERE ls.catalog_name = 'your/collection/name';
```

Each entry shows `detail`, `result`, `errors`, and `count`. Look for:
- `"updating inferred schema"` detail entries
- `BuildFailed`, `EmptyDraft`, or `PublicationSuperseded` results
- Specific error messages in `errors`

### Step 5: Check for Alerts

```sql
SELECT *
FROM alert_history
WHERE catalog_name = 'your/collection/name'
    AND alert_type = 'background_publication_failed';
```

### Step 6: Verify Data Flow

Check if the capture is actually producing data:
```sql
SELECT * FROM catalog_stats
WHERE catalog_name LIKE '%your/capture%'
ORDER BY ts DESC LIMIT 5;
```

Check if stats_loader is healthy:
```sql
SELECT * FROM catalog_stats
WHERE catalog_name LIKE 'ops%stats%'
ORDER BY ts DESC LIMIT 5;
```

---

## 6. Remediation Actions

### If Stuck on Cooldown
- **Wait** for `publications.next_after` to pass
- Or publish the collection manually (which resets status)

### If Stuck on Build Failure
1. Read the error from `publications.history[0].errors`
2. Fix the underlying issue (typically downstream materialization incompatibility)
3. Publish the collection manually, or wait for retry

### If Stuck After User Publication (EmptyDraft Loop)
1. Manually publish the collection (even a no-op publish via the UI will call `collection_published_successfully()` if it succeeds as non-empty)
2. Or wait for the periodic rebuild (up to 20 days)
3. Long-term fix: address the `TODO(phil)` in `error_for_status()` to treat `EmptyDraft` as success

### If Stuck Due to Generation ID Mismatch
1. Verify the collection was recently reset
2. Wait for the capture to produce new documents
3. The new documents will generate a fresh inferred schema with the correct generation ID

### If Database Trigger Isn't Firing
1. Check trigger existence: `SELECT * FROM pg_trigger WHERE tgname LIKE 'inferred_schema%';`
2. Verify trigger is enabled: `ALTER TABLE inferred_schemas ENABLE TRIGGER ALL;`
3. Manually wake the controller: `SELECT internal.send_to_task(controller_task_id, '00:00:00:00:00:00:00:00'::flowid, '{"type":"inferred_schema_updated"}'::json) FROM live_specs WHERE catalog_name = 'your/collection/name';`

### If stats_loader Is Down
1. Check the ops stats materialization health
2. Ensure the data plane is running
3. Check for stats_loader errors in the ops logs

### Nuclear Option: Force Republish
Send a republish request to the controller task:
```sql
SELECT internal.send_to_task(
    ls.controller_task_id,
    '00:00:00:00:00:00:00:00'::flowid,
    '{"type":"republish","reason":"manual intervention for stuck inferred schema"}'::json
)
FROM live_specs ls
WHERE ls.catalog_name = 'your/collection/name';
```

---

## 7. Potential Code Improvements

1. **EmptyDraft handling** (`crates/control-plane-api/src/publications/mod.rs:123`): The existing `TODO(phil)` suggests treating `EmptyDraft` as success in `error_for_status()`, which would prevent the EmptyDraft loop scenario.

2. **`add_inferred_schema_md5` is unused** (`crates/control-plane-api/src/publications/db_complete.rs:351-368`): The function to update `live_specs.inferred_schema_md5` during publication commit exists but is never called. This means `live_specs.inferred_schema_md5` may be stale, causing the `unchanged_draft_specs` view to not properly account for inferred schema changes.

3. **`unchanged_draft_specs` view** (`supabase/migrations/01_compacted.sql:6815-6824`): The comment on `prune_unchanged_draft_specs` claims it checks inferred schema identity, but the view only compares `draft_spec_md5 = live_spec_md5` without checking `inferred_schema_md5 = live_inferred_schema_md5`.

4. **Periodic fallback interval** (300 minutes) could be reduced for collections with pending inferred schema updates.
