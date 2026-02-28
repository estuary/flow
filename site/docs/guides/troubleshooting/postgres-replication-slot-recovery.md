---
sidebar_label: PostgreSQL Replication Slot Recovery
description: How to recover a PostgreSQL capture when the replication slot has been dropped or invalidated.
---

# Recovering a PostgreSQL capture after replication slot loss

Estuary's PostgreSQL CDC connector relies on a logical replication slot to track its position in the WAL (Write-Ahead Log). If that slot is dropped or invalidated, the capture will fail and cannot automatically recover. This guide explains how to identify the problem and restore the capture with minimal data re-processing.

## Common causes

Replication slots can be lost in several scenarios:

- **Major version upgrades** - PostgreSQL drops all logical replication slots during a major version upgrade (e.g. 15 → 16, 17 → 18). This applies to RDS, Aurora, Cloud SQL, and self-managed Postgres.
- **WAL size limit exceeded** - If `max_slot_wal_keep_size` is configured and the slot falls behind, Postgres will invalidate the slot to free disk space.
- **Manual deletion** - The slot was dropped manually, or removed as part of a migration or failover procedure.
- **Database failover** - On failover to a standby, the logical replication slot from the old primary does not carry over to the new primary.

## Identifying the problem

When the replication slot is gone, the capture will fail with one of these errors:

```
runTransactions: readMessage: error starting replication: ERROR: replication slot "flow_slot" does not exist (SQLSTATE 42704)
```

or, after a version upgrade where the WAL epoch has reset:

```
resume cursor mismatch: resume LSN is greater than server flush LSN
```

The capture will retry automatically but cannot recover on its own. You need to trigger a backfill to reset the WAL position and recreate the slot.

## Recovery procedure

### Step 1: Find the last transaction ID

Find the last transaction ID the capture successfully processed before failing. You'll use this to limit the backfill to only rows that changed during the outage.

Run the following, replacing the task name with your capture. Adjust `--since` to cover back to when the capture was last running - if it's been down for more than 48 hours, increase this value accordingly (e.g. `--since 96h`):

```bash
flowctl logs --since 48h --task <your-capture-task> | \
  jq -c 'del(.shard, ._meta) | {ts, message} + .' | \
  grep '"xid"' | tail -1
```

Example output:

```json
{"ts":"2026-02-27T05:13:11Z","message":"current transaction ID","fields":{"xid":583198898}}
```

Note the `xid` value - you'll use it in the next step.

:::note
If no `xid` entries appear, the capture may have been down longer than your log retention window. In this case, skip the XMIN approach and run a full **incremental** backfill of all bindings instead.
:::

### Step 2: Trigger an XMIN backfill

In the Estuary web app:

1. Open the capture and click **Edit**.
2. Under **Endpoint Config → Advanced Options**, set the **Minimum Backfill XID** field to the transaction ID from Step 1.
3. In the **Target Collections** section, click the backfill button. This marks all bindings for backfill at once. In the backfill mode dropdown, select **Incremental Backfill (Advanced)**. This is important - it ensures your destination tables are not dropped and rebuilt from scratch.
4. Save & publish.

The connector will automatically recreate the replication slot when it restarts after publishing.

This will:

- Drop the broken slot state and create a fresh replication slot
- Scan each table for rows modified since the given transaction ID
- Resume CDC from the current WAL position

:::warning Choosing between XMIN and a full backfill
An XMIN backfill scans for rows that were **inserted or updated** since the cutoff transaction ID. Rows **deleted** during the outage window will not be detected and will remain in your destination tables.

Choose the approach based on your situation:
- **XMIN backfill (recommended for most cases)**: Much faster for large databases. Use this if deletions during the outage are not a concern and you have a reliable transaction ID from Step 1.
- **Full incremental backfill**: Re-reads all rows and reflects the current source state, including deletions. Use this if deletions occurred during the outage and need to be propagated, or if no transaction ID is available.
:::

### Step 3: Monitor recovery

Watch the task logs to confirm the capture is recovering:

```bash
flowctl logs --since 10m --task <your-capture-task> | \
  jq -c 'del(.shard, ._meta) | {ts, message} + .' | \
  grep -v "inferred schema\|processed replication\|binding removed\|autoselected\|explain backfill"
```

Key log messages during recovery:

| Message | What it means |
|---------|---------------|
| `attempting to create replication slot` | Slot creation in progress |
| `created replication slot` | Slot created successfully |
| `starting replication` | CDC stream established |
| `Backfilling Tables (N tables backfilling)` | XMIN scan running |
| `backfill completed` | Table scan finished - `rows: 0` is normal for tables with no changes |
| `processed replication events` | Back to normal CDC |

Once all tables complete, the capture resumes CDC from the new WAL position.

### Step 4: Clear the XMIN value

After the capture is healthy, we recommend clearing the **Minimum Backfill XID** value from **Endpoint Config → Advanced Options**. The XMIN value is only applied when a backfill is triggered - it has no effect during normal CDC operation. However, leaving it in place means that if a backfill is ever triggered again in the future (intentionally or otherwise), the scan will be incorrectly limited to rows modified since the old transaction ID, potentially missing data. Clearing it ensures any future backfill starts from a clean state.
