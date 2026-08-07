---
description: Move an existing pipeline to Estuary without dropping your destination tables, using allow_existing_tables_for_new_bindings, Exclude Flow Document, and a cutover filter.
slug: /guides/migrate-to-estuary/
---

# Migrate an Existing Pipeline to Estuary

When you replace another pipeline with Estuary, the destination tables it wrote are usually worth keeping. They hold history that is expensive to re-extract, they may be partitioned by hand, and downstream models already point at them.

Estuary can attach to those tables and keep writing to them. By default that is blocked:

```
validating binding: table [warehouse schema orders] already exists for new binding
"acmeCo/production/orders". You must drop this table to continue.
```

The default is right whenever the destination holds nothing you cannot regenerate: drop the table and let Estuary rebuild it. This guide is for when it does hold something.

## Which case are you in

| Your table | What you need |
| --- | --- |
| Exists but is **empty**, pre-created to control its DDL | [`allow_existing_tables_for_new_bindings`](/guides/advanced-usage/feature-flags/#allow_existing_tables_for_new_bindings) only |
| Exists and **holds rows Estuary did not write** | That flag, plus [Exclude Flow Document](#why-exclude-flow-document-is-required) and a cutover filter |

The trigger is rows existing, not the table existing. The first case is not really a migration: it is how you apply table-level DDL that Estuary does not manage, such as [BigQuery partitioning](/reference/Connectors/materialization-connectors/BigQuery/#partitioning-a-new-table), which cannot be set after a table is created. The rest of this page is the second case.

## Cutover

### 1. Run the capture alongside your existing pipeline

Create the capture and let it run. Data lands in collections and nothing reaches your destination yet. Captures are read-only against the source, so both tools can read it concurrently. For CDC sources each tool needs its own replication slot or equivalent, so check your source's limits before adding a second.

### 2. Decide how to avoid re-loading history

[`notBefore`](/reference/time-travel) filters on when a document was **published into the collection**, not on when the row changed in your source. A timestamp copied from your outgoing tool's last sync therefore does not work: if the capture backfills, every document is published today, nothing is filtered, and the whole backfill lands in your table.

Two approaches that do work:

- Capture with **Only Changes** backfill mode, so the collection only ever holds post-cutover events.
- Let the capture backfill, then set `notBefore` to when that backfill completed. Rows that changed between the old tool's last sync and the capture's start are then picked up only when CDC next touches them.

Cleanest of all is to stop the old pipeline's writes before you publish the binding, and use that moment as the cutover point.

### 3. Check the table against the collection

Do this before cutting over, not after. See [Pre-cutover checks](#pre-cutover-checks).

### 4. Create the materialization

```yaml
materializations:
  acmeCo/production/materialize-bigquery:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-bigquery:dev
        config:
          # ...other endpoint configuration...
          advanced:
            feature_flags: "allow_existing_tables_for_new_bindings"
            no_flow_document: true
    bindings:
      - source: acmeCo/production/orders
        resource: { table: orders, dataset: warehouse }
        notBefore: 2026-01-01T00:00:00Z
        onIncompatibleSchemaChange: abort
```

- `allow_existing_tables_for_new_bindings` lets the binding attach instead of failing. For what the connector then does to the table, see [What the connector does to the existing table](/guides/advanced-usage/feature-flags/#what-the-connector-does-to-the-existing-table).
- **Exclude Flow Document** (`no_flow_document`) is required once the table holds rows. See [why](#why-exclude-flow-document-is-required).
- [`onIncompatibleSchemaChange: abort`](/concepts/advanced/evolutions/) replaces the default of `backfill`, which responds to an incompatible schema change by refreshing the table from the collection. Against history the collection does not have, that is data loss.

:::warning
Do not use the **Backfill** button on this materialization. A backfill truncates the destination table, and paired with an incompatible schema change it drops and recreates it. Either way the pre-existing rows are gone, along with any partitioning. See [Schema changes during backfill](/reference/backfilling-data/#schema-changes-during-backfill).
:::

### 5. Overlap, then decommission

Let both pipelines run long enough to compare row counts and spot-check recently changed rows, then disable the old one.

## Why Exclude Flow Document is required

Under [standard updates](/concepts/materialization/#delta-updates) the connector keeps each complete document in a `flow_document` column and reads it back to merge updates into.

When a binding attaches to an existing table, the connector adds that column but cannot populate it for the rows already there. The load query selects it anyway and reads a null, and the task fails. The exact error varies by connector, but it is always a task failure rather than silent corruption: the pipeline stops instead of writing wrong rows.

[Exclude Flow Document](/guides/customize-materialization-fields/#excluding-flow_document-with-standard-updates) reconstructs the document from the table's own top-level columns instead, and those do hold values for the pre-existing rows.

Set it when you create the binding. Enabling it on a binding that already exists also works, and takes effect on that binding's next transaction, but the task will have been failing in the meantime.

## Pre-cutover checks

The connector cannot verify that an existing table is compatible with the collection. In rough order of how much damage they do when missed:

- **Row uniqueness must match the collection key.** The connector merges on the collection key. If the existing rows are not uniquely identified by that key, updates will not find them and will insert instead, leaving duplicates and no error. This is the most common and least visible migration failure. If the old tool generated a surrogate key of its own, use [`groupBy`](/guides/customize-materialization-fields/#group-by-keys) to merge on the real one.
- **Column names and types.** Other tools lowercase names or add their own metadata, and type mappings differ for the same source column, particularly numeric precision and timestamps. Estuary uses the collection's field names and the mapping documented on your destination connector's page.
- **The old tool's metadata columns go stale.** They stay in the table but stop being updated, since Estuary does not write them. Check that no downstream model or incremental filter depends on one. Estuary's equivalent is `flow_published_at`.
- **Deletion behavior.** Confirm whether the table used hard deletes or a soft-delete marker, and configure the materialization to match.

If you can, run the materialization against a copy of the table in a scratch schema first and compare. It catches all of the above at once.

## Related

- [Feature flags](/guides/advanced-usage/feature-flags/) for `allow_existing_tables_for_new_bindings`, and for `retain_existing_data_on_backfill`
- [Customize materialized fields](/guides/customize-materialization-fields/) for field selection, Exclude Flow Document, and `groupBy`
- [Time travel](/reference/time-travel) for `notBefore`
- [Backfilling data](/reference/backfilling-data/) for what a backfill does to a destination table
- [Schema evolution](/guides/schema-evolution/) for `onIncompatibleSchemaChange`
