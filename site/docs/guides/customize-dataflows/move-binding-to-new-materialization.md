---
description: Move a binding from one Estuary materialization to another without dropping the destination table or replaying data, using the allow_existing_tables_for_new_bindings feature flag and notBefore.
slug: /guides/move-binding-to-new-materialization/
---

# Moving a Binding to a New Materialization

You may want to move a binding from one materialization to another: to consolidate several materializations into one, to split a large materialization apart, or to move a table onto a materialization with different settings.

By default, a binding that is new to a materialization fails if its destination table already exists. This guide covers how to point a new binding at the existing table instead, so you keep the data already in it and avoid replaying history you have already materialized.

## Prerequisites

You need:

* An existing materialization with the binding you want to move.
* The target materialization the binding will move to. It can be an existing materialization or a new one.
* A materialization connector that supports the [`allow_existing_tables_for_new_bindings`](/guides/advanced-usage/feature-flags#allow_existing_tables_for_new_bindings) feature flag. This includes SQL and warehouse connectors (PostgreSQL, MySQL, Snowflake, BigQuery, Redshift) as well as MongoDB, DynamoDB, and Elasticsearch.

:::warning
The connector cannot verify that the existing table's schema is compatible with the new binding. Confirm the destination table matches the collection you are materializing before you begin.

If a column's type differs in a way the connector cannot migrate in place, the publication fails and the remedy offered is a backfill, which drops and recreates the table. That defeats the purpose of this procedure, so check the destination schema first.
:::

## Steps

1. **Pause writes to the source.** Disable the capture, or otherwise stop new documents from arriving in the source collection. Let the original materialization fully drain its backlog for the binding, so everything captured so far is written to the destination table.

2. **Confirm the drain finished.** Wait for the original materialization to report no remaining lag for the binding, then note the highest [`flow_published_at`](/guides/advanced-usage/metadata-fields#_metauuid-and-flow_published_at) value in the destination table:

   ```sql
   SELECT MAX(flow_published_at) FROM your_destination_table;
   ```

   This value anchors the `notBefore` timestamp you set in step 4. Do not skip this check: `notBefore` discards documents rather than deferring them, so anything the original materialization had not yet written when you cut over is lost from the destination unless you backfill.

3. **Disable the binding on the original materialization.** Disable it rather than deleting it, so you can re-enable it if the move needs to be reverted. Deleting the binding does not drop the destination table, but it does discard the binding's state.

4. **Add the binding to the new materialization.** Configure two settings on it:

   * Set the `allow_existing_tables_for_new_bindings` [feature flag](/guides/advanced-usage/feature-flags#allow_existing_tables_for_new_bindings) under **Endpoint Config → Advanced**. This lets the new binding write to the table that already exists.
   * Set [`notBefore`](/reference/time-travel) under **Binding → Advanced**, in the **Time Travel** section. Use the timestamp you recorded in step 2, or any point inside the pause window from step 1.

5. **Resume writes to the source.** Re-enable the capture. New data flows through the new binding only. `notBefore` causes the runtime to skip everything published before that timestamp, so the binding does not replay data the table already holds.

## Why notBefore is required

The feature flag and `notBefore` solve two different problems, and you need both.

`allow_existing_tables_for_new_bindings` only controls whether the connector will write to a table it did not create. It does not change where the binding starts reading. A binding that is new to a materialization always starts reading its source collection from the beginning, the same as any other backfill.

The flag also does not truncate or drop the existing table. A new binding whose destination table already exists is reconciled the same way an existing binding is when its schema changes: new columns are added and existing columns are widened as needed, but no data is removed.

`notBefore` is what limits the read. Without it, the new binding re-reads the full collection history and re-merges every document into the destination table.

## Leave the feature flag enabled

Keep `allow_existing_tables_for_new_bindings` set for as long as the binding materializes the adopted table. Do not remove it once the move is complete.

The flag does two separate things. It permits a new binding to write to a table that already exists, which only matters at the moment you add the binding. It also disables the runtime's load optimization, which matters permanently.

That optimization normally lets a materialization skip looking up keys it believes are new, based on the highest key the binding itself has stored. In an adopted table, rows the binding never wrote can sit above that high-water mark. With the optimization active, updates to those rows are treated as new keys and are inserted rather than merged.

Because the flag is only *required* while the binding is new, removing it later still publishes successfully. Nothing fails and no error is logged; merges quietly become inserts.

Note that this flag is set on the endpoint, so disabling the load optimization applies to every binding on the materialization, not only the one you moved.

## Related

* [Feature flags](/guides/advanced-usage/feature-flags) for the full list of available flags and their caveats.
* [Backfilling data](/reference/backfilling-data) for how backfills interact with existing destination data.
* [Time travel](/reference/time-travel) for `notBefore` and `notAfter` details.
