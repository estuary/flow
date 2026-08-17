---
description: Move a binding from one Estuary materialization to another without dropping the destination table or replaying data, using the allow_existing_tables_for_new_bindings feature flag and notBefore.
slug: /guides/move-binding-to-new-materialization/
---

# Moving a Binding to a New Materialization

You may want to move a binding from one materialization to another: to consolidate several materializations into one, to split a large materialization apart, or to move a table onto a materialization with different settings.

By default, a binding that is new to a materialization fails if its destination table already exists. This guide covers how to point a new binding at the existing table instead, so you keep the data already in it and avoid replaying history you have already materialized.

## Before you start

You need:

* An existing materialization with the binding you want to move.
* The target materialization the binding will move to. It can be an existing materialization or a new one.
* A SQL or warehouse materialization connector (PostgreSQL, MySQL, Snowflake, BigQuery, Redshift, and similar). See [`allow_existing_tables_for_new_bindings`](/guides/advanced-usage/feature-flags#allow_existing_tables_for_new_bindings) for connector support.

:::warning
The connector cannot verify that the existing table's schema is compatible with the new binding. Confirm the destination table matches the collection you are materializing before you begin.
:::

## Steps

1. **Pause writes to the source.** Disable the capture, or otherwise stop new documents from arriving in the source collection. Let the original materialization fully drain its backlog for the binding, so everything captured so far is written to the destination table.

2. **Disable the binding on the original materialization.** Do not delete the materialization or the binding while the table still needs to be written to by the new binding.

3. **Add the binding to the new materialization.** Configure two settings on it:

   * Set the `allow_existing_tables_for_new_bindings` [feature flag](/guides/advanced-usage/feature-flags#allow_existing_tables_for_new_bindings) under **Endpoint Config → Advanced**. This lets the new binding write to the table that already exists.
   * Set [`notBefore`](/reference/time-travel) under **Binding → Advanced**. Use a timestamp inside the pause window from step 1, or just after the last [`flow_published_at`](/guides/advanced-usage/metadata-fields#_metauuid-and-flow_published_at) value already written to the destination table.

4. **Resume writes to the source.** Re-enable the capture. New data flows through the new binding only. `notBefore` causes the runtime to skip everything published before that timestamp, so the binding does not replay data the table already holds.

## Why notBefore is required

The feature flag and `notBefore` solve two different problems, and you need both.

`allow_existing_tables_for_new_bindings` only controls whether the connector will write to a table it did not create. It does not change where the binding starts reading. A binding that is new to a materialization always starts reading its source collection from the beginning, the same as any other backfill.

The flag also does not truncate or drop the existing table. A new binding whose destination table already exists is reconciled the same way an existing binding is when its schema changes: new columns are added and existing columns are widened as needed, but no data is removed.

`notBefore` is what limits the read. Without it, the new binding re-reads the full collection history and re-merges every document into the destination table.

## Related

* [Feature flags](/guides/advanced-usage/feature-flags) for the full list of available flags and their caveats.
* [Backfilling data](/reference/backfilling-data) for how backfills interact with existing destination data.
* [Time travel](/reference/time-travel) for `notBefore` and `notAfter` details.
