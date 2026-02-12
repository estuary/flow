# Schema Evolution

When collection specifications and schemas change, you must make corresponding changes in other parts of your Data Flow to avoid errors. In this guide, you'll learn how to respond to different types of collection changes.

Manual methods (using flowctl) as well as features available in the Estuary web app are covered here.
For an in-depth overview of the automatic schema evolution feature in the web app and how it works, see [this article](../concepts/advanced/evolutions.md).

## Introduction

Estuary [collections](../concepts/collections.md) serve not only as your real-time data storage, but also as a contract between tasks that produce and consume their data. **Captures** are producers, **materializations** are consumers, and **derivations** can act as either.

This contract helps prevent data loss and error in your Data Flows, and is defined in terms of the collection specification, or spec, which includes:

* The JSON schema
* The collection `key`
* [Projections](../concepts/advanced/projections.md), if any

There are many reasons a collection spec might change. Often, it's due to a change in the source data. Regardless, you'll need to make changes to downstream tasks — most often, materializations — to avoid errors.

## Schema evolution scenarios

This guide is broken down into sections for different common scenarios, depending on which properties of the collection spec have changed.

- [The `key` pointers have changed](#re-creating-a-collection)
- [The logical partitioning configuration has changed](#re-creating-a-collection)
- The `schema` (or `readSchema` if defined separately) has changed
    - [A new field is added](#a-new-field-is-added)
    - [A field's data type has changed](#a-fields-data-type-has-changed)
    - [A field was removed](#a-field-was-removed)

:::info
There are a variety of reasons why these properties may change, and also different mechanisms for detecting changes in source data. In general, it doesn't matter why the collection spec has changed, only _what_ has changed. However, [AutoDiscovers](../concepts/captures.md#automatically-update-captures) are able to handle some of these scenarios automatically. Where applicable, AutoDiscover behavior will be called out under each section.
:::

### Re-creating a collection

*Scenario: the `key` pointer or logical partitioning configurations have changed.*

The `key` of an Estuary collection cannot be changed after the collection is created. The same is true of the logical partitioning, which also cannot be changed after the collection is created.

If you need to change either of those parts of a collection spec, you'll need to create a new collection and update the bindings of any captures or materializations that reference the old collection.

**Web app workflow**

If you're working in the Estuary web app, you'll see an error message and an option to re-create the collection as shown in the example below.

![](./guide-images/evolution-re-create-ui.png)

Click **Apply** to re-create the collection and update any tasks that reference the old collection with the new name.

**flowctl workflow:**

If you're working with flowctl, you'll need to re-create the collection manually in your `flow.yaml` file. You must also update any captures or materializations that reference it. For example, say you have a data flow defined by the following specs:

```yaml
captures:
  acmeCo/inventory/source-postgres:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:v1
        config: encrypted-pg-config.sops.yaml
    bindings:
      - resource:
          namespace: public
          stream: anvils
          mode: Normal
        target: acmeCo/inventory/anvils

collections:
  acmeCo/inventory/anvils:
    key: [/sku]
    schema:
      type: object
      properties:
        sku: { type: string }
        warehouse_id: { type: string }
        quantity: { type: integer }
      required: [sku, warehouse_id, quantity]

materializations:
  acmeCo/data-warehouse/materialize-snowflake:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-snowflake:v1
        config: encrypted-snowflake-config.sops.yaml
    bindings:
      - source: acmeCo/inventory/anvils
        resource:
          table: anvils
          schema: inventory
```

To change the collection key, you would update the YAML like so. Note the capture `target`, collection name, and materialization `source`.

```yaml
captures:
  acmeCo/inventory/source-postgres:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:v1
        config: encrypted-pg-config.sops.yaml
    bindings:
      - resource:
          namespace: public
          stream: anvils
          mode: Normal
        backfill: 1
        target: acmeCo/inventory/anvils_v2

collections:
  acmeCo/inventory/anvils_v2:
    key: [/sku]
    schema:
      type: object
      properties:
        sku: { type: string }
        warehouse_id: { type: string }
        quantity: { type: integer }
      required: [sku, warehouse_id, quantity]

materializations:
  acmeCo/data-warehouse/materialize-snowflake:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-snowflake:v1
        config: encrypted-snowflake-config.sops.yaml
    bindings:
      - source: acmeCo/inventory/anvils_v2
        backfill: 1
        resource:
          table: anvils
          schema: inventory
```

The existing `acmeCo/inventory/anvils` collection will not be modified and will remain in place, but won't update because no captures are writing to it.

 Also note the addition of the `backfill` property. If the `backfill` property already exists, just increment its value. For the materialization, this will ensure that the destination table in Snowflake gets dropped and re-created, and that the materialization will backfill it from the beginning. In the capture, it similarly causes it to start over from the beginning, writing the captured data into the new collection.

**Auto-Discovers:**

If you enabled the option to [**Automatically keep schemas up to date** (`autoDiscover`)](../concepts/captures.md#automatically-update-captures) and selected **Breaking change re-versions collections** (`evolveIncompatibleCollections`) for the capture, this evolution would be performed automatically.

### A new field is added

*Scenario: this is one way in which the schema can change.*

When a new field appears in the collection schema, it _may_ automatically be added to any materializations that use `recommended` fields. Recommended fields are enabled by default in each binding. See [the materialization docs](/concepts/materialization/#projected-fields) for more info about how to enable or disable `recommended` fields.

When recommended fields are enabled, new fields are added automatically if they meet the criteria for the particular materialization connector. For example, scalar fields (strings, numbers, and booleans) are considered "recommended" fields when materializing to database tables.

If your materialization binding is set to `recommended: false`, or if the new field is not recommended, you can manually add it to the materialization.

To manually add a field:

* **In the web app,** [edit the materialization](/guides/edit-data-flows/#edit-a-materialization), find the affected binding, and click **Show Fields**.
* **Using flowctl,** add the field to `fields.include` in the materialization specification as shown [here](/concepts/materialization/#projected-fields).

:::info
Newly added fields will not be set for rows that have already been materialized. If you want to ensure that all rows have the new field, just increment the `backfill` counter in the affected binding to have it re-start from the beginning.
:::

### A field's data type has changed

*Scenario: this is one way in which the schema can change.*

When a field's data type has changed, the effect on your materialization depends on the specific connector you're using.

:::warning
Note that these restrictions only apply to fields that are actively being materialized. If a field is [excluded from your materialization](/concepts/materialization/#projected-fields), either explicitly or because it's not recommended, then the data types may change in any way.

Regardless of whether the field is materialized or not, it must still pass schema validation tests. Therefore, you must still make sure existing data remains valid against the new schema. For example, if you changed `excluded_field: { type: string }` to `type: integer` while there was existing data with string values, your materialization would fail due to a schema validation error.
:::

Database and data warehouse materializations tend to be somewhat restrictive about changing column types. They typically only allow dropping `NOT NULL` constraints. This means that you can safely change a schema to make a required field optional, or to add `null` as a possible type, and the materialization will continue to work normally.  Most other types of changes will require materializing into a new table.

The best way to find out whether a change is acceptable to a given connector is to run a test or attempt to re-publish. Failed attempts to publish won't affect any tasks that are already running.

**Web app workflow**

If you're working in Estuary's web app, and attempt to publish a change that's unacceptable to the connector, you'll see an error message and an offer to increment the necessary `backfill` counters, or, in rare cases, to re-create the collection.

Click **Apply** to to accept this solution and continue to publish.

**flowctl workflow**

If you test or attempt to publish a change that's unacceptable to the connector, you'll see an error message pointing to the field that's changed. In most cases, you can work around the issue by manually updating the materialization to materialize into a new table.

For example, say you have a data flow defined by the following specs:

```yaml
collections:
  acmeCo/inventory/anvils:
    key: [/sku]
    schema:
      type: object
      properties:
        sku: { type: string }
        quantity: { type: integer }
        description: { type: string }
      required: [sku, quantity]

materializations:
  acmeCo/data-warehouse/materialize-snowflake:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-snowflake:v1
        config: encrypted-snowflake-config.sops.yaml
    bindings:
      - source: acmeCo/inventory/anvils
        backfill: 3
        resource:
          table: anvils
          schema: inventory
```

Let's say the type of `description` was broadened to allow `object` values in addition to `string`. You'd update your specs as follows:

```yaml
collections:
  acmeCo/inventory/anvils:
    key: [/sku]
    schema:
      type: object
      properties:
        sku: { type: string }
        quantity: { type: integer }
        description: { type: [string, object] }
      required: [sku, quantity]

materializations:
  acmeCo/data-warehouse/materialize-snowflake:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-snowflake:v1
        config: encrypted-snowflake-config.sops.yaml
    bindings:
      - source: acmeCo/inventory/anvils
        backfill: 4
        resource:
          table: anvils
          schema: inventory
```

Note that the only change was to increment the `backfill` counter. If the previous binding spec did not specify `backfill`, then just add `backfill: 1`.

This works because the type is broadened, so existing values will still validate against the new schema. If this were not the case, then you'd likely need to [re-create the whole collection](#re-creating-a-collection).

**Auto-Discovers:**

If you enabled the option to [**Automatically keep schemas up to date** (`autoDiscover`)](../concepts/captures.md#automatically-update-captures) and selected **Breaking change re-versions collections** (`evolveIncompatibleCollections`) for the capture, this evolution would be performed automatically.

### A field was removed

*Scenario: this is one way in which the schema can change.*

Removing fields is generally allowed by all connectors, and does not require new tables or collections. Note that for database materializations, the existing column will _not_ be dropped, and will just be ignored by the materialization going forward. A `NOT NULL` constraint would be removed from that column, but it will otherwise be left in place.

## How schema evolution works

Understanding how Estuary handles schemas helps you troubleshoot issues and make informed decisions about backfills.

### Write schemas vs. read schemas

Collections maintain two separate schemas:

- **Write schema**: Constrains new documents coming from captures and derivations
- **Read schema**: Validates existing documents when they're consumed by materializations

These operate independently. When a capture discovers a schema change, it updates the write schema for new documents. Existing documents in the collection were written under older schemas and remain valid under the read schema.

This separation is why new columns show `NULL` for historical data—those documents were captured before the column existed.

### Auto-inference only widens

Estuary's schema inference creates "shrink-wrap" schemas that accommodate all document variations seen so far. Importantly, **inference only widens schemas, never narrows them**.

**Compatible changes (collection level):**
- Adding a new field
- Changing `required` to optional
- Widening types (e.g., `integer` to `number`)

**Incompatible changes (require backfill):**
- Narrowing types (e.g., `string` to `integer`)
- Adding a `required` constraint
- Changing key pointers or partitioning

:::note
Even when a change is compatible at the collection level, your destination connector may require a backfill to alter the table schema. For example, widening `integer` to `bigint` is compatible in the collection, but some databases need a backfill to apply the column type change.
:::

:::tip
For a deep dive into the architecture of schema evolution, see [Discussion #1988: Collection Evolution and Source-Defined Schema](https://github.com/estuary/flow/discussions/1988).
:::

## Troubleshooting

### Why are my new columns showing NULL values?

When you add a new column to a source table, existing documents in Estuary collections were captured *before* that column existed. The new column will only have values for documents captured *after* the schema change.

**Example:**
1. You add column `is_active` to your source table
2. Estuary detects the change and updates the collection schema
3. New documents include `is_active`, but historical documents don't have it
4. When materialized, historical rows show `NULL` for `is_active`

**Solution:** Trigger a backfill to recapture historical data with the new schema.

- **UI**: Go to your capture → Click **Backfill** on the affected binding
- **flowctl**: Increment the `backfill` counter in your capture spec and republish

:::tip
For very large tables, consider whether you need historical values for the new column, or if `NULL` for older records is acceptable.
:::

### My data stopped flowing after a schema change

If data stops flowing after a schema change at your source, work through these checks:

1. **Is the capture running?**
   - Check the hourly data graph and logs in the UI
   - Most schema changes (like adding columns) are handled automatically—the capture briefly restarts to pick up the new schema

2. **Is the materialization running?**
   - Check the hourly data graph and logs in the UI
   - If stopped, look for schema validation errors in the logs

3. **Did you see a "Changes rejected" prompt?**
   - When schema changes would break downstream tasks, the UI shows: *"Changes rejected due to incompatible collection updates"*
   - Click **Apply** to automatically update downstream tasks (this may trigger backfills)

**Common causes and solutions:**

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Transient "document failed validation" error | Schema inference racing with new records | See [Transient validation errors](#transient-validation-errors-during-schema-changes) section below |
| "Changes rejected due to incompatible collection updates" | Incompatible schema change detected | Click **Apply** to update downstream tasks |
| "Unsupported operation DROP TABLE" | Destructive DDL change | Disable capture, remove binding, re-enable |
| Data flows but destination unchanged | Processing delay or materialization paused | Check task status in UI |

### Transient validation errors during schema changes

When your source data changes shape, you may see errors like:

```
document failed validation against its collection JSON Schema
```

**Why this happens:**

Schema updates and new data records are processed through separate paths. When a source schema changes:

1. The capture detects the change and publishes an updated collection schema
2. New records (with the new shape) start flowing immediately
3. The materialization receives these new records
4. If a record arrives *before* the schema update is applied, validation fails

This is a side effect of a low-latency system—we don't artificially delay records, so the schema change can arrive slightly after the first records with the new shape. The system is designed to recover automatically.

**How to identify transient vs. persistent failures:**

| Indicator | Transient (wait) | Persistent (action needed) |
|-----------|------------------|---------------------------|
| Timing | Started during/after a source schema change | No recent schema changes |
| Duration | Resolves within 5-10 minutes | Persists beyond 30 minutes |
| Error pattern | Intermittent, then stops | Continuous, every record fails |
| Materialization status | Restarts automatically | Stays failed or loops |

**What happens during recovery:**

1. The materialization encounters validation errors and restarts
2. On restart, it picks up the updated collection schema
3. Processing resumes normally with the new schema
4. No data is lost—failed records are retried after the schema update

**When to take action:**

- **Wait 15-30 minutes** before investigating further
- **Check the logs** for the specific validation error—if it references a field that was just added/changed, it's likely transient
- **If errors persist**, verify your source schema change was compatible (see [Auto-inference only widens](#auto-inference-only-widens) above)
- **If you made an incompatible change**, you may need to trigger a backfill or adjust your schema

:::tip
High-volume sources with frequent schema changes may see these transient errors more often. If this becomes disruptive, consider batching schema changes during low-traffic periods.
:::

### Configuring automatic schema change handling

You can configure how materializations respond to incompatible schema changes using the `onIncompatibleSchemaChange` setting.

**In the Flow web app:**

1. Edit your materialization
2. Go to **Collections**
3. Click **Config** on a collection
4. Expand **Advanced Options**
5. Set **Incompatible Schema Change** to your preferred action

**Available options:**

| Option | Behavior |
|--------|----------|
| **backfill** *(default)* | Automatically backfill and re-materialize the affected binding |
| **abort** | Fail the publication, preventing incompatible changes |
| **disableBinding** | Disable only the affected binding until manually re-enabled |
| **disableTask** | Disable the entire materialization until manually re-enabled |

**Using flowctl:**

Set the field at the top level (applies to all bindings) or per-binding:

```yaml
materializations:
  myPrefix/my-materialization:
    onIncompatibleSchemaChange: disableTask  # or: backfill (default), abort, disableBinding
    bindings:
      - source: myPrefix/my-collection
        onIncompatibleSchemaChange: abort  # Override for this binding
        resource:
          table: my_table
```

:::note
These behaviors only trigger during automated actions like AutoDiscover. Manual changes via the UI will prompt you to choose an action.
:::

### Type inference issues with NoSQL sources

NoSQL databases like MongoDB allow flexible schemas where the same field can have different types across documents. Estuary infers a schema based on the data it observes, which can lead to unexpected type widening.

**Common issue:** "My date field is being stored as a string"

**Why this happens:**
1. The first documents captured had the field as a string value (e.g., `"2024-01-15"`)
2. Estuary inferred the type as `string`
3. Later documents with proper Date types are converted to strings to match the inferred schema

Remember: auto-inference only widens, never narrows. Once a field is inferred as `string`, it won't narrow to `date`—even if all subsequent documents use the correct type.

**Options:**

1. **Accept the widened type**: In many cases, the widened type works fine downstream. String dates can still be parsed by your destination or BI tools. Evaluate whether this actually causes problems before investing effort in a fix.

2. **Fix at the source, then backfill**: The cleanest solution when the type matters:
   - Correct the inconsistent data types in your source database
   - Trigger a [dataflow reset](/reference/backfilling-data/#dataflow-reset) to re-infer the schema from scratch
   - The backfill recaptures all data with consistent types, and the schema is re-inferred correctly

3. **Use a derivation** *(last resort)*: If you can't fix the source data, create a derivation to transform the field:
   ```sql
   SELECT
     _id,
     PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', created_at) as created_at
   FROM source_collection
   ```
   This adds complexity to your pipeline, so only use this when options 1 and 2 aren't viable.

:::tip
When setting up captures from schema-flexible sources, review the inferred schema before creating materializations. If types look incorrect, fix the source data and backfill *before* proceeding—it's much easier to get the schema right from the start.
:::

### Schema not yet available for new collections

When creating a materialization for a newly captured collection, you may see:

```
Schema not yet available for collection '{collection}'
```

**Why this happens:**

Schema inference requires data to flow through the capture before it can determine the schema. For new captures:
1. The capture starts and begins reading from the source
2. Documents are captured and schema inference analyzes them
3. The inferred schema is published to the collection
4. Only then can materializations validate against the schema

**Solution:**

1. **Wait 5-10 minutes** for the capture to process initial documents
2. **Check capture logs** to confirm data is flowing
3. **Re-publish the materialization** to pick up the inferred schema

If the issue persists after 30 minutes:
- Verify your source has data to capture
- Check capture logs for connection or permission errors

### Binding not found

When editing or publishing a capture, you may see:

```
Binding not found for '{binding_name}'
```

**Common causes:**

1. **Source table was renamed or deleted**
   - Check if the table still exists in your source system
   - If renamed, remove the old binding and add the new table name

2. **Insufficient permissions**
   - Verify the capture's credentials have access to the table
   - For databases, check SELECT permissions on the specific table/schema

3. **Table not yet discovered**
   - Click **Refresh** on your capture to re-run discovery
   - New tables may take a few minutes to appear

4. **Case sensitivity**
   - Some databases are case-sensitive for table names
   - Ensure the binding name matches the exact table name

**To resolve:**

1. Go to your capture and click **Edit**
2. Click **Refresh** to re-discover available tables
3. If the table appears, re-add it as a binding
4. If not, check source permissions and table existence

### Unsupported DDL operations (MySQL/MariaDB CDC)

MySQL and MariaDB CDC captures read changes from the binary log. Certain DDL operations cannot be processed from the binlog:

```
Unsupported operation ALTER TABLE for table '{table_name}'
```

**Why this happens:**

The binary log contains row-level changes, but some DDL operations (like `ALTER TABLE`) don't include enough information to reconstruct the new schema. When the capture encounters these, it cannot continue processing that table.

**Solution - Remove and re-add the binding:**

1. **Edit your capture** and remove the affected table binding
2. **Save and publish** - this preserves other bindings
3. **Edit again** and re-add the table binding
4. **Trigger a backfill** on the materialization for this table

This resets the CDC position for only this table. Other tables continue from their current position.

**To avoid this issue:**

- Use `pt-online-schema-change` or `gh-ost` for schema migrations (these use row-based operations)
- Or temporarily pause the capture before DDL operations

:::note
This only affects MySQL/MariaDB CDC captures. PostgreSQL and SQL Server handle DDL differently.
:::
