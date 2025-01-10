
# Backfilling Data

When new captures are created, you often have the option of backfilling data. This captures data in its current state and then switches to capturing change events on an ongoing basis.

This is desirable in most cases, as it ensures that a complete view of your data is captured into Flow.

Also see how [schema evolution](https://docs.estuary.dev/concepts/advanced/evolutions/#what-do-schema-evolutions-do) can help with backfills when a source's schema becomes incompatible with the existing schema.

## Preventing backfills

Preventing backfills when possible can help save costs and computational resources. You may find it appropriate to skip the backfill, especially for extremely large datasets or tables.

In this case, many connectors allow you to turn off backfilling on a per-stream or per-table basis. See each individual connector's properties for details.

### Preventing backfills during database upgrades

It is common to want to prevent backfills when performing database maintenance, as database upgrades can kick off a new backfill with Flow. Whether or not a database upgrade automatically performs a backfill depends on the database itself.

During an upgrade, some databases invalidate a replication slot, binlog position, CDC tables, or similar. As Flow relies on these methods to keep its place, upgrades will disrupt the Flow pipeline in these cases.

If a database upgrade **will** affect these or similar resources, you can manually prevent a backfill.
If a database upgrade **will not** affect these resources, the Flow connector should simply resume when the upgrade completes.

For example, Postgres currently deletes or requires users to drop logical replication slots during a major version upgrade. To prevent a backfill during the upgrade, follow these steps:

1. Pause database writes so no further changes can occur.

2. Monitor the current capture to ensure captures are fully up-to-date.
   - These two steps ensure the connector won't miss any changes.

3. Perform the database upgrade.

4. Backfill each binding of the capture using the ["Only Changes" backfill mode](#backfill-modes).
   - This will not cause a full backfill. "Backfilling" the bindings resets the WAL (Write-Ahead Log) position for the capture, essentially resetting its place. The "Only Changes" mode will skip re-reading existing table content.

5. Resume database writes.

## Backfill modes

Some connectors, such as those working with SQL sources, allow fine-grained control of backfills for individual tables. These bindings include a "Backfill Mode" dropdown in their resource configuration. This setting then translates to a `mode` field for that resource in the specification. For example:

```yaml
"bindings": [
    {
      "resource": {
        "namespace": "public",
        "stream": "tableName",
        "mode": "Only Changes"
      },
      "target": "Artificial-Industries/postgres/public/tableName"
    }
  ]
```

:::warning
In general, you should not change this setting. Make sure you understand your use case, such as preventing backfills, as discussed above.
:::

The following modes are available:

* **Normal:** backfills chunks of the table and emits all replication events regardless of whether they occur within the backfilled portion of the table or not.

* **Precise:** backfills chunks of the table and filters replication events in portions of the table which haven't yet been reached.

* **Only Changes:** skips backfilling the table entirely and jumps directly to replication streaming for the entire dataset.

* **Without Primary Key:** can be used to capture tables without any form of unique primary key, but lacks the exact correctness properties of the normal backfill mode.

If you do not choose a specific backfill mode, Flow will default to an automatic mode.
