
# Customizing Data Flows

Estuary provides a number of options to customize your data flows beyond the default connector settings.
This section provides guides on customizing your data flows to your exact specifications.

While many customizations depend on the specific capture or materialization connector, there is some shared functionality between common connectors.
This page introduces some of these shared options.

See the relevant [connector reference pages](/reference/Connectors) to confirm which options your chosen connectors support.

## SQL Captures

CDC SQL capture connectors share a number of customization options in common.
Connectors that use this shared library include:

* [MariaDB](/reference/Connectors/capture-connectors/MariaDB)
* [MySQL](/reference/Connectors/capture-connectors/MySQL)
* [OracleDB](/reference/Connectors/capture-connectors/OracleDB)
* [PostgreSQL](/reference/Connectors/capture-connectors/PostgreSQL)
* [SQL Server](/reference/Connectors/capture-connectors/SQLServer)
* Variants of these connectors (Supabase, Amazon RDS for MySQL, etc)

***

### History Mode

When History Mode is enabled, change events are captured without being reduced.
This means that events are preserved individually for a complete _history_ of change events.
Effectively, History Mode ignores any [reduce annotations](/concepts/schemas/#reductions) on the capture.

To ignore reductions on the materialization side as well, you can pair a History Mode capture with a [Delta Updates](#delta-updates) materialization.
This lets you load a fully un-reduced history of change events to your destination.

While this setup supports use cases like audit trails, un-reduced documents will take up much more storage, especially for tables that experience frequent changes.

***

### Source Tag

If you provide a value in a capture's Source Tag field, that value will be populated in the `_meta/source` column on every row the capture writes.
For captures that write into the same collection, this feature helps note downstream which capture wrote a particular row.

You can edit the Source Tag in a capture's **Advanced Options** configuration.

***

## Materializations

Materializations also provide a number of customization options.
Some, like [field selection](./customize-materialization-fields.md), are built-in features common across all materializations.

The options listed below are common to some—but not all—materialization connectors.
Check your materialization's connector documentation to confirm compatibility.

***

### Delta Updates

If the Delta Updates option is enabled, data collection updates are streamed to the destination without being reduced.
This materialization option is similar to a capture's [History Mode](#history-mode)
and you may enable both for a completely un-reduced outcome in your destination.

In contrast to History Mode, which is enabled on a per-capture basis, Delta Updates can be enabled on a per-binding basis.
This provides more granularity and the ability to use both standard and delta updates as part of the same materialization.

You therefore manage Delta Updates in the materialization's **Source Collections** section rather than the **Endpoint Config**.

[Learn more about Delta Updates](/concepts/materialization/#delta-updates).

***

### Sync Schedule

For batch materializations, you can configure how frequently you want to sync data.
The **Sync Schedule** section in the Endpoint Config provides options on frequency and sync timeframes.

[Learn more about sync schedules](/reference/materialization-sync-schedule).
