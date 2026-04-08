

# ClickHouse

This connector materializes Estuary collections into tables in a ClickHouse database.

[ClickHouse](https://clickhouse.com/) is a column-oriented OLAP database designed for real-time analytics.
This connector writes batches directly to ClickHouse using the
[Native protocol](https://clickhouse.com/docs/interfaces/tcp) and
[Native format](https://clickhouse.com/docs/interfaces/formats/Native).

Estuary also provides a [Dekaf-based integration](./Dekaf/clickhouse.md) for users who prefer to ingest via ClickPipes.

## Prerequisites

To use this connector, you'll need:

* A ClickHouse database (self-hosted or ClickHouse Cloud) with a user that has permissions to create tables and write data.
* The connector uses the ClickHouse native protocol. The default port is **9440** (TLS enabled, the default) or **9000** (TLS disabled). It does not use the HTTP interface on port 8123.
* At least one Estuary collection.

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

## Configuration

To use this connector, begin with data in one or more Estuary collections.
Use the below properties to configure a ClickHouse materialization, which will direct the contents of these collections into ClickHouse tables.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/address`** | Address | Host and port of the database, in the form of `host[:port]`. Port 9440 is used as the default when SSL is enabled (the default), or 9000 when SSL is disabled. | string | Required |
| **`/credentials`** | Authentication | | object | Required |
| **`/credentials/auth_type`** | Auth Type | Authentication type. Must be `user_password`. | string | Required |
| **`/credentials/username`** | Username | Database username. | string | Required |
| **`/credentials/password`** | Password | Database password. | string | Required |
| **`/database`** | Database | Name of the ClickHouse database to materialize to. | string | Required |
| `/hardDelete` | Hard Delete | If enabled, items deleted in the source will also be deleted from the destination. By default, deletions are tracked via `_meta/op` (soft-delete). | boolean | `false` |
| `/advanced/sslmode` | SSL Mode | Controls the TLS connection behavior. Options: `disable`, `require`, `verify-full`. | string | `verify-full` |
| `/advanced/no_flow_document` | Exclude Flow Document | When enabled, the root document column will not be required for standard updates. | boolean | `false` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Name of the database table to materialize to. The connector will create the table if it doesn't already exist. | string | Required |
| `/delta_updates` | Delta Update | Should updates to this table be done via delta updates. | boolean | `false` |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        config:
          address: clickhouse.example.com:9440
          credentials:
            auth_type: user_password
            username: flow_user
            password: secret
          database: my_database
        image: ghcr.io/estuary/materialize-clickhouse:v1
    bindings:
      - resource:
          table: my_table
        source: ${PREFIX}/${source_collection}
```

## ReplacingMergeTree and FINAL

In standard (non-delta) mode, the connector creates tables using the [ReplacingMergeTree engine](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree) with `flow_published_at` as the version column.
Updated records are inserted as new rows; ClickHouse deduplicates them in a background process, keeping the row with the highest `flow_published_at` value for each key.

The connector also configures automatic background cleanup merges so that superseded rows and tombstones are eventually removed from disk.

Your queries should use the `FINAL` directive to get results with duplicate and tombstone rows removed:

```sql
SELECT * FROM my_table FINAL;
```

## Hard deletes

When `hardDelete: true` is set in the endpoint configuration, the connector adds an `_is_deleted` (UInt8) column to each table.
When a source document is deleted, the connector inserts a **tombstone row** with `_is_deleted = 1` and the same key columns as the original row.
The `ReplacingMergeTree` engine uses `_is_deleted` to exclude these rows from `FINAL` queries, and automatic cleanup merges eventually remove the tombstoned records from disk.

## Soft deletes

By default (when `hardDelete` is not enabled), source deletions are tracked in the destination via the `_meta/op` column, which indicates whether a row was created, updated, or deleted. The row itself remains in the table.

## Delta updates

This connector supports [delta updates](/concepts/materialization/#delta-updates) on a per-binding basis. When `delta_updates` is enabled for a binding, the table uses the `MergeTree` engine instead of `ReplacingMergeTree`. Every store operation is appended as-is with no deduplication — rows accumulate and are never removed.
