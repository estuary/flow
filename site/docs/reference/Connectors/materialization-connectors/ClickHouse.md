

# ClickHouse

This connector materializes Estuary collections into tables in a ClickHouse database.

[ClickHouse](https://clickhouse.com/) is a column-oriented OLAP database designed for real-time analytics.
This connector writes directly to ClickHouse using the native protocol.

Estuary also provides a [Dekaf-based integration](./Dekaf/clickhouse.md) for users who prefer to ingest via ClickPipes.

## Prerequisites

To use this connector, you'll need:

* A ClickHouse database (self-hosted or ClickHouse Cloud) with a user that has permissions to create tables and write data.
* The connector uses the ClickHouse native protocol (port 9000 by default, not the HTTP interface on port 8123).
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
| **`/address`** | Address | Host and port of the database, in the form of `host[:port]`. Port 9000 is used as the default if no specific port is provided. | string | Required |
| **`/credentials`** | Authentication | | object | Required |
| **`/credentials/auth_type`** | Auth Type | Authentication type. Must be `user_password`. | string | Required |
| **`/credentials/username`** | Username | Database username. | string | Required |
| **`/credentials/password`** | Password | Database password. | string | Required |
| **`/database`** | Database | Name of the ClickHouse database to materialize to. | string | Required |
| `/hardDelete` | Hard Delete | If enabled, the connector inserts tombstone rows with `_is_deleted = 1` when source documents are deleted, causing them to be excluded from `FINAL` queries. By default, source deletions are ignored at the destination. | boolean | `false` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Name of the database table to materialize to. The connector will create the table if it doesn't already exist. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        config:
          address: clickhouse.example.com:9000
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

The connector creates tables using the [ReplacingMergeTree engine](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree). Updated records are actually inserted as duplicates; ClickHouse later deduplicates these as a background process.

Your queries should use the `FINAL` directive to get deduplicated results, and include the predicate `_is_deleted = 0` to ignore deleted records.

```sql
SELECT * FROM my_table FINAL WHERE _is_deleted = 0;
```

## Hard deletes

All tables are created with `_version` (UInt64) and `_is_deleted` (UInt8) columns used internally by the `ReplacingMergeTree` engine.

If you set `hardDelete: true` in the endpoint configuration, the connector inserts a **tombstone row** when a source document is deleted. The tombstone has `_is_deleted = 1`, the same key columns as the original row, and zero values for all other columns. The `ReplacingMergeTree` engine then uses `_is_deleted` to hide these rows from `FINAL` queries, and eventually removes the tombstoned records from the table.

## Soft deletes not supported

Source deletions are effectively ignored at the destination.

## Delta updates not supported

This connector does not support [delta updates](/concepts/materialization/#delta-updates). Only standard (merge) mode is supported.
