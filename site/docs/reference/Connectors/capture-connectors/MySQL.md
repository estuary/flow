This is a change data capture (CDC) connector that captures change events from a MySQL database via the [Binary Log](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html).

:::caution
This connector is still under development. Estuary does not currently guarantee
that it will behave as expected in all production environments.
:::

`ghcr.io/estuary/source-mysql:dev` provides the latest connector image when using the Flow GitOps environment.
You can also follow the link in your browser to see past image versions.

## Prerequisites
To use this connector, you'll need a MySQL database setup with the following:
* [`binlog_row_metadata`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_metadata)
  system variable set to `FULL`.
  - Note that this can be done on a dedicated replica even if the primary database has it set to `MINIMAL`.
* [Binary log expiration period](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_expire_logs_seconds) of at at least seven days.
If possible, it's recommended to keep the default setting of 2592000 seconds (30 days).
* A watermarks table. The watermarks table is a small "scratch space"
  to which the connector occasionally writes a small amount of data (a UUID,
  specifically) to ensure accuracy when backfilling preexisting table contents.
  - The default name is `"flow.watermarks"`, but this can be overridden in `config.json`.
* A capture user with appropriate permissions:
  - `REPLICATION CLIENT` and `REPLICATION SLAVE` privileges.
  - Permission to insert, update, and delete on the watermarks table.
  - Permission to read the tables being captured.
  - Permission to read from `information_schema` tables, if automatic discovery is used.

### Setup
To meet these requirements, do the following:

1. Create the watermarks table. This table can have any name and be in any database, so long as `config.json` is modified accordingly.
```sql
CREATE DATABASE IF NOT EXISTS flow;
CREATE TABLE IF NOT EXISTS flow.watermarks (slot INTEGER PRIMARY KEY, watermark TEXT);
```
2. Create the `flow_capture` user with replication permission, the ability to read all tables, and the ability to read and write the watermarks table.

  The `SELECT` permission can be restricted to just the tables that need to be
  captured, but automatic discovery requires `information_schema` access as well.
```sql
CREATE USER IF NOT EXISTS flow_capture
  IDENTIFIED BY 'secret'
  COMMENT 'User account for Flow MySQL data capture';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
GRANT SELECT ON *.* TO 'flow_capture';
GRANT INSERT, UPDATE, DELETE ON flow.watermarks TO 'flow_capture';
```
3. Configure the binary log to record complete table metadata.
```sql
SET PERSIST binlog_row_metadata = 'FULL';
```
4. Configure the binary log to retain data for at least seven days, if previously set lower.
```sql
SET PERSIST binlog_expire_logs_seconds = 604800;
```

## Configuration
There are various ways to configure and implement connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and code sample below provide configuration details specific to the MySQL source connector.

### Values

#### Endpoint

| Value | Name | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| `address` | Address | IP address and port of the database host. | String | Required |
| `user` | User | Database user to connect as. | String | Required |
| `password` | Password | Password for the specified database user. | string | Required |
| `dbname` | Database name | Name of the database to connect to. | string | Required |
| `server_id` | Server ID | Server ID for replication. | int | Required |
| `watermarks_table`| Watermarks Table | The name of the table used for watermark writes during backfills. | string | `"flow.watermarks"` |

#### Bindings

| Value | Name | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| `namespace` | Namespace | The [namespace](https://dev.mysql.com/doc/refman/5.6/en/ha-memcached-using-namespaces.html) of the table, if used. | string | |
| `stream` | Stream | Table name. | string | Required |
| `syncMode` | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample
A minimal capture definition within the catalog spec will look like the following:

```yaml
captures:
  ${TENANT}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-mysql:dev
        config:
          address: "127.0.0.1:3306"
          dbname: "test"
          password: "secret"
          server_id: 12345
          user: "flow_capture"
          watermarks_table: "flow.watermarks"
    bindings:
      - resource:
          namespace: ${TABLE_NAMESPACE}
          stream: ${TABLE_NAME}
          syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}

```
Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures).
