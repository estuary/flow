This is a change data capture (CDC) connector that captures change events from a MySQL database via the [Binary Log](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html).

`ghcr.io/estuary/source-mysql:dev` provides the latest connector image when using the Flow GitOps environment.
You can also follow the link in your browser to see past image versions.

## Prerequisites
To use this connector, you'll need a MySQL database setup with the following:
* [`binlog_row_metadata`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_metadata)
  system variable set to `FULL`.
  - Note that this can be done on a dedicated replica even if the primary database has it set to `MINIMAL`.
* A watermarks table. The watermarks table is a small "scratch space"
  to which the connector occasionally writes a small amount of data (a UUID,
  specifically) to ensure accuracy when backfilling preexisting table contents.
  - By default this is named `"flow.watermarks"` but this can be overridden in `config.json`.
* A capture user with appropriate permissions:
  - `REPLICATION CLIENT` and `REPLICATION SLAVE` privileges.
  - Permission to insert/update/delete on the watermarks table.
  - Permission to read the tables being captured.
  - Permission to read from `information_schema` tables (if automatic discovery is used).

### Setup
To meet these requirements, do the following:

1. Create the watermarks table. This table can have any name and be in any database, so long as `config.json` is modified accordingly.
```sql
CREATE DATABASE IF NOT EXISTS flow;
CREATE TABLE IF NOT EXISTS flow.watermarks (slot INTEGER PRIMARY KEY, watermark TEXT);
```
2. Create the 'flow_capture' user with replication permission, the ability to read all tables, and the ability to read and write the watermarks table. The `SELECT` permission can be restricted to just the tables that need to be
captured, but automatic discovery requires `information_schema` access too.
```sql
CREATE USER IF NOT EXISTS flow_capture
  IDENTIFIED BY 'secret'
  COMMENT 'User account for Flow MySQL data capture';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
GRANT SELECT ON *.* TO 'flow_capture';
GRANT INSERT, UPDATE, DELETE ON flow.watermarks TO 'flow_capture';

SET PERSIST binlog_row_metadata = 'FULL';
```

## Configuration
There are various ways to configure and implement connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and code sample below provide configuration details specific to the PostgreSQL source connector.

### Values
TODO check value names
| Value | Name | Type | Required/Default | Details |
|-------|------|------|---------| --------|
| `address` | Address | string | Required | Database host:port to connect to |
| `user` | User | string | Required; `"flow_capture"` | Database user to connect as |
| `password` | Password | string | Required | Password for the specified database user |
| `dbname` | Database name | string | Required | Name of the database to connect to |
| `serverid` !! | Server ID | int | Required | Server ID for replication |
| `WatermarksTable` !| Watermarks Table | string | `"flow.watermarks"` | The name of the table used for watermark writes during backfills |