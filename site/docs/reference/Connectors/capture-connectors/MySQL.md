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
| Value | Name | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| `address` | Address | IP address and port of the database host | String | Required |
| `user` | User | Database user to connect as | String | Required |
| `password` | Password | Password for the specified database user | string | Required |
| `dbname` | Database name | Name of the database to connect to | string | Required |
| `server_id` | Server ID | Server ID for replication | int | Required |
| `watermarks_table`| Watermarks Table | The name of the table used for watermark writes during backfills | string | `"flow.watermarks"` |

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

## Connecting to secure networks

The MySQL source connector [supports SSH tunneling](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
to allow Flow to connect to databases ports in secure networks.

To set up and configure your SSH server, see the [guide](../../../../guides/connect-network/).

## MySQL on Amazon RDS

Amazon Relational Database Service (RDS) is a managed web service providing cloud-based instances
of popular relational databases, including MySQL.

You can use this Flow connector for MySQL instances on RDS, but the setup requirements are different.

Estuary also recommends creating a [read replica](https://aws.amazon.com/rds/features/read-replicas/)
in RDS for use with Flow; however, it's not required.
You're able to apply the connector directly to the primary instance if you'd like.

### Setup

1. You'll need to configure secure access to the database to enable the Flow capture.
  Currently, Estuary supports SSH tunneling to allow this.
  Follow the guide to [configure an SSH server for tunneling](../../../../guides/connect-network/).

2. Create a RDS parameter group to enable replication on for MySQL.

  a. Create a [parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.html)
  with the following properties:
    * **Family**: mysql #FOR REVIEW: does version number matter here?
    * **Type**: DB Parameter group
    * **Name**: mysql-replication
    * **Description**: Database with replication enabled

  b. [Modify the new parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.html#USER_WorkingWithParamGroups.Modifying) and update the following parameters:
    * binlog_format: ROW
    * binlog_row_metadata: FULL
    * read_only: 0

  c. If using the primary instance (not recommended), [associate the parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.html#USER_WorkingWithParamGroups.Associating)
  with the database and reboot the database to allow the change to take effect

3. Create a read replica with the new parameter group applied (recommended).

  a. [Create a read replica](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ReadRepl.html#USER_ReadRepl.Create)
  of your MySQL database.

  b. [Modify the replica](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html)
  and set the following:
  * **DB parameter group**: choose the parameter group you created previously
  * **Backup retention period**: 7 days

  c. Reboot the replica to allow the changes to take effect.

4. Switch to your MySQL client. Run the following commands to create a new user for the capture with appropriate permissions
and set up the watermarks table:

```sql
CREATE DATABASE IF NOT EXISTS flow;
CREATE TABLE IF NOT EXISTS flow.watermarks (slot INTEGER PRIMARY KEY, watermark TEXT);
CREATE USER IF NOT EXISTS flow_capture
  IDENTIFIED BY 'secret'
  COMMENT 'User account for Flow MySQL data capture';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
GRANT SELECT ON *.* TO 'flow_capture';
GRANT INSERT, UPDATE, DELETE ON flow.watermarks TO 'flow_capture';
```

5. Run the following command to set the binary log retention to seven days:
```sql
CALL mysql.rds_set_configuration('binlog retention hours', 168);
```