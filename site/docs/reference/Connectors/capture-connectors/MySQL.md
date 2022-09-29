---
sidebar_position: 7
---

This is a change data capture (CDC) connector that captures change events from a MySQL database via the [Binary Log](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-mysql:dev`](https://github.com/estuary/connectors/pkgs/container/source-mysql) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites
To use this connector, you'll need a MySQL database setup with the following:
* [`binlog_format`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format)
  system variable set to `ROW` (the default value).
* [Binary log expiration period](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_expire_logs_seconds) set to MySQL's default value of 30 days (2592000 seconds) if at all possible.
  - This value may be set lower if necessary, but we [strongly discourage](#insufficient-binlog-retention) going below 7 days as this may increase the likelihood of unrecoverable failures.
* A watermarks table. The watermarks table is a small "scratch space"
  to which the connector occasionally writes a small amount of data (a UUID,
  specifically) to ensure accuracy when backfilling preexisting table contents.
  - The default name is `"flow.watermarks"`, but this can be overridden in `config.json`.
* A database user with appropriate permissions:
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
3. Configure the binary log to retain data for the default MySQL setting of 30 days, if previously set lower.
```sql
SET PERSIST binlog_expire_logs_seconds = 2592000;
```

## Backfills and performance considerations

When the a MySQL capture is initiated, by default, the connector first *backfills*, or captures the targeted tables in their current state. It then transitions to capturing change events on an ongoing basis.

This is desirable in most cases, as in ensures that a complete view of your tables is captured into Flow.
However, you may find it appropriate to skip the backfill, especially for extremely large tables.

In this case, you may turn of backfilling on a per-table basis. See [properties](#properties) for details.

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the MySQL source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/address`** | Server Address | The host or host:port at which the database can be reached. | string | Required |
| **`/user`** | Login User | The database user to authenticate as. | string | Required, `"flow_capture"` |
| **`/password`** | Login Password | Password for the specified database user. | string | Required |
| `/advanced/watermarks_table` | Watermarks Table Name | The name of the table used for watermark writes. Must be fully-qualified in &#x27;&lt;schema&gt;.&lt;table&gt;&#x27; form. | string | `"flow.watermarks"` |
| `/advanced/dbname` | Database Name | The name of database to connect to. In general this shouldn&#x27;t matter. The connector can discover and capture from all databases it&#x27;s authorized to access. | string | `"mysql"` |
| `/advanced/node_id` | Node ID | Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn&#x27;t matter so long as it is unique. If unset or zero the connector will pick a value. | integer |  |
| `/advanced/skip_backfills` | Skip Backfills | A comma-separated list of fully-qualified table names which should not be backfilled. | string |  |
| `/advanced/skip_binlog_retention_check` | Skip Binlog Retention Sanity Check | Bypasses the &#x27;dangerously short binlog retention&#x27; sanity check at startup. Only do this if you understand the danger and have a specific need. | boolean |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| **`/namespace`** | Namespace | The [database/schema](https://dev.mysql.com/doc/refman/8.0/en/show-databases.html) in which the table resides. | string | Required |
| **`/stream`** | Stream | Name of the table to be captured from the database. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental`. | string | Required |

:::info
When you configure this connector in the web application, the automatic **discovery** process sets up a binding for _most_ tables it finds in your database, but there are exceptions.

Tables in the MySQL system schemas `information_schema`, `mysql`, `performance_schema`, and `sys` will not be discovered.
You can add bindings for such tables manually.
:::

### Sample
A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-mysql:dev
        config:
          address: "127.0.0.1:3306"
          user: "flow_capture"
          password: "secret"
    bindings:
      - resource:
          namespace: ${TABLE_NAMESPACE}
          stream: ${TABLE_NAME}
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}
```

Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures).

## MySQL on managed cloud platforms

In addition to standard MySQL, this connector supports cloud-based MySQL instances on certain platforms.

### Amazon RDS

You can use this connector for MySQL instances on Amazon RDS using the following setup instructions.

Estuary recommends creating a [read replica](https://aws.amazon.com/rds/features/read-replicas/)
in RDS for use with Flow; however, it's not required.
You're able to apply the connector directly to the primary instance if you'd like.

#### Setup

1. Allow connections to the database from the Estuary Flow IP address.

   1. Edit the VPC security group associated with your database, or create a new VPC security group and associate it with the database.
      Refer to the [steps in the Amazon documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create).

   2. Create a new inbound rule and a new outbound rule that allow all traffic from the IP address `34.121.207.128`.

   :::info
   Alternatively, you can allow secure connections via SSH tunneling. To do so:
     * Follow the guide to [configure an SSH server for tunneling](../../../../guides/connect-network/)
     * When you configure your connector as described in the [configuration](#configuration) section above,
        including the additional `networkTunnel` configuration to enable the SSH tunnel.
        See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
        for additional details and a sample.
   :::

2. Create a RDS parameter group to enable replication in MySQL.

   1. [Create a parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Creating).
   Create a unique name and description and set the following properties:
      * **Family**: mysql 8.0
      * **Type**: DB Parameter group

   2. [Modify the new parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Modifying) and update the following parameters:
      * binlog_format: ROW
      * binlog_row_metadata: FULL
      * read_only: 0

   3. If using the primary instance  (not recommended), [associate the  parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Associating)
   with the database and set [Backup Retention Period](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.html#USER_WorkingWithAutomatedBackups.Enabling) to 7 days.
   Reboot the database to allow the changes to take effect.

3. Create a read replica with the new parameter group applied (recommended).

   1. [Create a read replica](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ReadRepl.html#USER_ReadRepl.Create)
   of your MySQL database.

   2. [Modify the replica](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html)
   and set the following:
      * **DB parameter group**: choose the parameter group you created previously
      * **Backup retention period**: 7 days

   3. Reboot the replica to allow the changes to take effect.

4. Switch to your MySQL client. Run the following commands to create a new user for the capture with appropriate permissions,
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

5. Run the following command to set the binary log retention to 7 days, the maximum value which RDS MySQL permits:
```sql
CALL mysql.rds_set_configuration('binlog retention hours', 168);
```

6. In the [RDS console](https://console.aws.amazon.com/rds/), note the instance's Endpoint and Port. You'll need these for the `address` property when you configure the connector.

### Google Cloud SQL

You can use this connector for MySQL instances on Google Cloud SQL using the following setup instructions.

#### Setup

1. Allow connections to the database from the Estuary Flow IP address.

   1. [Enable public IP on your database](https://cloud.google.com/sql/docs/mysql/configure-ip#add) and add
      `34.121.207.128` as an authorized IP address.

   :::info
   Alternatively, you can allow secure connections via SSH tunneling. To do so:
     * Follow the guide to [configure an SSH server for tunneling](../../../../guides/connect-network/)
     * When you configure your connector as described in the [configuration](#configuration) section above,
        including the additional `networkTunnel` configuration to enable the SSH tunnel.
        See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
        for additional details and a sample.
   :::

2. Set the instance's `binlog_expire_logs_seconds` [flag](https://cloud.google.com/sql/docs/mysql/flags?_ga=2.8077298.-1359189752.1655241239&_gac=1.226418280.1655849730.Cj0KCQjw2MWVBhCQARIsAIjbwoOczKklaVaykkUiCMZ4n3_jVtsInpmlugWN92zx6rL5i7zTxm3AALIaAv6nEALw_wcB)
to `2592000`.

3. Using [Google Cloud Shell](https://cloud.google.com/sql/docs/mysql/connect-instance-cloud-shell) or your preferred client, create the watermarks table.
```sql
CREATE DATABASE IF NOT EXISTS flow;
CREATE TABLE IF NOT EXISTS flow.watermarks (slot INTEGER PRIMARY KEY, watermark TEXT);
```

4. Create the `flow_capture` user with replication permission, the ability to read all tables, and the ability to read and write the watermarks table.

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
5. In the Cloud Console, note the instance's host under Public IP Address. Its port will always be `3306`.
Together, you'll use the host:port as the `address` property when you configure the connector.

### Azure Database for MySQL

You can use this connector for MySQL instances on Azure Database for MySQL using the following setup instructions.

#### Setup

1. Allow connections to the database from the Estuary Flow IP address.

   1. Create a new [firewall rule](https://docs.microsoft.com/en-us/azure/mysql/flexible-server/how-to-manage-firewall-portal#create-a-firewall-rule-after-server-is-created)
   that grants access to the IP address `34.121.207.128`.

   :::info
   Alternatively, you can allow secure connections via SSH tunneling. To do so:
     * Follow the guide to [configure an SSH server for tunneling](../../../../guides/connect-network/)
     * When you configure your connector as described in the [configuration](#configuration) section above,
        including the additional `networkTunnel` configuration to enable the SSH tunnel.
        See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
        for additional details and a sample.
   :::

2. Set the `binlog_expire_logs_seconds` [server perameter](https://docs.microsoft.com/en-us/azure/mysql/single-server/concepts-server-parameters#configurable-server-parameters)
to `2592000`.

3. Using [MySQL workbench](https://docs.microsoft.com/en-us/azure/mysql/single-server/connect-workbench) or your preferred client, create the watermarks table.

:::tip
Your username must be specified in the format `username@servername`.
:::

```sql
CREATE DATABASE IF NOT EXISTS flow;
CREATE TABLE IF NOT EXISTS flow.watermarks (slot INTEGER PRIMARY KEY, watermark TEXT);
```

4. Create the `flow_capture` user with replication permission, the ability to read all tables, and the ability to read and write the watermarks table.

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

4. Note the instance's host under Server name, and the port under Connection Strings (usually `3306`).
Together, you'll use the host:port as the `address` property when you configure the connector.

## Troubleshooting Capture Errors

The `source-mysql` connector is designed to halt immediately if something wrong or unexpected happens, instead of continuing on and potentially outputting incorrect data. What follows is a non-exhaustive list of some potential failure modes, and what action should be taken to fix these situations:

### Unsupported Operations

If your capture is failing with an `"unsupported operation {ALTER,DROP,TRUNCATE,etc} TABLE"` error, this indicates that such an operation has taken place impacting a table which is currently being captured.

In the case of `DROP TABLE` and other destructive operations this is not supported, and can only be resolved by removing the offending table(s) from the capture bindings list, after which you may recreate the capture if desired (causing the latest state of the table to be recaptured in its entirety).

In the case of `ALTER TABLE` query we intend to support a limited subset of table alterations in the future, however this error indicates that whatever alteration took place is not currently supported. Practically speaking the immediate resolution is the same as for a `DROP` or `TRUNCATE TABLE`, but if you frequently perform schema migrations it may be worth reaching out to see if we can add support for whatever table alteration you just did.

### Data Manipulation Queries

If your capture is failing with an `"unsupported DML query"` error, this means that an `INSERT`, `UPDATE`, `DELETE` or other data manipulation query is present in the MySQL binlog. This should generally not happen if `binlog_format = 'ROW'` as described in the [Prerequisites](#prerequisites) section.

Resolving this error requires fixing the `binlog_format` system variable, and then either tearing down and recreating the entire capture so that it restarts at a later point in the binlog, or in the case of an `INSERT`/`DELETE` query it may suffice to remove the capture binding for the offending table and then re-add it.

### Unhandled Queries

If your capture is failing with an `"unhandled query"` error, some SQL query is present in the binlog which the connector does not (currently) understand.

In general, this error suggests that the connector should be modified to at least recognize this type of query, and most likely categorize it as either an unsupported [DML Query](#data-manipulation-queries), an unsupported [Table Operation](#unsupported-operations), or something that can safely be ignored. Until such a fix is made the capture cannot proceed, and you will need to tear down and recreate the entire capture so that it restarts from a later point in the binlog.

### Metadata Errors

If your capture is failing with a `"metadata error"` then something has gone badly wrong with the capture's tracking of table metadata, such as column names or datatypes.

This should never happen, and most likely means that the MySQL binlog itself is corrupt in some way. If this occurs, it can be resolved by removing the offending table(s) from the capture bindings list and then recreating the capture (generally into a new collection, as this process will cause the table to be re-captured in its entirety).

### Insufficient Binlog Retention

If your capture fails with a `"binlog retention period is too short"` error, it is informing you that the MySQL binlog retention period is set to a dangerously low value, and your capture would risk unrecoverable failure if it were paused or the server became unreachable for a nontrivial amount of time, such that the database expired a binlog segment that the capture was still reading from.

(If this were to happen, then change events would be permanently lost and that particular capture would never be able to make progress without potentially producing incorrect data. Thus the capture would need to be torn down and recreated so that each table could be re-captured in its entirety, starting with a complete backfill of current contents.)

The `"binlog retention period is too short"` error should normally be fixed by setting `binlog_expire_logs_seconds = 2592000` as described in the [Prerequisites](#prerequisites) section (and when running on a managed cloud platform additional steps may be required, refer to the managed cloud setup instructions above). However, advanced users who understand the risks can use the `skip_binlog_retention_check` configuration option to disable this safety.
