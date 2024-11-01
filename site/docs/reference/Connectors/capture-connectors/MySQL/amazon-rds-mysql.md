---
sidebar_position: 5
---

# Amazon RDS for MySQL

This is a change data capture (CDC) connector that captures change events from a MySQL database via the [Binary Log](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-mysql:dev`](https://github.com/estuary/connectors/pkgs/container/source-mysql) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need a MySQL database setup with the following.

- The [`binlog_format`](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html#sysvar_binlog_format)
  system variable must be set to `ROW` (the default value).
- The [binary log retention](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-configuring.html#mysql_rds_set_configuration-usage-notes.binlog-retention-hours)
  period should be set to 168 hours (the maximum allowed by RDS).
  - This value may be set lower if necessary, but we [discourage](#insufficient-binlog-retention) doing so as this may increase the likelihood of unrecoverable failures.
- A database user with appropriate permissions:
  - `REPLICATION CLIENT` and `REPLICATION SLAVE` privileges.
  - Permission to read the tables being captured.
  - Permission to read from `information_schema` tables, if automatic discovery is used.
- If the table(s) to be captured include columns of type `DATETIME`, the `time_zone` system variable
  must be set to an IANA zone name or numerical offset or the capture configured with a `timezone` to use by default.

## Setup

1. Allow connections between the database and Estuary Flow. There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - [Modify the database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html), setting **Public accessibility** to **Yes**.
      - Edit the VPC security group associated with your database, or create a new VPC security group and associate it with the database as described in [the Amazon documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create). Create a new inbound rule and a new outbound rule that allow all traffic from the [Estuary Flow IP addresses](/reference/allow-ip-addresses).

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. Create a RDS parameter group to enable replication in MySQL.

   1. [Create a parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Creating).
      Create a unique name and description and set the following properties:

      - **Family**: mysql8.0
      - **Type**: DB Parameter group

   2. [Modify the new parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Modifying) and update the following parameters:

      - binlog_format: ROW

   3. [Associate the parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Associating)
      with the database and set [Backup Retention Period](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.html#USER_WorkingWithAutomatedBackups.Enabling) to 7 days.
      Reboot the database to allow the changes to take effect.

3. Switch to your MySQL client. Run the following commands to create a new user for the capture with appropriate permissions:

```sql
CREATE USER IF NOT EXISTS flow_capture
  IDENTIFIED BY 'secret'
  COMMENT 'User account for Flow MySQL data capture';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
GRANT SELECT ON *.* TO 'flow_capture';
```

4. Run the following command to set the binary log retention to 7 days, the maximum value which RDS MySQL permits:

```sql
CALL mysql.rds_set_configuration('binlog retention hours', 168);
```

5. In the [RDS console](https://console.aws.amazon.com/rds/), note the instance's Endpoint and Port. You'll need these for the `address` property when you configure the connector.

## Capturing from Read Replicas

This connector supports capturing from a read replica of your database, provided that
binary logging is enabled on the replica and all other requirements are met. To create
a read replica:

1. Follow RDS instructions to [create a read replica](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ReadRepl.html#USER_ReadRepl.Create)
   of your MySQL database.

2. [Modify the replica](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html)
   and set the following:

   - **DB parameter group**: the parameter group you created previously
   - **Backup retention period**: 7 days
   - **Public access**: Publicly accessible

3. Reboot the replica to allow the changes to take effect.

## Setting the MySQL time zone

MySQL's [`time_zone` server system variable](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_time_zone) is set to `SYSTEM` by default.

If you intend to capture tables including columns of the type `DATETIME`,
and `time_zone` is set to `SYSTEM`,
Flow won't be able to detect the time zone and convert the column to [RFC3339 format](https://www.rfc-editor.org/rfc/rfc3339).
To avoid this, you must explicitly set the time zone for your database.

You can:

- Specify a numerical offset from UTC.

  - For MySQL version 8.0.19 or higher, values from `-13:59` to `+14:00`, inclusive, are permitted.
  - Prior to MySQL 8.0.19, values from `-12:59` to `+13:00`, inclusive, are permitted

- Specify a named timezone in [IANA timezone format](https://www.iana.org/time-zones).

For example, if you're located in New Jersey, USA, you could set `time_zone` to `-05:00` or `-04:00`, depending on the time of year.
Because this region observes daylight savings time, you'd be responsible for changing the offset.
Alternatively, you could set `time_zone` to `America/New_York`, and time changes would occur automatically.

If using IANA time zones, your database must include time zone tables. [Learn more in the MySQL docs](https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html).

:::tip Capture Timezone Configuration
If you are unable to set the `time_zone` in the database and need to capture tables with `DATETIME` columns, the capture can be configured to assume a time zone using the `timezone` configuration property (see below). The `timezone` configuration property can be set as a numerical offset or IANA timezone format.
:::

## Backfills and performance considerations

When the a MySQL capture is initiated, by default, the connector first _backfills_, or captures the targeted tables in their current state. It then transitions to capturing change events on an ongoing basis.

This is desirable in most cases, as in ensures that a complete view of your tables is captured into Flow.
However, you may find it appropriate to skip the backfill, especially for extremely large tables.

In this case, you may turn of backfilling on a per-table basis. See [properties](#properties) for details.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the MySQL source connector.

### Properties

#### Endpoint

| Property                                | Title                              | Description                                                                                                                                                                                                                                                                                                                                                                             | Type    | Required/Default           |
| --------------------------------------- | ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | -------------------------- |
| **`/address`**                          | Server Address                     | The host or host:port at which the database can be reached.                                                                                                                                                                                                                                                                                                                             | string  | Required                   |
| **`/user`**                             | Login User                         | The database user to authenticate as.                                                                                                                                                                                                                                                                                                                                                   | string  | Required, `"flow_capture"` |
| **`/password`**                         | Login Password                     | Password for the specified database user.                                                                                                                                                                                                                                                                                                                                               | string  | Required                   |
| `/timezone`                             | Timezone                           | Timezone to use when capturing datetime columns. Should normally be left blank to use the database's `'time_zone'` system variable. Only required if the `'time_zone'` system variable cannot be read and columns with type datetime are being captured. Must be a valid IANA time zone name or +HH:MM offset. Takes precedence over the `'time_zone'` system variable if both are set. | string  |                            |
| `/advanced/dbname`                      | Database Name                      | The name of database to connect to. In general this shouldn&#x27;t matter. The connector can discover and capture from all databases it&#x27;s authorized to access.                                                                                                                                                                                                                    | string  | `"mysql"`                  |
| `/advanced/node_id`                     | Node ID                            | Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn&#x27;t matter so long as it is unique. If unset or zero the connector will pick a value.                                                                                                                                                                             | integer |                            |
| `/advanced/skip_backfills`              | Skip Backfills                     | A comma-separated list of fully-qualified table names which should not be backfilled.                                                                                                                                                                                                                                                                                                   | string  |                            |
| `/advanced/backfill_chunk_size`         | Backfill Chunk Size                | The number of rows which should be fetched from the database in a single backfill query.                                                                                                                                                                                                                                                                                                | integer | `131072`                   |
| `/advanced/skip_binlog_retention_check` | Skip Binlog Retention Sanity Check | Bypasses the &#x27;dangerously short binlog retention&#x27; sanity check at startup. Only do this if you understand the danger and have a specific need.                                                                                                                                                                                                                                | boolean |                            |

#### Bindings

| Property         | Title     | Description                                                                                                    | Type   | Required/Default |
| ---------------- | --------- | -------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/namespace`** | Namespace | The [database/schema](https://dev.mysql.com/doc/refman/8.0/en/show-databases.html) in which the table resides. | string | Required         |
| **`/stream`**    | Stream    | Name of the table to be captured from the database.                                                            | string | Required         |

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
        target: ${PREFIX}/${COLLECTION_NAME}
```

Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](/concepts/captures.md)

## Troubleshooting Capture Errors

The `source-mysql` connector is designed to halt immediately if something wrong or unexpected happens, instead of continuing on and potentially outputting incorrect data. What follows is a non-exhaustive list of some potential failure modes, and what action should be taken to fix these situations:

### Unsupported Operations

If your capture is failing with an `"unsupported operation {ALTER,DROP,TRUNCATE,etc} TABLE"` error, this indicates that such an operation has taken place impacting a table which is currently being captured.

In the case of `DROP TABLE` and other destructive operations this is not supported, and can only be resolved by removing the offending table(s) from the capture bindings list, after which you may recreate the capture if desired (causing the latest state of the table to be recaptured in its entirety).

In the case of `ALTER TABLE` we currently support table alterations to add or drop columns from a table. This error indicates that whatever alteration took place is not currently supported. Practically speaking the immediate resolution is the same as for a `DROP` or `TRUNCATE TABLE`, but if you frequently perform schema migrations it may be worth reaching out to see if we can add support for whatever table alteration you just did.

### Data Manipulation Queries

If your capture is failing with an `"unsupported DML query"` error, this means that an `INSERT`, `UPDATE`, `DELETE` or other data manipulation query is present in the MySQL binlog. This should generally not happen if `binlog_format = 'ROW'` as described in the [Prerequisites](#prerequisites) section.

Resolving this error requires fixing the `binlog_format` system variable, and then either tearing down and recreating the entire capture so that it restarts at a later point in the binlog, or in the case of an `INSERT`/`DELETE` query it may suffice to remove the capture binding for the offending table and then re-add it.

### Unhandled Queries

If your capture is failing with an `"unhandled query"` error, some SQL query is present in the binlog which the connector does not (currently) understand.

In general, this error suggests that the connector should be modified to at least recognize this type of query, and most likely categorize it as either an unsupported [DML Query](#data-manipulation-queries), an unsupported [Table Operation](#unsupported-operations), or something that can safely be ignored. Until such a fix is made the capture cannot proceed, and you will need to backfill all collections to allow the capture to jump ahead to a later point in the binlog.

### Metadata Errors

If your capture is failing with a `"metadata error"` then something has gone badly wrong with the capture's tracking of table metadata, such as column names or datatypes.

This should never happen, and most likely means that the MySQL binlog itself is corrupt in some way. If this occurs, it can be resolved by backfilling all collections from the source.

### Insufficient Binlog Retention

If your capture fails with a `"binlog retention period is too short"` error, it is informing you that the MySQL binlog retention period is set to a dangerously low value.

The concern is that if a capture is disabled or the server becomes unreachable for longer than the binlog retention period, the database might delete a binlog segment which the capture isn't yet done with. If this happens then change events have been permanently lost, and the only way to get the capture running again is to skip ahead to a portion of the binlog which still exists. For correctness this requires backfilling the current contents of all tables from the source, and so we prefer to avoid it as much as possible. It's much easier to just set up your binlog retention with enough wiggle room to recover from temporary failures.

The `"binlog retention period is too short"` error should normally be fixed by setting a longer retention period as described in these setup instructions. However, advanced users who understand the risks can use the `skip_binlog_retention_check` configuration option to disable this safety.

### Empty Collection Key

Every Flow collection must declare a [key](/concepts/collections.md#keys) which is used to group its documents. When testing your capture, if you encounter an error indicating collection key cannot be empty, you will need to either add a key to the table in your source, or manually edit the generated specification and specify keys for the collection before publishing to the catalog as documented [here](/concepts/collections.md#empty-keys).
