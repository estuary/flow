---
sidebar_position: 3
---

# MariaDB

This is a change data capture (CDC) connector that captures change events from a MariaDB database via the [Binary Log](https://mariadb.com/kb/en/overview-of-the-binary-log/).
It's derived from the [MySQL capture connector](../MySQL/MySQL.md),
so the same configuration applies, but the setup steps look somewhat different.

This connector is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-mariadb:dev`](https://github.com/estuary/connectors/pkgs/container/source-mariadb) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need a MariaDB database setup with the following.

- The [`binlog_format`](https://mariadb.com/kb/en/binary-log-formats/)
  system variable must be set to `ROW`.
- The [binary log expiration period](https://mariadb.com/kb/en/using-and-maintaining-the-binary-log/#purging-log-files) should be at least 7 days.
  - This value may be set lower if necessary, but we [discourage](#insufficient-binlog-retention) doing so as this may increase the likelihood of unrecoverable failures.
- A database user with appropriate permissions:
  - `REPLICATION CLIENT` and `REPLICATION SLAVE` [privileges](https://mariadb.com/docs/skysql/ref/es10.6/privileges/).
  - Permission to read the tables being captured.
  - Permission to read from `information_schema` tables, if automatic discovery is used.
- If the table(s) to be captured include columns of type `DATETIME`, the `time_zone` system variable
  must be set to an IANA zone name or numerical offset or the capture configured with a `timezone` to use by default.

:::tip Configuration Tip
To configure this connector to capture data from databases hosted on your internal network, you must set up SSH tunneling. For more specific instructions on setup, see [configure connections with SSH tunneling](/guides/connect-network/).
:::

## Setup

### Self Hosted MariaDB

To meet these requirements, do the following:

1. Create the `flow_capture` user with replication permission, and the ability to read all tables.

The `SELECT` permission can be restricted to just the tables that need to be
captured, but automatic discovery requires `information_schema` access as well.

```sql
CREATE USER IF NOT EXISTS flow_capture IDENTIFIED BY 'secret'
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
GRANT SELECT ON *.* TO 'flow_capture';
```

2. Configure the binary log to retain data for at least 7 days. We recommend 30 days where possible.

```sql
SET PERSIST binlog_expire_logs_seconds = 2592000;
```

3. Configure the database's time zone. See [below](#setting-the-mariadb-time-zone) for more information.

```sql
SET PERSIST time_zone = '-05:00'
```

### Azure Database for MariaDB

You can use this connector for MariaDB instances on Azure Database for MariaDB using the following setup instructions.

1. Allow connections to the database from the Estuary Flow IP address.

   1. Create a new [firewall rule](https://learn.microsoft.com/en-us/azure/mariadb/howto-manage-firewall-portal)
      that grants access to the [Estuary Flow IP addresses](/reference/allow-ip-addresses).

   :::info
   Alternatively, you can allow secure connections via SSH tunneling. To do so:

   - Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
   - When you configure your connector as described in the [configuration](#configuration) section above,
     including the additional `networkTunnel` configuration to enable the SSH tunnel.
     See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
     for additional details and a sample.
     :::

2. Set the `binlog_expire_logs_seconds` [server perameter](https://learn.microsoft.com/en-us/azure/mariadb/howto-server-parameters#configure-server-parameters)
   to `2592000`.

3. Using your preferred MariaDB client, create the `flow_capture` user with replication permission, and the ability to read all tables.

   The `SELECT` permission can be restricted to just the tables that need to be captured, but automatic discovery requires `information_schema` access as well.

:::tip
Your username must be specified in the format `username@servername`.
:::

```sql
CREATE USER IF NOT EXISTS flow_capture IDENTIFIED BY 'secret'
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
GRANT SELECT ON *.* TO 'flow_capture';
```

4. Note the instance's host under Server name, and the port under Connection Strings (usually `3306`).
   Together, you'll use the host:port as the `address` property when you configure the connector.

## Capturing from Read Replicas

This connector supports capturing from a read replica of your database, provided that
binary logging is enabled on the replica and all other requirements are met.

## Setting the MariaDB time zone

MariaDB's [`time_zone` server system variable](https://mariadb.com/kb/en/server-system-variables/#system_time_zone) is set to `SYSTEM` by default.
Flow is not able to detect your time zone when it's set this way, so you must explicitly set the variable for your database.

If you intend to capture tables including columns of the type `DATETIME`,
and `time_zone` is set to `SYSTEM`,
Flow won't be able to detect the time zone and convert the column to [RFC3339 format](https://www.rfc-editor.org/rfc/rfc3339).
To avoid this, you must explicitly set the time zone for your database.

You can:

- Specify a numerical offset from UTC.

- Specify a named timezone in [IANA timezone format](https://www.iana.org/time-zones).

For example, if you're located in New Jersey, USA, you could set `time_zone` to `-05:00` or `-04:00`, depending on the time of year.
Because this region observes daylight savings time, you'd be responsible for changing the offset.
Alternatively, you could set `time_zone` to `America/New_York`, and time changes would occur automatically.

If using IANA time zones, your database must include time zone tables. [Learn more in the MariaDB docs](https://mariadb.com/kb/en/time-zones/).

:::tip Capture Timezone Configuration
If you are unable to set the `time_zone` in the database and need to capture tables with `DATETIME` columns, the capture can be configured to assume a time zone using the `timezone` configuration property (see below). The `timezone` configuration property can be set as a numerical offset or IANA timezone format.
:::

## Backfills and performance considerations

When the MariaDB capture is initiated, by default, the connector first _backfills_, or captures the targeted tables in their current state. It then transitions to capturing change events on an ongoing basis.

This is desirable in most cases, as it ensures that a complete view of your tables is captured into Flow.
However, you may find it appropriate to skip the backfill, especially for extremely large tables.

In this case, you may turn off backfilling on a per-table basis. See [properties](#properties) for details.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the MariaDB source connector.

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

| Property         | Title     | Description                                                                                                         | Type   | Required/Default |
| ---------------- | --------- | ------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/namespace`** | Namespace | The [database](https://mariadb.com/kb/en/understanding-mariadb-architecture/#databases) in which the table resides. | string | Required         |
| **`/stream`**    | Stream    | Name of the table to be captured from the database.                                                                 | string | Required         |

:::info
When you configure this connector in the web application, the automatic **discovery** process sets up a binding for _most_ tables it finds in your database, but there are exceptions.

Tables in the MariaDB system databases `information_schema`, `mysql`, and `performance_schema` will not be discovered.
You can add bindings for such tables manually.
:::

### Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-mariadb:dev
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

The `source-mariadb` connector is designed to halt immediately if something wrong or unexpected happens, instead of continuing on and potentially outputting incorrect data. What follows is a non-exhaustive list of some potential failure modes, and what action should be taken to fix these situations:

### Unsupported Operations

If your capture is failing with an `"unsupported operation {ALTER,DROP,TRUNCATE,etc} TABLE"` error, this indicates that such an operation has taken place impacting a table which is currently being captured.

In the case of `DROP TABLE` and other destructive operations this is not supported, and can only be resolved by removing the offending table(s) from the capture bindings list, after which you may recreate the capture if desired (causing the latest state of the table to be recaptured in its entirety).

In the case of `ALTER TABLE` we currently support table alterations to add or drop columns from a table. This error indicates that whatever alteration took place is not currently supported. Practically speaking the immediate resolution is the same as for a `DROP` or `TRUNCATE TABLE`, but if you frequently perform schema migrations it may be worth reaching out to see if we can add support for whatever table alteration you just did.

### Data Manipulation Queries

If your capture is failing with an `"unsupported DML query"` error, this means that an `INSERT`, `UPDATE`, `DELETE` or other data manipulation query is present in the binlog. This should generally not happen if `binlog_format = 'ROW'` as described in the [Prerequisites](#prerequisites) section.

Resolving this error requires fixing the `binlog_format` system variable, and then either tearing down and recreating the entire capture so that it restarts at a later point in the binlog, or in the case of an `INSERT`/`DELETE` query it may suffice to remove the capture binding for the offending table and then re-add it.

### Unhandled Queries

If your capture is failing with an `"unhandled query"` error, some SQL query is present in the binlog which the connector does not (currently) understand.

In general, this error suggests that the connector should be modified to at least recognize this type of query, and most likely categorize it as either an unsupported [DML Query](#data-manipulation-queries), an unsupported [Table Operation](#unsupported-operations), or something that can safely be ignored. Until such a fix is made the capture cannot proceed, and you will need to backfill all collections to allow the capture to jump ahead to a later point in the binlog.

### Metadata Errors

If your capture is failing with a `"metadata error"` then something has gone badly wrong with the capture's tracking of table metadata, such as column names or datatypes.

This should never happen, and most likely means that the binlog itself is corrupt in some way. If this occurs, it can be resolved by backfilling all collections from the source.

### Insufficient Binlog Retention

If your capture fails with a `"binlog retention period is too short"` error, it is informing you that the MariaDB binlog retention period is set to a dangerously low value.

The concern is that if a capture is disabled or the server becomes unreachable for longer than the binlog retention period, the database might delete a binlog segment which the capture isn't yet done with. If this happens then change events have been permanently lost, and the only way to get the capture running again is to skip ahead to a portion of the binlog which still exists. For correctness this requires backfilling the current contents of all tables from the source, and so we prefer to avoid it as much as possible. It's much easier to just set up your binlog retention with enough wiggle room to recover from temporary failures.

The `"binlog retention period is too short"` error should normally be fixed by setting a longer retention period as described in these setup instructions. However, advanced users who understand the risks can use the `skip_binlog_retention_check` configuration option to disable this safety.
