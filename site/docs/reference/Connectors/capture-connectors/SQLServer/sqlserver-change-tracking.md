---
sidebar_position: 3
---

# Microsoft SQL Server (Change Tracking)

This connector uses SQL Server Change Tracking to continuously capture updates
in a Microsoft SQL Server database into one or more Flow collections.

It's available for use in the Flow web application. For local development or
open-source workflows, [`ghcr.io/estuary/source-sqlserver-ct:dev`](https://ghcr.io/estuary/source-sqlserver-ct:dev)
provides the latest version of the connector as a Docker image. You can also
follow the link in your browser to see past image versions.

## When to use this connector

Estuary offers two SQL Server connectors: this one (Change Tracking) and the
[CDC connector](http://go.estuary.dev/source-sqlserver). Both provide real-time
change capture with similar performance characteristics, but have different
strengths.

**Choose Change Tracking when:**

- You need to capture computed columns or computed primary keys (CDC cannot capture these)
- You want lower storage overhead (CT stores only primary keys, not full row contents)

**Choose CDC when:**

- You need to capture from tables without a primary key
- You need complete audit logging with full row history (CT may combine intermediate
  changes when they occur in rapid succession)

## Supported versions and platforms

This connector will work on both hosted deployments and all major cloud providers. It is designed for databases using any version of SQL Server which has Change Tracking support, and is regularly tested against SQL Server 2017 and up.

Setup instructions are provided for the following platforms:

- [Self-hosted SQL Server](#self-hosted-sql-server)
- [Azure SQL Database](#azure-sql-database)
- [Amazon RDS for SQL Server](#amazon-rds-for-sql-server)
- [Google Cloud SQL for SQL Server](#google-cloud-sql-for-sql-server)

## Prerequisites

To capture change events from SQL Server tables using this connector, you need:

- Primary keys on all tables you intend to capture. Change Tracking does not support tables without primary keys.

- [Change Tracking enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server)
  on both the database and the individual tables to be captured.

- A user role with:
  - `SELECT` permissions on the schemas that contain tables to be captured.
  - `VIEW CHANGE TRACKING` permission on the schemas containing tables to capture.

## Setup

To meet these requirements, follow the steps for your hosting type.

### Self-hosted SQL Server

1. Connect to the server and issue the following commands:

```sql
USE <database>;

-- Enable Change Tracking for the database with a 3-day retention period.
ALTER DATABASE <database> SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 3 DAYS, AUTO_CLEANUP = ON);

-- Enable Change Tracking on tables. The below query enables CT on table 'dbo.foobar',
-- you should add similar query for all other tables you intend to capture.
ALTER TABLE dbo.foobar ENABLE CHANGE_TRACKING;

-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;

-- Grant the user permissions on schemas with data. This example assumes all tables
-- to be captured are in the default schema, `dbo`. Add similar queries for any other
-- schemas which contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT VIEW CHANGE TRACKING ON SCHEMA :: dbo TO flow_capture;
```

2. Allow secure connection to Estuary from your hosting environment. Either:

   - Set up an [SSH server for tunneling](/guides/connect-network/).

     When you fill out the [endpoint configuration](#endpoint), include the
     additional `networkTunnel` configuration to enable the SSH tunnel. See
     [Connecting to Endpoints on Secure Networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
     for additional details and a sample.

   - [Allowlist the Estuary IP addresses](/reference/allow-ip-addresses) in your firewall rules.

### Azure SQL Database

Follow the [Self-hosted SQL Server](#self-hosted-sql-server) setup instructions above, with the following Azure-specific notes:

- **Firewall rules**: Configure [server-level IP firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql#use-the-azure-portal-to-manage-server-level-ip-firewall-rules) to allow the [Estuary Flow IP addresses](/reference/allow-ip-addresses), or set up an SSH tunnel as described in the self-hosted instructions.
- **Connection address**: Find the server hostname under **Server Name** in the Azure portal. The port is always `1433`.

### Amazon RDS for SQL Server

Follow the [Self-hosted SQL Server](#self-hosted-sql-server) setup instructions above, with the following RDS-specific notes:

- **Firewall rules**: Modify the [security group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html) associated with your RDS instance to allow inbound traffic from the [Estuary Flow IP addresses](/reference/allow-ip-addresses) on port 1433, or set up an SSH tunnel as described in the self-hosted instructions.
- **Connection address**: Find the endpoint hostname under **Connectivity & security** in the RDS console. The port is typically `1433` unless you configured a custom port.

### Google Cloud SQL for SQL Server

Follow the [Self-hosted SQL Server](#self-hosted-sql-server) setup instructions above, with the following Cloud SQL-specific notes:

- **Firewall rules**: Add the [Estuary Flow IP addresses](/reference/allow-ip-addresses) as [authorized networks](https://cloud.google.com/sql/docs/sqlserver/authorize-networks) for your instance, or set up an SSH tunnel as described in the self-hosted instructions.
- **Connection address**: Find the **Public IP address** on the instance's **Overview** page in the Cloud Console. The port is `1433`.

## Change Tracking Retention

Change Tracking data is automatically cleaned up based on the retention period
configured when enabling Change Tracking. If the connector is offline for
longer than this retention period, or if it falls too far behind, it will
automatically perform a full backfill of impacted tables to re-establish
consistency.

To adjust the retention period:

```sql
ALTER DATABASE <database> SET CHANGE_TRACKING (CHANGE_RETENTION = 5 DAYS);
```

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SQL Server Change Tracking source connector.

### Properties

#### Endpoint

| Property                        | Title               | Description                                                                                                                                 | Type    | Required/Default           |
| ------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ------- | -------------------------- |
| **`/address`**                  | Server Address      | The host or host:port at which the database can be reached.                                                                                 | string  | Required                   |
| **`/database`**                 | Database            | Logical database name to capture from.                                                                                                      | string  | Required                   |
| **`/user`**                     | User                | The database user to authenticate as.                                                                                                       | string  | Required, `"flow_capture"` |
| **`/password`**                 | Password            | Password for the specified database user.                                                                                                   | string  | Required                   |
| `/historyMode` | History Mode | Capture each change event, without merging. | boolean | `false` |
| `/advanced`                     | Advanced Options    | Options for advanced users. You should not typically need to modify these.                                                                  | object  |                            |
| `/advanced/discover_tables_without_ct` | Discover Tables Without Change Tracking | When set, the connector will discover all tables even if they do not have Change Tracking enabled. By default, only CT-enabled tables are discovered. | boolean | `false` |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query.                                                    | integer | `50000`                     |
| `/advanced/skip_backfills`      | Skip Backfills      | A comma-separated list of fully-qualified table names which should not be backfilled.                                                       | string  |                            |
| `/advanced/source_tag` | Source Tag | This value is added as the property 'tag' in the source metadata of each document, when set. | string |  |

#### Bindings

| Property         | Title             | Description                                                                                                                                                                                                               | Type    | Required/Default |
| ---------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/namespace`** | Table Schema      | The schema in which the table resides.                                                                                                                                                                                    | string  | Required         |
| **`/stream`**    | Table Name        | The name of the table to be captured.                                                                                                                                                                                     | string  | Required         |
| `/priority`      | Backfill Priority | An optional integer priority for this binding. The highest priority binding(s) will be backfilled completely before any others. The default priority is zero. Negative priorities will cause a binding to be backfilled after others. | integer |                  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-sqlserver-ct:dev"
        config:
          address: "<host>:1433"
          database: "my_db"
          user: "flow_capture"
          password: "secret"
    bindings:
      - resource:
          namespace: dbo
          stream: ${TABLE_NAME}
        target: ${PREFIX}/${COLLECTION_NAME}
```

Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](/concepts/captures.md)
