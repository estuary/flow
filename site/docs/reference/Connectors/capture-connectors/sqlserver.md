---
sidebar_position: 3
---
# Microsoft SQL Server

This connector uses change data capture (CDC) to continuously capture updates in a Microsoft SQL Server database into one or more Flow collections.

It’s available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-sqlserver:dev`](https://ghcr.io/estuary/source-sqlserver:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported versions and platforms

This connector supports SQL Server 2017 and later on major cloud providers,
as well as self-hosted instances.
Setup instructions are provided for the following platforms:

* [Self-hosted SQL Server](#setup-self-hosted-sql-server)
* [Azure SQL Database](#setup-azure-sql-database)
* [Amazon RDS for SQL Server](#setup-amazon-rds-for-sql-server)
* [Google Cloud SQL for SQL Server](#setup-google-cloud-sql-for-sql-server)

## Prerequisites

To capture change events from SQL Server tables using this connector, you need:

* For each table to be captured, a primary key should be specified in the database.
If a table doesn't have a primary key, you must manually specify a key in the associated Flow collection definition while creating the capture.
[See detailed steps](#specifying-flow-collection-keys).

* [CDC enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver16)
on the database and the individual tables to be captured.
(This creates *change tables* in the database, from which the connector reads.)

* A **watermarks table**. The watermarks table is a small “scratch space” to which the connector occasionally writes a small amount of data to ensure accuracy when backfilling preexisting table contents.

* A user role with:
  * `SELECT` permissions on the CDC schema and the schemas that contain tables to be captured.
  * Access to the change tables created as part of the SQL Server CDC process.
  * `SELECT`, `INSERT`, and `UPDATE` permissions on the watermarks table

To meet these requirements, follow the steps for your hosting type.

* [Self-hosted SQL Server](#setup-self-hosted-sql-server)
* [Azure SQL Database](#setup-azure-sql-database)
* [Amazon RDS for SQL Server](#setup-amazon-rds-for-sql-server)
* [Google Cloud SQL for SQL Server](#setup-google-cloud-sql-for-sql-server)

### Setup: Self-hosted SQL Server

1. Connect to the server and issue the following commands:

```sql
USE <database>;
-- Enable CDC for the database.
EXEC sys.sp_cdc_enable_db;
-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
-- Grant the user permissions on the CDC schema and schemas with data.
-- This assumes all tables to be captured are in the default schema, `dbo`.
-- Add similar queries for any other schemas that contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT SELECT ON SCHEMA :: cdc TO flow_capture;
-- Create the watermarks table and grant permissions.
CREATE TABLE dbo.flow_watermarks(slot INTEGER PRIMARY KEY, watermark TEXT);
GRANT SELECT, INSERT, UPDATE ON dbo.flow_watermarks TO flow_capture;
-- Enable CDC on tables. The below query enables CDC the watermarks table ONLY.
-- You should add similar query for all other tables you intend to capture.
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'flow_watermarks', @role_name = 'flow_capture';
```

2. Allow secure connection to Estuary Flow from your hosting environment. Either:
   * Set up an [SSH server for tunneling](../../../../guides/connect-network/).

     When you fill out the [endpoint configuration](#endpoint),
     include the additional `networkTunnel` configuration to enable the SSH tunnel.
     See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
     for additional details and a sample.

   * Whitelist the Estuary IP address, `34.121.207.128` in your firewall rules.

### Setup: Azure SQL Database

1. Allow connections to the server from the Estuary Flow IP address.

   1. Create a new [firewall rule](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql#use-the-azure-portal-to-manage-server-level-ip-firewall-rules)
   that grants access to the IP address `34.121.207.128`.

   :::info
   Alternatively, you can allow secure connections via SSH tunneling as described in the setup steps for
   [self-hosted databases](#setup-self-hosted-sql-server).
   :::

2. In your SQL client, connect to your instance as the default `sqlserver` user and issue the following commands.

```sql
USE <database>;
-- Enable CDC for the database.
EXEC sys.sp_cdc_enable_db;
-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
-- Grant the user permissions on the CDC schema and schemas with data.
-- This assumes all tables to be captured are in the default schema, `dbo`.
-- Add similar queries for any other schemas that contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT SELECT ON SCHEMA :: cdc TO flow_capture;
-- Create the watermarks table and grant permissions.
CREATE TABLE dbo.flow_watermarks(slot INTEGER PRIMARY KEY, watermark TEXT);
GRANT SELECT, INSERT, UPDATE ON dbo.flow_watermarks TO flow_capture;
-- Enable CDC on tables. The below query enables CDC the watermarks table ONLY.
-- You should add similar query for all other tables you intend to capture.
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'flow_watermarks', @role_name = 'flow_capture';
```

3. Note the following important items for configuration:

   * Find the instance's host under Server Name. The port is always `1433`. Together, you'll use the host:port as the `address` property when you configure the connector.
   * Format `user` as `username@databasename`; for example, `flow_capture@myazuredb`.

### Setup: Amazon RDS for SQL Server

1. Allow connections to the database from the Estuary Flow IP address.

   1. [Modify the database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html), setting **Public accessibility** to **Yes**.  See the instructions below to use SSH tunneling instead of enabling public access.

   2. Edit the VPC security group associated with your database, or create a new VPC security group and associate it with the database.
      Refer to the [steps in the Amazon documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create).
      Create a new inbound rule and a new outbound rule that allow all traffic from the IP address `34.121.207.128`.

   :::info
   Alternatively, you can allow secure connections via SSH tunneling as described in the setup steps for
   [self-hosted databases](#setup-self-hosted-sql-server).
   :::

2.  In your SQL client, connect to your instance as the default `sqlserver` user and issue the following commands.

```sql
USE <database>;
-- Enable CDC for the database.
EXEC msdb.dbo.rds_cdc_enable_db;
-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
-- Grant the user permissions on the CDC schema and schemas with data.
-- This assumes all tables to be captured are in the default schema, `dbo`.
-- Add similar queries for any other schemas that contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT SELECT ON SCHEMA :: cdc TO flow_capture;
-- Create the watermarks table and grant permissions.
CREATE TABLE dbo.flow_watermarks(slot INTEGER PRIMARY KEY, watermark TEXT);
GRANT SELECT, INSERT, UPDATE ON dbo.flow_watermarks TO flow_capture;
-- Enable CDC on tables. The below query enables CDC the watermarks table ONLY.
-- You should add similar query for all other tables you intend to capture.
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'flow_watermarks', @role_name = 'flow_capture';
```
6. In the [RDS console](https://console.aws.amazon.com/rds/), note the instance's Endpoint and Port. You'll need these for the `address` property when you configure the connector.

### Setup: Google Cloud SQL for SQL Server

1. Allow connections to the database from the Estuary Flow IP address.

   1. [Enable public IP on your database](https://cloud.google.com/sql/docs/sqlserver/configure-ip#add) and add
      `34.121.207.128` as an authorized IP address.  See the instructions below to use SSH tunneling instead of enabling public access.

   :::info
   Alternatively, you can allow secure connections via SSH tunneling as described in the setup steps for
   [self-hosted databases](#setup-self-hosted-sql-server).
   :::

2. In your SQL client, connect to your instance as the default `sqlserver` user and issue the following commands.

```sql
USE <database>;
-- Enable CDC for the database.
EXEC msdb.dbo.gcloudsql_cdc_enable_db '<database>';
-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
-- Grant the user permissions on the CDC schema and schemas with data.
-- This assumes all tables to be captured are in the default schema, `dbo`.
-- Add similar queries for any other schemas that contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT SELECT ON SCHEMA :: cdc TO flow_capture;
-- Create the watermarks table and grant permissions.
CREATE TABLE dbo.flow_watermarks(slot INTEGER PRIMARY KEY, watermark TEXT);
GRANT SELECT, INSERT, UPDATE ON dbo.flow_watermarks TO flow_capture;
-- Enable CDC on tables. The below query enables CDC the watermarks table ONLY.
-- You should add similar query for all other tables you intend to capture.
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'flow_watermarks', @role_name = 'flow_capture';
```

3. In the Cloud Console, note the instance's host under Public IP Address. Its port will always be `1433`.
Together, you'll use the host:port as the `address` property when you configure the connector.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SQL Server source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/address`** | Server Address | The host or host:port at which the database can be reached. | string | Required |
| **`/database`** | Database | Logical database name to capture from. | string | Required |
| **`/user`** | User | The database user to authenticate as. | string | Required, `"flow_capture"` |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object |  |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query. | integer | `4096` |
| `/advanced/skip_backfills` | Skip Backfills | A comma-separated list of fully-qualified table names which should not be backfilled. | string |  |
| `/advanced/watermarksTable` | Watermarks Table | The name of the table used for watermark writes during backfills. Must be fully-qualified in &#x27;&lt;schema&gt;.&lt;table&gt;&#x27; form. | string | `"dbo.flow_watermarks"` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| **`/namespace`** | Namespace | The [namespace/schema](https://learn.microsoft.com/en-us/sql/relational-databases/databases/databases?view=sql-server-ver16#basic-information-about-databases) of the table. | string | Required |
| **`/stream`** | Stream | Table name. | string | Required |
| `/primary_key` | Primary Key Columns | array | The columns which together form the primary key of the table. | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-sqlserver:dev"
        config:
          address: "<host>:1433"
          database: "my_db"
          user: "flow_capture"
          password: "secret"
    bindings:
      - resource:
          stream: ${TABLE_NAME}
          namespace: dbo
          primary_key: ["id"]
        target: ${PREFIX}/${COLLECTION_NAME}
```
Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)

## Specifying Flow collection keys

Every Flow collection must have a [key](../../../concepts/collections.md#keys).
As long as your SQL Server tables have a primary key specified, the connector will set the
corresponding collection's key accordingly.

In cases where a SQL Server table you want to capture doesn't have a primary key,
you can manually add it to the collection definition during the [capture creation workflow](../../../guides/create-dataflow.md#create-a-capture).

1. After you input the endpoint configuration and click **Next**,
the tables in your database have been mapped to Flow collections.
Click each collection's **Specification** tab and identify a collection where `"key": [ ],` is empty.

2. Click inside the empty key value in the editor and input the name of column in the table to use as the key, formatted as a JSON pointer. For example `"key": ["/foo"],`

   Make sure the key field is required, not nullable, and of an [allowed type](../../../concepts/collections.md#schema-restrictions).
   Make any other necessary changes to the [collection specification](../../../concepts/collections.md#specification) to accommodate this.

3. Repeat with other missing collection keys, if necessary.

4. Save and publish the capture as usual.
