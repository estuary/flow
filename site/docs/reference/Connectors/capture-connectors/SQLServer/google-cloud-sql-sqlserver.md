---
sidebar_position: 6
---

# Google Cloud SQL for SQL Server

This connector uses change data capture (CDC) to continuously capture updates in a Microsoft SQL Server database into one or more Flow collections.

It’s available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-sqlserver:dev`](https://ghcr.io/estuary/source-sqlserver:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported versions and platforms

This connector is designed for databases using any version of SQL Server which has CDC support, and is regularly tested against SQL Server 2017 and up.

## Prerequisites

To capture change events from SQL Server tables using this connector, you need:

- For each table to be captured, a primary key should be specified in the database.
  If a table doesn't have a primary key, you must manually specify a key in the associated Flow collection definition while creating the capture.
  [See detailed steps](#specifying-flow-collection-keys).

- [CDC enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver16)
  on the database and the individual tables to be captured.
  (This creates _change tables_ in the database, from which the connector reads.)

- A user role with:
  - `SELECT` permissions on the CDC schema and the schemas that contain tables to be captured.
  - Access to the change tables created as part of the SQL Server CDC process.
  - The `VIEW DATABASE STATE` or (in newer versions of SQL Server) `VIEW DATABASE PERFORMANCE STATE` permission.

### Setup

1. Allow connections between the database and Estuary Flow. There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - [Enable public IP on your database](https://cloud.google.com/sql/docs/sqlserver/configure-ip#add) and add the [Estuary Flow IP addresses](/reference/allow-ip-addresses) as authorized IP addresses.

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. In your SQL client, connect to your instance as the default `sqlserver` user and issue the following commands.

```sql
USE <database>;
-- Enable CDC for the database.
EXEC msdb.dbo.gcloudsql_cdc_enable_db '<database>';
-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'Secret123!';
CREATE USER flow_capture FOR LOGIN flow_capture;
-- Grant the user permissions on the CDC schema and schemas with data.
-- This assumes all tables to be captured are in the default schema, `dbo`.
-- Add similar queries for any other schemas that contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT SELECT ON SCHEMA :: cdc TO flow_capture;
-- Grant the 'VIEW DATABASE STATE' permission.
GRANT VIEW DATABASE STATE TO flow_capture;
-- Enable CDC on tables. The below query enables CDC on table 'dbo.foobar',
-- you should add similar query for all other tables you intend to capture.
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foobar', @role_name = 'flow_capture';
```

3. In the Cloud Console, note the instance's host under Public IP Address. Its port will always be `1433`.
   Together, you'll use the host:port as the `address` property when you configure the connector.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SQL Server source connector.

### Properties

#### Endpoint

| Property                        | Title               | Description                                                                                                                                 | Type    | Required/Default           |
| ------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ------- | -------------------------- |
| **`/address`**                  | Server Address      | The host or host:port at which the database can be reached.                                                                                 | string  | Required                   |
| **`/database`**                 | Database            | Logical database name to capture from.                                                                                                      | string  | Required                   |
| **`/user`**                     | User                | The database user to authenticate as.                                                                                                       | string  | Required, `"flow_capture"` |
| **`/password`**                 | Password            | Password for the specified database user.                                                                                                   | string  | Required                   |
| `/advanced`                     | Advanced Options    | Options for advanced users. You should not typically need to modify these.                                                                  | object  |                            |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query.                                                    | integer | `4096`                     |
| `/advanced/skip_backfills`      | Skip Backfills      | A comma-separated list of fully-qualified table names which should not be backfilled.                                                       | string  |                            |

#### Bindings

| Property         | Title               | Description                                                                                                                                                                  | Type                                                          | Required/Default |
| ---------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- | ---------------- |
| **`/namespace`** | Namespace           | The [namespace/schema](https://learn.microsoft.com/en-us/sql/relational-databases/databases/databases?view=sql-server-ver16#basic-information-about-databases) of the table. | string                                                        | Required         |
| **`/stream`**    | Stream              | Table name.                                                                                                                                                                  | string                                                        | Required         |
| `/primary_key`   | Primary Key Columns | array                                                                                                                                                                        | The columns which together form the primary key of the table. |                  |

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

[Learn more about capture definitions.](/concepts/captures.md)

## Specifying Flow collection keys

Every Flow collection must have a [key](/concepts/collections.md#keys).
As long as your SQL Server tables have a primary key specified, the connector will set the
corresponding collection's key accordingly.

In cases where a SQL Server table you want to capture doesn't have a primary key,
you can manually add it to the collection definition during the [capture creation workflow](/guides/create-dataflow.md#create-a-capture).

1. After you input the endpoint configuration and click **Next**,
   the tables in your database have been mapped to Flow collections.
   Click each collection's **Specification** tab and identify a collection where `"key": [ ],` is empty.

2. Click inside the empty key value in the editor and input the name of column in the table to use as the key, formatted as a JSON pointer. For example `"key": ["/foo"],`

   Make sure the key field is required, not nullable, and of an [allowed type](/concepts/collections.md#schema-restrictions).
   Make any other necessary changes to the [collection specification](/concepts/collections.md#specification) to accommodate this.

3. Repeat with other missing collection keys, if necessary.

4. Save and publish the capture as usual.
