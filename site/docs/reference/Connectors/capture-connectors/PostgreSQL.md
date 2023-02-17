---
sidebar_position: 8
---
This connector uses change data capture (CDC) to continuously capture updates in a PostgreSQL database into one or more Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-postgres:dev`](https://github.com/estuary/connectors/pkgs/container/source-postgres) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported versions and platforms

This connector supports PostgreSQL versions 10.0 and later on major cloud platforms, as well as self-hosted instances.

Setup instructions are provided for the following platforms:

* [Self-hosted PostgreSQL](#self-hosted-postgresql)
* [Amazon RDS](#amazon-rds)
* [Amazon Aurora](#amazon-aurora)
* [Google Cloud SQL](#google-cloud-sql)
* [Azure Database for PostgreSQL](#azure-database-for-postgresql)

## Prerequisites

You'll need a PostgreSQL database setup with the following:
* [Logical replication enabled](https://www.postgresql.org/docs/current/runtime-config-wal.html) — `wal_level=logical`
* [User role](https://www.postgresql.org/docs/current/sql-createrole.html) with `REPLICATION` attribute
* A [replication slot](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS). This represents a “cursor” into the PostgreSQL write-ahead log from which change events can be read.
    * Optional; if none exist, one will be created by the connector.
    * If you wish to run multiple captures from the same database, each must have its own slot.
    You can create these slots yourself, or by specifying a name other than the default in the advanced [configuration](#configuration).
* A [publication](https://www.postgresql.org/docs/current/sql-createpublication.html). This represents the set of tables for which change events will be reported.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.
* A watermarks table. The watermarks table is a small “scratch space” to which the connector occasionally writes a small amount of data to ensure accuracy when backfilling preexisting table contents.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.

## Setup

To meet these requirements, follow the steps for your hosting type.

* [Self-hosted PostgreSQL](#self-hosted-postgresql)
* [Amazon RDS](#amazon-rds)
* [Amazon Aurora](#amazon-aurora)
* [Google Cloud SQL](#google-cloud-sql)
* [Azure Database for PostgreSQL](#azure-database-for-postgresql)

### Self-hosted PostgreSQL

The simplest way to meet the above prerequisites is to change the WAL level and have the connector use a database superuser role.

For a more restricted setup, create a new user with just the required permissions as detailed in the following steps:

1. Connect to your instance and create a new user and password:
```sql
CREATE USER flow_capture WITH PASSWORD 'secret' REPLICATION;
```
2. Assign the appropriate role.
    1. If using PostgreSQL v14 or later:

    ```sql
    GRANT pg_read_all_data TO flow_capture;
    ```

    2. If using an earlier version:

    ```sql
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to flow_capture;
    GRANT SELECT ON ALL TABLES IN SCHEMA public, <others> TO flow_capture;
    GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
    ```

    where `<others>` lists all schemas that will be captured from.
    :::info
    If an even more restricted set of permissions is desired, you can also grant SELECT on
    just the specific table(s) which should be captured from. The ‘information_schema’ and
    ‘pg_catalog’ access is required for stream auto-discovery, but not for capturing already
    configured streams.
    :::
3. Create the watermarks table, grant privileges, and create publication:

```sql
CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
CREATE PUBLICATION flow_publication FOR ALL TABLES;
```
4. Set WAL level to logical:
```sql
ALTER SYSTEM SET wal_level = logical;
```
5. Restart PostgreSQL to allow the WAL level change to take effect.


### Amazon RDS

1. Allow connections to the database from the Estuary Flow IP address.

   1. [Modify the database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html), setting **Public accessibility** to **Yes**.

   2. Edit the VPC security group associated with your database, or create a new VPC security group and associate it with the database.
      Refer to the [steps in the Amazon documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create).
      Create a new inbound rule and a new outbound rule that allow all traffic from the IP address `34.121.207.128`.

   :::info
   Alternatively, you can allow secure connections via SSH tunneling. To do so:
     * Follow the guide to [configure an SSH server for tunneling](../../../../guides/connect-network/)
     * When you configure your connector as described in the [configuration](#configuration) section above,
        including the additional `networkTunnel` configuration to enable the SSH tunnel.
        See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
        for additional details and a sample.
   :::

2. Enable logical replication on your RDS PostgreSQL instance.

   1. Create a [parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Creating).
   Create a unique name and description and set the following properties:
      * **Family**: postgres13
      * **Type**: DB Parameter group

   2. [Modify the new parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Modifying) and set `rds.logical_replication=1`.

   3. [Associate the parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Associating) with the database.

   4. Reboot the database to allow the new parameter group to take effect.

3. In the PostgreSQL client, connect to your instance and run the following commands to create a new user for the capture with appropriate permissions,
and set up the watermarks table and publication.
  ```sql
  CREATE USER flow_capture WITH PASSWORD 'secret';
  GRANT rds_replication TO flow_capture;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flow_capture;
  CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
  GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
  CREATE PUBLICATION flow_publication FOR ALL TABLES;
  ```

6. In the [RDS console](https://console.aws.amazon.com/rds/), note the instance's Endpoint and Port. You'll need these for the `address` property when you configure the connector.

### Amazon Aurora

You must apply some of the settings to the entire Aurora DB cluster, and others to a database instance within the cluster
(typically, you'll want to use a [replica, or reader instance](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.html)).
For each step, take note of which entity you're working with.

1. Allow connections to the DB instance from the Estuary Flow IP address.

   1. [Modify the instance](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Modifying.html#Aurora.Modifying.Instance), choosing **Publicly accessible** in the **Connectivity** settings.

   2. Edit the VPC security group associated with your instance, or create a new VPC security group and associate it with the instance.
      Refer to the [steps in the Amazon documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create).
      Create a new inbound rule and a new outbound rule that allow all traffic from the IP address `34.121.207.128`.

   :::info
   Alternatively, you can allow secure connections via SSH tunneling. To do so:
     * Follow the guide to [configure an SSH server for tunneling](../../../../guides/connect-network/)
     * When you configure your connector as described in the [configuration](#configuration) section above,
        including the additional `networkTunnel` configuration to enable the SSH tunnel.
        See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
        for additional details and a sample.
   :::

2. Enable logical replication on your Aurora DB cluster.

   1. Create a [parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_WorkingWithDBClusterParamGroups.html#USER_WorkingWithParamGroups.CreatingCluster).
   Create a unique name and description and set the following properties:
      * **Family**: aurora-postgresql13, or substitute the version of Aurora PostgreSQL used for your cluster.
      * **Type**: DB Cluster Parameter group

   2. [Modify the new parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_WorkingWithDBClusterParamGroups.html#USER_WorkingWithParamGroups.ModifyingCluster) and set `rds.logical_replication=1`.

   3. [Associate the parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_WorkingWithDBClusterParamGroups.html#USER_WorkingWithParamGroups.AssociatingCluster) with the DB cluster.

   4. Reboot the cluster to allow the new parameter group to take effect.

3. In the PostgreSQL client, connect to your instance and run the following commands to create a new user for the capture with appropriate permissions,
and set up the watermarks table and publication.
  ```sql
  CREATE USER flow_capture WITH PASSWORD 'secret';
  GRANT rds_replication TO flow_capture;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flow_capture;
  CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
  GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
  CREATE PUBLICATION flow_publication FOR ALL TABLES;
  ```

6. In the [RDS console](https://console.aws.amazon.com/rds/), note the instance's Endpoint and Port. You'll need these for the `address` property when you configure the connector.

### Google Cloud SQL

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

2. Set [the `cloudsql.logical_decoding` flag to `on`](https://cloud.google.com/sql/docs/postgres/flags) to enable logical replication on your Cloud SQL PostgreSQL instance.

3. In your PostgreSQL client, connect to your instance and issue the following commands to create a new user for the capture with appropriate permissions,
and set up the watermarks table and publication.

  ```sql
  CREATE USER flow_capture WITH REPLICATION
  IN ROLE cloudsqlsuperuser LOGIN PASSWORD 'secret';
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flow_capture;
  CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
  GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
  CREATE PUBLICATION flow_publication FOR ALL TABLES;
  ```

4. In the Cloud Console, note the instance's host under Public IP Address. Its port will always be `5432`.
Together, you'll use the host:port as the `address` property when you configure the connector.

### Azure Database for PostgreSQL

1. Allow connections to the database from the Estuary Flow IP address.

   1. Create a new [firewall rule](https://docs.microsoft.com/en-us/azure/postgresql/flexible-server/how-to-manage-firewall-portal#create-a-firewall-rule-after-server-is-created)
   that grants access to the IP address `34.121.207.128`.

   :::info
   Alternatively, you can allow secure connections via SSH tunneling. To do so:
     * Follow the guide to [configure an SSH server for tunneling](../../../../guides/connect-network/)
     * When you configure your connector as described in the [configuration](#configuration) section above,
        including the additional `networkTunnel` configuration to enable the SSH tunnel.
        See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
        for additional details and a sample.
   :::

2. In your Azure PostgreSQL instance's support parameters, [set replication to logical](https://docs.microsoft.com/en-us/azure/postgresql/single-server/concepts-logical#set-up-your-server) to enable logical replication.

3. In the PostgreSQL client, connect to your instance and run the following commands to create a new user for the capture with appropriate permissions.

```sql
CREATE USER flow_capture WITH PASSWORD 'secret' REPLICATION;
```

  * If using PostgreSQL v14 or later:

```sql
GRANT pg_read_all_data TO flow_capture;
```

  * If using an earlier version:

    ```sql
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to flow_capture;
        GRANT SELECT ON ALL TABLES IN SCHEMA public, <others> TO flow_capture;
        GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
    ```
    where `<others>` lists all schemas that will be captured from.

    :::info
    If an even more restricted set of permissions is desired, you can also grant SELECT on
    just the specific table(s) which should be captured from. The ‘information_schema’ and      ‘pg_catalog’ access is required for stream auto-discovery, but not for capturing already
    configured streams.
    :::

4. Set up the watermarks table and publication.

```sql
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to flow_capture;
GRANT SELECT ON ALL TABLES IN SCHEMA public, <others> TO flow_capture;
GRANT SELECT ON information_schema.columns, information_schema.tables, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_index, pg_catalog.pg_namespace TO flow_capture;
CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
CREATE PUBLICATION flow_publication FOR TABLE schema.table1, schema.table2;
```

5. Note the following important items for configuration:

   * Find the instance's host under Server Name, and the port under Connection Strings (usually `5432`). Together, you'll use the host:port as the `address` property when you configure the connector.
   * Format `user` as `username@databasename`; for example, `flow_capture@myazuredb`.

## Backfills and performance considerations

When the a PostgreSQL capture is initiated, by default, the connector first *backfills*, or captures the targeted tables in their current state. It then transitions to capturing change events on an ongoing basis.

This is desirable in most cases, as in ensures that a complete view of your tables is captured into Flow.
However, you may find it appropriate to skip the backfill, especially for extremely large tables.

In this case, you may turn of backfilling on a per-table basis. See [properties](#properties) for details.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the PostgreSQL source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/address`** | Address | The host or host:port at which the database can be reached. | string | Required |
| **`/database`** | Database | Logical database name to capture from. | string | Required, `"postgres"` |
| **`/user`** | User | The database user to authenticate as. | string | Required, `"flow_capture"` |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object |  |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query. | integer | `4096` |
| `/advanced/publicationName` | Publication Name | The name of the PostgreSQL publication to replicate from. | string | `"flow_publication"` |
| `/advanced/skip_backfills` | Skip Backfills | A comma-separated list of fully-qualified table names which should not be backfilled. | string |  |
| `/advanced/slotName` | Slot Name | The name of the PostgreSQL replication slot to replicate from. | string | `"flow_slot"` |
| `/advanced/watermarksTable` | Watermarks Table | The name of the table used for watermark writes during backfills. Must be fully-qualified in &#x27;&lt;schema&gt;.&lt;table&gt;&#x27; form. | string | `"public.flow_watermarks"` |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| **`/namespace`** | Namespace | The [namespace/schema](https://www.postgresql.org/docs/9.1/ddl-schemas.html) of the table. | string | Required |
| **`/stream`** | Stream | Table name. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-postgres:dev"
        config:
          address: "localhost:5432"
          database: "postgres"
          user: "flow_capture"
          password: "secret"
    bindings:
      - resource:
          stream: ${TABLE_NAME}
          namespace: ${TABLE_NAMESPACE}
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}
```
Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures).

]

## TOASTed values

PostgreSQL has a hard page size limit, usually 8 KB, for performance reasons.
If your tables contain values that exceed the limit, those values can't be stored directly.
PostgreSQL uses [TOAST](https://www.postgresql.org/docs/current/storage-toast.html) (The Oversized-Attribute Storage Technique) to
store them separately.

TOASTed values can sometimes present a challenge for systems that rely on the PostgreSQL write-ahead log (WAL), like this connector.
If a change event occurs on a row that contains a TOASTed value, _but the TOASTed value itself is unchanged_, it is omitted from the WAL.
As a result, the connector emits a row update with the a value omitted, which might cause
unexpected results in downstream catalog tasks if adjustments are not made.

The PostgreSQL connector handles TOASTed values for you when you follow the [standard discovery workflow](../../../concepts/connectors.md#flowctl-discover)
or use the [Flow UI](../../../concepts/connectors.md#flow-ui) to create your capture.
It uses [merge](../../reduction-strategies/merge.md) [reductions](../../../concepts/schemas.md#reductions)
to fill in the previous known TOASTed value in cases when that value is omitted from a row update.

However, due to the event-driven nature of certain tasks in Flow, it's still possible to see unexpected results in your data flow, specifically:

- When you materialize the captured data to another system using a connector that requires [delta updates](../../../concepts/materialization.md#delta-updates)
- When you perform a [derivation](../../../concepts/derivations.md) that uses TOASTed values

### Troubleshooting

If you encounter an issue that you suspect is due to TOASTed values, try the following:

- Ensure your collection's schema is using the merge [reduction strategy](../../../concepts/schemas.md#reduce-annotations).
- [Set REPLICA IDENTITY to FULL](https://www.postgresql.org/docs/9.4/sql-altertable.html) for the table. This circumvents the problem by forcing the
WAL to record all values regardless of size. However, this can have performance impacts on your database and must be carefully evaluated.
- [Contact Estuary support](mailto:support@estuary.dev) for assistance.
