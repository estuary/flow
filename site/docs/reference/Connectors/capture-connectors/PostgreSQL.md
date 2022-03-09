This connector uses change data capture (CDC) to continuously capture updates in a PostgreSQL database into one or more Flow collections.

[`ghcr.io/estuary/source-postgres:dev`](https://github.com/estuary/connectors/pkgs/container/source-postgres) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

This connector supports PostgreSQL versions 10.0 and later.

You'll need a PostgreSQL database setup with the following:
* [Logical replication enabled](https://www.postgresql.org/docs/current/runtime-config-wal.html) — `wal_level=logical`
* [User role](https://www.postgresql.org/docs/current/sql-createrole.html) with `REPLICATION` attribute
* A [replication slot](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS). This represents a “cursor” into the PostgreSQL write-ahead log from which change events can be read.
    * Optional; if none exist, one will be created by the connector.
* A [publication](https://www.postgresql.org/docs/current/sql-createpublication.html). This represents the set of tables for which change events will be reported.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.
* A watermarks table. The watermarks table is a small “scratch space” to which the connector occasionally writes a small amount of data to ensure accuracy when backfilling preexisting table contents.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.


### Setup

:::info
These setup instructions are PostgreSQL instances you manage yourself. If you use a cloud-based managed service
for your database, different setup steps may be required.

Instructions for setup on Amazon RDS can be found [here](#postgresql-on-amazon-rds). If you use a different managed service
and the standard steps don't work as expected,
contact [Estuary support](mailto:support@estuary.dev).
:::

The simplest way to meet the above prerequisites is to change the WAL level and have the connector use a database superuser role.

For a more restricted setup, create a new user with just the required permissions as detailed in the following steps:

1. Create a new user and password:
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

## Configuration

There are various ways to configure connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and YAML sample below provide configuration details specific to the PostgreSQL source connector.

### Values

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/database`** |  | Logical database name to capture from. | string | Required, `"postgres"` |
| **`/host`** |  | Host name of the database to connect to. | string | Required |
| **`/password`** |  | User password configured within the database. | string | Required |
| **`/port`** |  |  | integer | Required, `5432` |
| `/publicationName` |  | The name of the PostgreSQL publication to replicate from. | string | `"flow_publication"` |
| `/slotName` |  | The name of the PostgreSQL replication slot to replicate from. | string | `"flow_slot"` |
| **`/user`** |  | Database user to use. | string | Required, `"postgres"` |
| `/watermarksTable` |  | The name of the table used for watermark writes during backfills. | string | `"public.flow_watermarks"` |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| `/namespace` | Namespace | The [namespace](https://www.postgresql.org/docs/9.1/ddl-schemas.html) of the table, if used. | string | |
| **`/stream`** | Stream | Table name. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample

A minimal capture definition within the catalog spec will look like the following:

```yaml
captures:
  ${tenant}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-postgres:dev"
        config:
          host: "localhost"
          port: 5432
          database: "flow"
          user: "flow_capture"
          password: "secret"
          # slot_name: “flow_slot”                     # Default
          # publication_name: “flow_publication”       # Default
          # watermarks_table: “public.flow_watermarks” # Default
    bindings:
      - resource:
          stream: ${TABLE_NAME}
          namespace: ${TABLE_NAMESPACE}
          syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}
```
Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures).

## Connecting to secure networks

The PostgreSQL source connector [supports SSH tunneling](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
to allow Flow to connect to databases ports in secure networks.

To set up and configure your SSH server, see the [guide](../../../../guides/connect-network/).

## PostgreSQL on Amazon RDS

Amazon Relational Database Service (RDS) is a managed web service providing cloud-based instances
of popular relational databases, including PostgreSQL.

You can use this connector for PostgreSQL instances on RDS, but the setup requirements are different.

### Setup

1. You'll need to configure secure access to the database to enable the Flow capture.
  Currently, Estuary supports SSH tunneling to allow this.
  Follow the guide to [configure an SSH server for tunneling](../../../../guides/connect-network/).

2. Enable logical replication on your existing RDS PostgreSQL instance.

  a. Create a [parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.html)
  with the following properties:
    * **Family**: postgres13
    * **Type**: DB Parameter group
    * **Name**: postgres13-logical-replication
    * **Description**: Database with logical replication enabled

  b. [Modify the new parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.html#USER_WorkingWithParamGroups.Modifying) and set `rds.logical_replication=1`.

  c. [Associate the parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.html#USER_WorkingWithParamGroups.Associating) with the database.

  d. Reboot the database to allow the new parameter group to take effect.

3. In the PostgreSQL client, run the following commands to create a new user for the capture with appropriate permissions,
and set up the watermarks table and publication.
  ```sql
  CREATE USER flow_capture WITH PASSWORD '<secret>';
  GRANT rds_replication TO flow_capture;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flow_capture;
  CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
  GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
  CREATE PUBLICATION flow_publication FOR ALL TABLES;
  ```

4. Configure your connector as described in the [configuration](#configuration) section above,
with the additional of the `proxy` stanza to enable the SSH tunnel.
See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
for additional details and a sample.
You can find the `remoteHost` and `remotePort` in the [RDS console](https://console.aws.amazon.com/rds/) as the Endpoint and Port, respectively.

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
