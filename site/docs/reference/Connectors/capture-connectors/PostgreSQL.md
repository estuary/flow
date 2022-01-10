This connector uses change data capture (CDC) to continuously capture updates in a PostgreSQL database into one or more Flow collections.

`ghcr.io/estuary/source-postgres:dev` provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites
To use this connector, you'll need a PostgreSQL database setup with the following:
* [Logical replication enabled](https://www.postgresql.org/docs/current/runtime-config-wal.html) — `wal_level=logical`
* [User role](https://www.postgresql.org/docs/current/sql-createrole.html) with `REPLICATION` attribute
* A [replication slot](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS). This represents a “cursor” into the PostgreSQL write-ahead log from which change events can be read.
    * Optional; if none exist, one will be created by the connector.
* A [publication](https://www.postgresql.org/docs/current/sql-createpublication.html). This represents the set of tables for which change events will be reported.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.
* A watermarks table. The watermarks table is a small “scratch space” to which the connector occasionally writes a small amount of data to ensure accuracy when backfilling preexisting table contents.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.


### Setup
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
There are various ways to configure and implement connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and code sample below provide configuration details specific to the PostgreSQL source connector.

### Values
| Value | Name | Type | Required/Default | Details |
|-------|------|------|---------| --------|
| `database` | Database | string | `"postgres"` | Logical database name to capture from. |
| `host` | Host | String | Required | Host name of the database to connect to. |
| `port` | Port | uint16 | `5432` | Port on which to connect to the database. |
| `user` | User | String | Required | Database user to use. |
| `password` | Password | string | Required | User password configured within the database. |
| `publication_name` | Publication Name | string | `"flow_publication"` | The name of the PostgreSQL publication to replicate from |
| `slot_name` | Replication Slot Name | string | `"flow_slot"` | The name of the PostgreSQL replication slot to replicate from |
| `watermarks_table` | Watermarks Table | string | `"public.flow_watermarks"` | The name of the table used for watermark writes during backfills |

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
