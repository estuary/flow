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
The simplest way to meet the above prerequisites is to connect to the database as a superuser and change the WAL level.

For a more restricted setup, create a new user with just the required permissions as detailed in the following steps:

1. Create a new user and password:
```console
CREATE USER flow_capture WITH PASSWORD 'secret' REPLICATION;
```
2. Assign the appropriate role.
    1. If using PostgreSQL v14 or later:
    ```console
    GRANT pg_read_all_data TO flow_capture;
    ```

    2. If using an earlier version:

    ```console
    GRANT SELECT ON ALL TABLES IN SCHEMA public, <others> TO flow_capture;
    GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
    ```

    where `<others>` lists all schemas that will be captured.
    :::info
    If an even more restricted set of permissions is desired, you can also grant SELECT on
    just the specific table(s) which should be captured from. The ‘information_schema’ and
    ‘pg_catalog’ access is required for stream auto-discovery, but not for capturing already
    configured streams.
    :::
3. Create the watermarks table, grant privileges, and create publication:

```console
CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);

GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
CREATE PUBLICATION flow_publication FOR ALL TABLES;
```
4. Set WAL level to logical:
```console
ALTER SYSTEM SET wal_level = logical;
```
5. Restart PostgreSQL to allow the WAL level change to take effect.

## Configuration
You may set up the configuration using the `flowctl discover` workflow, or with Flow's coming UI. Either of these methods will set up a basic [capture](../../../concepts/catalog-entities/captures.md) configuration, but you must supply additional values for the PostgreSQL connector.

### Values
| Value | Name | Type | Required/Default | Details |
|-------|------|------|---------| --------|
|  |Tenant |String| Required | The tenant in which to create the capture. This is the prefix of your cloud storage bucket. |
| |Name | String | Required |The unique name of the capture |
| `connectionURI` | Database Connection URL | String | Required | Connection parameters, as a libpq-compatible connection string |
| `max_lifespan_seconds` | Maximum Connector Lifespan (seconds) | number | 0.0 | When nonzero, imposes a maximum runtime after which to unconditionally shut down |
| `poll_timeout_seconds` | Poll Timeout (seconds) | number | 0.0 | When tail=false, controls how long to sit idle before shutting down |
| `publication_name` | Publication Name | string | `"flow_publication"` | The name of the PostgreSQL publication to replicate from |
| `slot_name` | Replication Slot Name | string | `"flow_slot"` | The name of the PostgreSQL replication slot to replicate from |
| `watermarks_table` | Watermarks Table | string | `"public.flow_watermarks"` | The name of the table used for watermark writes during backfills |

### Sample
A minimal capture definition within the catalog spec will look like the following:

```yaml
captures:
  $tenant/CAPTURE_NAME:
    endpoint:
      airbyteSource:
        image: "ghcr.io/estuary/source-postgres:dev"
        config:
          connectionURI: "postgres://flow:flow@localhost:5432/flow"
          # slot_name: “flow_slot”                     # Default
          # publication_name: “flow_publication”       # Default
          # watermarks_table: “public.flow_watermarks” # Default
    bindings:
      - resource:
        stream: ${TABLE_NAME}
        syncMode: incremental
      target: ${COLLECTION_NAME}
```
We recommend using `flowctl discover` to generate it, as detailed below. Your capture definition will likely be more complex, with a  **binding** for each table in the source database.


## How to use `flowctl discover`
Currently, `flowctl discover` is the provided method to begin setting up a capture, and saves significant time
compared to manually writing the catalog spec. `discover` generates the capture specification as well as the
**collections** you'll need to perpetuate each bound resource within the Flow runtime.

1. In your terminal, run:
```console
flowctl discover --image=ghcr.io/estuary/source-postgres:dev
```
This generates a config from the latest version of the connector, provided as a Docker image.

2. Open the config file called `discover-source-postgres-config.yaml`. This is your space to specify the required values detailed above. Fill in `connectionURI` (required) and modify other values, if you'd like.
3. Run the command again:
```console
flowctl discover --image=ghcr.io/estuary/source-postgres:dev
```
4. Open the resulting catalog spec file, which has a name like `discover-source-postgres.flow.yaml`.
Note the capture definition and the collection(s) created to support each binding.

You can now continue to build out and customize your catalog.
