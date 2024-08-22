---
sidebar_position: 6
---

# Google Cloud SQL for PostgreSQL

This connector uses change data capture (CDC) to continuously capture updates in a PostgreSQL database into one or more Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-postgres:dev`](https://github.com/estuary/connectors/pkgs/container/source-postgres) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported versions and platforms

This connector supports PostgreSQL versions 10.0 and later.

## Prerequisites

You'll need a PostgreSQL database setup with the following:

- [Logical replication enabled](https://www.postgresql.org/docs/current/runtime-config-wal.html) — `wal_level=logical`
- [User role](https://www.postgresql.org/docs/current/sql-createrole.html) with `REPLICATION` attribute
- A [replication slot](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS). This represents a “cursor” into the PostgreSQL write-ahead log from which change events can be read.
  - Optional; if none exist, one will be created by the connector.
  - If you wish to run multiple captures from the same database, each must have its own slot.
    You can create these slots yourself, or by specifying a name other than the default in the advanced [configuration](#configuration).
- A [publication](https://www.postgresql.org/docs/current/sql-createpublication.html). This represents the set of tables for which change events will be reported.
  - In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.
- A watermarks table. The watermarks table is a small “scratch space” to which the connector occasionally writes a small amount of data to ensure accuracy when backfilling preexisting table contents.
  - In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.

## Setup

1. Allow connections between the database and Estuary Flow. There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - [Enable public IP on your database](https://cloud.google.com/sql/docs/mysql/configure-ip#add) and add the [Estuary Flow IP addresses](/reference/allow-ip-addresses) as authorized IP addresses.

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](../../../../../guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](../../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. On Google Cloud, navigate to your instance's Overview page. Click "Edit configuration". Scroll down to the Flags section. Click "ADD FLAG". Set [the `cloudsql.logical_decoding` flag to `on`](https://cloud.google.com/sql/docs/postgres/flags) to enable logical replication on your Cloud SQL PostgreSQL instance.

3. In your PostgreSQL client, connect to your instance and issue the following commands to create a new user for the capture with appropriate permissions,
   and set up the watermarks table and publication.

```sql
CREATE USER flow_capture WITH REPLICATION
IN ROLE cloudsqlsuperuser LOGIN PASSWORD 'secret';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flow_capture;
CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks, <other_tables>;
```

where `<other_tables>` lists all tables that will be captured from. The `publish_via_partition_root`
setting is recommended (because most users will want changes to a partitioned table to be captured
under the name of the root table) but is not required.

4. In the Cloud Console, note the instance's host under Public IP Address. Its port will always be `5432`.
   Together, you'll use the host:port as the `address` property when you configure the connector.

## Backfills and performance considerations

When the a PostgreSQL capture is initiated, by default, the connector first _backfills_, or captures the targeted tables in their current state. It then transitions to capturing change events on an ongoing basis.

This is desirable in most cases, as in ensures that a complete view of your tables is captured into Flow.
However, you may find it appropriate to skip the backfill, especially for extremely large tables.

In this case, you may turn of backfilling on a per-table basis. See [properties](#properties) for details.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the PostgreSQL source connector.

### Properties

#### Endpoint

| Property                        | Title               | Description                                                                                                                                 | Type    | Required/Default           |
| ------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ------- | -------------------------- |
| **`/address`**                  | Address             | The host or host:port at which the database can be reached.                                                                                 | string  | Required                   |
| **`/database`**                 | Database            | Logical database name to capture from.                                                                                                      | string  | Required, `"postgres"`     |
| **`/user`**                     | User                | The database user to authenticate as.                                                                                                       | string  | Required, `"flow_capture"` |
| **`/password`**                 | Password            | Password for the specified database user.                                                                                                   | string  | Required                   |
| `/advanced`                     | Advanced Options    | Options for advanced users. You should not typically need to modify these.                                                                  | object  |                            |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query.                                                    | integer | `4096`                     |
| `/advanced/publicationName`     | Publication Name    | The name of the PostgreSQL publication to replicate from.                                                                                   | string  | `"flow_publication"`       |
| `/advanced/skip_backfills`      | Skip Backfills      | A comma-separated list of fully-qualified table names which should not be backfilled.                                                       | string  |                            |
| `/advanced/slotName`            | Slot Name           | The name of the PostgreSQL replication slot to replicate from.                                                                              | string  | `"flow_slot"`              |
| `/advanced/watermarksTable`     | Watermarks Table    | The name of the table used for watermark writes during backfills. Must be fully-qualified in &#x27;&lt;schema&gt;.&lt;table&gt;&#x27; form. | string  | `"public.flow_watermarks"` |
| `/advanced/sslmode`             | SSL Mode            | Overrides SSL connection behavior by setting the 'sslmode' parameter.                                                                       | string  |                            |

#### Bindings

| Property         | Title     | Description                                                                                | Type   | Required/Default |
| ---------------- | --------- | ------------------------------------------------------------------------------------------ | ------ | ---------------- |
| **`/namespace`** | Namespace | The [namespace/schema](https://www.postgresql.org/docs/9.1/ddl-schemas.html) of the table. | string | Required         |
| **`/stream`**    | Stream    | Table name.                                                                                | string | Required         |
| **`/syncMode`**  | Sync mode | Connection method. Always set to `incremental`.                                            | string | Required         |

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

[Learn more about capture definitions.](../../../../concepts/captures.md#pull-captures)

## TOASTed values

PostgreSQL has a hard page size limit, usually 8 KB, for performance reasons.
If your tables contain values that exceed the limit, those values can't be stored directly.
PostgreSQL uses [TOAST](https://www.postgresql.org/docs/current/storage-toast.html) (The Oversized-Attribute Storage Technique) to
store them separately.

TOASTed values can sometimes present a challenge for systems that rely on the PostgreSQL write-ahead log (WAL), like this connector.
If a change event occurs on a row that contains a TOASTed value, _but the TOASTed value itself is unchanged_, it is omitted from the WAL.
As a result, the connector emits a row update with the a value omitted, which might cause
unexpected results in downstream catalog tasks if adjustments are not made.

The PostgreSQL connector handles TOASTed values for you when you follow the [standard discovery workflow](../../../../concepts/connectors.md#flowctl-discover)
or use the [Flow UI](../../../../concepts/connectors.md#flow-ui) to create your capture.
It uses [merge](../../../reduction-strategies/merge.md) [reductions](../../../../concepts/schemas.md#reductions)
to fill in the previous known TOASTed value in cases when that value is omitted from a row update.

However, due to the event-driven nature of certain tasks in Flow, it's still possible to see unexpected results in your data flow, specifically:

- When you materialize the captured data to another system using a connector that requires [delta updates](../../../../concepts/materialization.md#delta-updates)
- When you perform a [derivation](../../../../concepts/derivations.md) that uses TOASTed values

### Troubleshooting

If you encounter an issue that you suspect is due to TOASTed values, try the following:

- Ensure your collection's schema is using the merge [reduction strategy](../../../../concepts/schemas.md#reduce-annotations).
- [Set REPLICA IDENTITY to FULL](https://www.postgresql.org/docs/9.4/sql-altertable.html) for the table. This circumvents the problem by forcing the
  WAL to record all values regardless of size. However, this can have performance impacts on your database and must be carefully evaluated.
- [Contact Estuary support](mailto:support@estuary.dev) for assistance.

## Publications

It is recommended that the publication used by the capture only contain the tables that will be captured. In some cases it may be desirable to create this publication for all tables in the database instead of specific tables, for example using:

```sql
CREATE PUBLICATION flow_publication FOR ALL TABLES WITH (publish_via_partition_root = true);
```

Caution must be used if creating the publication in this way as all existing tables (even those not part of the capture) will be included in it, and if any of them do not have a primary key they will no longer be able to process updates or deletes.
