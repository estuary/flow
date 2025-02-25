---
sidebar_position: 6
---

# PostgreSQL

This connector uses change data capture (CDC) to continuously capture updates in a PostgreSQL database into one or more Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-postgres:dev`](https://github.com/estuary/connectors/pkgs/container/source-postgres) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

For managed PostgreSQL instances that do not support logical replication, we offer a [PostgreSQL Batch Connector](./postgres-batch/) as an alternative.

## Supported versions and platforms

This connector supports PostgreSQL versions 10.0 and later on major cloud platforms, as well as self-hosted instances.

Setup instructions are provided for the following platforms:

- [Self-hosted PostgreSQL](#self-hosted-postgresql)
- [Amazon RDS](./amazon-rds-postgres/)
- [Amazon Aurora](#amazon-aurora)
- [Google Cloud SQL](./google-cloud-sql-postgres/)
- [Azure Database for PostgreSQL](#azure-database-for-postgresql)

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
  - **For read-only environments**, the capture can operate in read-only mode which does not require a watermarks table. See [Read-Only Captures](#read-only-captures) for details.

:::tip Configuration Tip
To configure this connector to capture data from databases hosted on your internal network, you must set up SSH tunneling. For more specific instructions on setup, see [configure connections with SSH tunneling](/guides/connect-network/).
:::

## Setup

To meet these requirements, follow the steps for your hosting type.

- [Self-hosted PostgreSQL](#self-hosted-postgresql)
- [Amazon RDS](./amazon-rds-postgres/)
- [Amazon Aurora](#amazon-aurora)
- [Google Cloud SQL](./google-cloud-sql-postgres/)
- [Azure Database for PostgreSQL](#azure-database-for-postgresql)
- [Supabase](Supabase)

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
   GRANT SELECT ON ALL TABLES IN SCHEMA public, <other_schema> TO flow_capture;
   GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
   ```

   where `<other_schema>` lists all schemas that will be captured from.
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
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks, <other_tables>;
```

where `<other_tables>` lists all tables that will be captured from. The `publish_via_partition_root`
setting is recommended (because most users will want changes to a partitioned table to be captured
under the name of the root table) but is not required.

4. Set WAL level to logical:

```sql
ALTER SYSTEM SET wal_level = logical;
```

5. Restart PostgreSQL to allow the WAL level change to take effect.

### Amazon Aurora

You must apply some of the settings to the entire Aurora DB cluster, and others to a database instance within the cluster.
For each step, take note of which entity you're working with.

1. Allow connections between the database and Estuary Flow. There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - [Modify the instance](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Modifying.html#Aurora.Modifying.Instance), choosing **Publicly accessible** in the **Connectivity** settings.
      - Edit the VPC security group associated with your instance, or create a new VPC security group and associate it with the instance as described in [the Amazon documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create). Create a new inbound rule and a new outbound rule that allow all traffic from the [Estuary Flow IP addresses](/reference/allow-ip-addresses).

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. Enable logical replication on your Aurora DB cluster.

   1. Create a [parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_WorkingWithDBClusterParamGroups.html#USER_WorkingWithParamGroups.CreatingCluster).
      Create a unique name and description and set the following properties:

      - **Family**: aurora-postgresql13, or substitute the version of Aurora PostgreSQL used for your cluster.
      - **Type**: DB Cluster Parameter group

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
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks, <other_tables>;
```

where `<other_tables>` lists all tables that will be captured from. The `publish_via_partition_root`
setting is recommended (because most users will want changes to a partitioned table to be captured
under the name of the root table) but is not required.

6. In the [RDS console](https://console.aws.amazon.com/rds/), note the instance's Endpoint and Port. You'll need these for the `address` property when you configure the connector.

### Azure Database for PostgreSQL

1. Allow connections between the database and Estuary Flow. There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - Create a new [firewall rule](https://docs.microsoft.com/en-us/azure/postgresql/flexible-server/how-to-manage-firewall-portal#create-a-firewall-rule-after-server-is-created) that grants access to the [Estuary Flow IP addresses](/reference/allow-ip-addresses).

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. In your Azure PostgreSQL instance's support parameters, [set replication to logical](https://docs.microsoft.com/en-us/azure/postgresql/single-server/concepts-logical#set-up-your-server) to enable logical replication.

3. In the PostgreSQL client, connect to your instance and run the following commands to create a new user for the capture with appropriate permissions.

```sql
CREATE USER flow_capture WITH PASSWORD 'secret' REPLICATION;
```

- If using PostgreSQL v14 or later:

```sql
GRANT pg_read_all_data TO flow_capture;
```

- If using an earlier version:

  ```sql
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to flow_capture;
      GRANT SELECT ON ALL TABLES IN SCHEMA public, <others> TO flow_capture;
      GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
  ```

  where `<others>` lists all schemas that will be captured from.

  :::info
  If an even more restricted set of permissions is desired, you can also grant SELECT on
  just the specific table(s) which should be captured from. The ‘information_schema’ and ‘pg_catalog’ access is required for stream auto-discovery, but not for capturing already
  configured streams.
  :::

4. Set up the watermarks table and publication.

```sql
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to flow_capture;
GRANT SELECT ON ALL TABLES IN SCHEMA public, <others> TO flow_capture;
GRANT SELECT ON information_schema.columns, information_schema.tables, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_index, pg_catalog.pg_namespace TO flow_capture;
CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks, <other_tables>;
```

5. Note the following important items for configuration:

   - Find the instance's host under Server Name, and the port under Connection Strings (usually `5432`). Together, you'll use the host:port as the `address` property when you configure the connector.
   - Format `user` as `username@databasename`; for example, `flow_capture@myazuredb`.

## Backfills and performance considerations

When the PostgreSQL capture is initiated, by default, the connector first _backfills_, or captures the targeted tables in their current state. It then transitions to capturing change events on an ongoing basis.

This is desirable in most cases, as it ensures that a complete view of your tables is captured into Flow.
However, you may find it appropriate to skip the backfill, especially for extremely large tables.

In this case, you may turn off backfilling on a per-table basis. See [properties](#properties) for details.

## WAL Retention and Tuning Parameters

Postgres logical replication works by reading change events from the writeahead log,
reordering WAL events in memory on the server, and sending them to the client in the
order that transactions were committed. The replication slot used by the capture is
essentially a cursor into that logical sequence of changes.

Because of how Postgres reorders WAL events into atomic transactions, there are two
distinct LSNs which matter when it comes to WAL retention. The `confirmed_flush_lsn`
property of a replication slot represents the latest event in the WAL which has been
sent to and confirmed by the client. However there may be some number of uncommitted
changes prior to this point in the WAL which are still relevant and will be sent to
the client in later transactions. Thus there is also a `restart_lsn` property which
represents the point in the WAL from which logical decoding must resume in the future
if the replication connection is closed and restarted.

The server cannot clean up old WAL files so long as there are active replication slots
whose `restart_lsn` position requires them. There are two ways that `restart_lsn` might
get stuck at a particular point in the WAL:

1. When a capture is deleted, disabled, or repeatedly failing for other reasons,
   it is not able to advance the `confirmed_flush_lsn` and thus `restart_lsn` cannot
   advance either.
2. When a long-running transaction is open on the server the `restart_lsn` of a
   replication slot may be unable to advance even though `confirmed_flush_lsn` is.

By default Postgres will retain an unbounded amount of WAL data and fill up the entire
disk if a replication slot stops advancing. There are two ways to address this:

1. When deleting a capture, make sure that the replication slot is also successfully deleted.
   - You can list replication slots with the query `SELECT * FROM pg_replication_slots` and
     can drop the replication slot manually with `pg_drop_replication_slot('flow_slot')`.
2. The database setting `max_slot_wal_keep_size` can be used to bound the maximum amount of
   WAL data which a replication slot can force the database to retain.
   - This setting defaults to `-1` (unlimited) but should be set on production databases
     to protect them from unbounded WAL retention filling up the entire disk.
   - Proper sizing of this setting is complex for reasons discussed below, but a value
     of `50GB` should be enough for many databases.

When the `max_slot_wal_keep_size` limit is exceeded, Postgres will terminate any active
replication connections using that slot and invalidate the replication slot so that it
can no longer be used. If Postgres invalidates the replication slot, the Flow capture
using that slot will fail and manual intervention will be required to restart the capture
and re-backfill all tables.

Setting too low of a limit for `max_slot_wal_keep_size` can cause additional failures
in the presence of long-running transactions. Even when a client is actively receiving
and acknowledging replication events, a long-running transaction can cause the `restart_lsn`
of the replication slot to remain stuck until that transaction commits. Thus the value of
`max_slot_wal_keep_size` needs to be set high enough to avoid this happening. The precise
value depends on the overall change rate of your database and worst-case transaction open
time, but there is no downside to using a larger value provided you have enough free disk
space.

## Read-Only Captures

The PostgreSQL CDC connector supports capturing data in "read-only" mode which does not
require a watermark table or watermark writes. This is not the default mode of operation
because it comes with one very significant caveat: you must ensure there are frequent
changes to at least one of the tables being captured.

:::warning
When using a read-only capture, you must either ensure that some table you are capturing
is modified regularly, or else create a dedicated "heartbeat" table which is updated
every few minutes and include that in the capture.
:::

PostgreSQL logical replication can only acknowledge changes which modify at least one
table in the publication. If all of the tables being captured are idle while there are
significant changes to other tables on the same server, the replication slot cannot
advance and PostgreSQL WAL retention will continue to grow, potentially without bound (see [WAL Retention and Tuning Parameters](#wal-retention-and-tuning-parameters))
for more information.

To enable read-only operation:

- In the Flow web app: Select the "Read-Only Capture" checkbox in the "Advanced Options" section of the capture configuration.
- In the YAML configuration: Set read_only_capture: true in the advanced section of the config.

### Capturing from Read-Only Standbys

A read-only capture can be used to capture from a read-only standby replica. This feature can
be useful when you want to offload the impact of CDC operations from your primary database to
a replica.

In addition to the requirement that there be frequent writes to at least one captured table,
there is one other significant constraint on this setup: the `hot_standby_feedback` setting
must be enabled on the standby from which you intend to capture.

This setting prevents the primary database from vacuuming rows that are still needed by the
standby for logical decoding. If not enabled, catalog metadata may get vacuumed on the primary
DB while still needed for logical decoding on the standby. This is not a rare edge case, and
will frequently be observed if there are even a few minutes of downtime or replication lag.

This will cause the logical replication slot to be invalidated, breaking the capture process.

The solution is to set `hot_standby_feedback = on` so that the standby replica will keep the
upstream database informed about what catalog metadata needs to be retained. To enable hot
standby feedback on a self-managed PostgreSQL instance, run the following statements
(on the standby replica):

```sql
ALTER SYSTEM SET hot_standby_feedback = on;
SELECT pg_reload_conf();
```

You can verify whether the setting is enabled by running `SHOW hot_standby_feedback;`

Managed PostgreSQL instances from a cloud provider may require use of provider-specific
mechanisms to enable this setting and/or reload the modified configuration.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the PostgreSQL source connector.

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

#### SSL Mode

Certain managed PostgreSQL implementations may require you to explicitly set the [SSL Mode](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION) to connect with Flow. One example is [Neon](https://neon.tech/docs/connect/connect-securely), which requires the setting `verify-full`. Check your managed PostgreSQL's documentation for details if you encounter errors related to the SSL mode configuration.

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

[Learn more about capture definitions.](/concepts/captures.md)

## TOASTed values

PostgreSQL has a hard page size limit, usually 8 KB, for performance reasons.
If your tables contain values that exceed the limit, those values can't be stored directly.
PostgreSQL uses [TOAST](https://www.postgresql.org/docs/current/storage-toast.html) (The Oversized-Attribute Storage Technique) to
store them separately.

TOASTed values can sometimes present a challenge for systems that rely on the PostgreSQL write-ahead log (WAL), like this connector.
If a change event occurs on a row that contains a TOASTed value, _but the TOASTed value itself is unchanged_, it is omitted from the WAL.
As a result, the connector emits a row update with the value omitted, which might cause
unexpected results in downstream catalog tasks if adjustments are not made.

The PostgreSQL connector handles TOASTed values for you when you follow the [standard discovery workflow](/concepts/captures.md#discovery)
or use the [Flow UI](/concepts/web-app.md) to create your capture.
It uses [merge](/reference/reduction-strategies/merge.md) [reductions](/concepts/schemas.md#reductions)
to fill in the previous known TOASTed value in cases when that value is omitted from a row update.

However, due to the event-driven nature of certain tasks in Flow, it's still possible to see unexpected results in your data flow, specifically:

- When you materialize the captured data to another system using a connector that requires [delta updates](/concepts/materialization.md#delta-updates)
- When you perform a [derivation](/concepts/derivations.md) that uses TOASTed values

### Troubleshooting

If you encounter an issue that you suspect is due to TOASTed values, try the following:

- Ensure your collection's schema is using the merge [reduction strategy](/concepts/schemas.md#reduce-annotations).
- [Set REPLICA IDENTITY to FULL](https://www.postgresql.org/docs/9.4/sql-altertable.html) for the table. This circumvents the problem by forcing the
  WAL to record all values regardless of size. However, this can have performance impacts on your database and must be carefully evaluated.
- [Contact Estuary support](mailto:support@estuary.dev) for assistance.

## Publications

It is recommended that the publication used by the capture only contain the tables that will be captured. In some cases it may be desirable to create this publication for all tables in the database instead of specific tables, for example using:

```sql
CREATE PUBLICATION flow_publication FOR ALL TABLES WITH (publish_via_partition_root = true);
```

Caution must be used if creating the publication in this way as all existing tables (even those not part of the capture) will be included in it, and if any of them do not have a primary key they will no longer be able to process updates or deletes.
