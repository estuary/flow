---
sidebar_position: 6
---
# Supabase

This connector uses change data capture (CDC) to continuously capture updates in a Supabase PostgreSQL database into one or more Estuary collections.

## Supported versions and platforms

This connector supports all Supabase PostgreSQL instances.

## Prerequisites

You'll need a Supabase PostgreSQL database setup with the following:
* A Supabase IPv4 address and direct connection hostname which bypasses the Supabase connection pooler.
  See [Direct Database Connection](#direct-database-connection) for details.
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

:::tip Configuration Tip
To capture data from databases hosted on your internal network, you may need to
use [SSH tunneling](/guides/connect-network/). If you have a
[private deployment](/getting-started/deployment-options/#private-deployment),
you can also use private cloud networking features to reach your database.
:::

### Direct Database Connection

By default, Supabase guides users into connecting to their database through a
[Connection Pooler](https://supabase.com/docs/guides/database/connecting-to-postgres#connection-pooler).
Connection poolers are helpful for many applications, but unfortunately the pooler
does not support the CDC replication features that this connector relies on.

This capture connector requires a direct connection address for your database.
This address can be found by navigating to `Settings > Database` in the Supabase
dashboard and then making sure that the `Display connection pooler` checkbox is
**unchecked** so that the appropriate connection information is shown for a direct
connection.

You will also need to configure a [dedicated IPv4 address](https://supabase.com/docs/guides/platform/ipv4-address)
for your database, if you have not already done so. This can be configured under `Project Settings > Add Ons > Dedicated IPv4 address`
in the Supabase dashboard.

## Setup


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


## Backfills and performance considerations

When the PostgreSQL capture is initiated, by default, the connector first *backfills*, or captures the targeted tables in their current state. It then transitions to capturing change events on an ongoing basis.

This is desirable in most cases, as it ensures that a complete view of your tables is captured into Estuary.
However, you may find it appropriate to skip the backfill, especially for extremely large tables.

In this case, you may turn off backfilling on a per-table basis. See [properties](#properties) for details.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the PostgreSQL source connector.


### Properties

#### Endpoint

| Property                        | Title               | Description                                                                                                                                 | Type    | Required/Default           |
|---------------------------------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------|---------|----------------------------|
| **`/address`**                  | Address             | The host or host:port at which the database can be reached.                                                                                 | string  | Required                   |
| **`/database`**                 | Database            | Logical database name to capture from.                                                                                                      | string  | Required, `"postgres"`     |
| **`/user`**                     | User                | The database user to authenticate as.                                                                                                       | string  | Required, `"flow_capture"` |
| `/historyMode` | History Mode | Capture each change event, without merging. | boolean | `false` |

##### Authentication

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/credentials`** | Authentication | Authentication method and credentials that provide access to the database. | object | Required |
| `/credentials/auth_type` | Auth Type | The authentication method to use. One of `UserPassword`, `AWSIAM`, `GCPIAM`, or `AzureIAM`. | string |  |
| `/credentials/password` | Password | Password for the specified database user. | string | Required for `UserPassword` auth |
| `/credentials/aws_region` | AWS Region | AWS region of your resource. | string | Required for `AWSIAM` auth |
| `/credentials/aws_role_arn` | AWS Role ARN | AWS role for Estuary to use that has access to the resource. | string | Required for `AWSIAM` auth |
| `/credentials/gcp_service_account_to_impersonate` | GCP Service Account | GCP service account email for Cloud SQL IAM authentication. | string | Required for `GCPIAM` auth |
| `/credentials/gcp_workload_identity_pool_audience` | Workload Identity Pool Audience | GCP workload identity pool audience. The format should be similar to: `//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider`. | string | Required for `GCPIAM` auth |
| `/credentials/azure_client_id` | Azure Client ID | Azure App Registration Client ID for Azure Active Directory authentication. | string | Required for `AzureIAM` auth |
| `/credentials/azure_tenant_id` | Azure Tenant ID | Azure Tenant ID for Azure Active Directory authentication. | string | Required for `AzureIAM` auth |

##### Advanced options

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/advanced`                     | Advanced Options    | Options for advanced users. You should not typically need to modify these.                                                                  | object  |                            |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query.                                                    | integer | `4096`                     |
| `/advanced/publicationName`     | Publication Name    | The name of the PostgreSQL publication to replicate from.                                                                                   | string  | `"flow_publication"`       |
| `/advanced/skip_backfills`      | Skip Backfills      | A comma-separated list of fully-qualified table names which should not be backfilled.                                                       | string  |                            |
| `/advanced/slotName`            | Slot Name           | The name of the PostgreSQL replication slot to replicate from.                                                                              | string  | `"flow_slot"`              |
| `/advanced/watermarksTable`     | Watermarks Table    | The name of the table used for watermark writes during backfills. Must be fully-qualified in &#x27;&lt;schema&gt;.&lt;table&gt;&#x27; form. | string  | `"public.flow_watermarks"` |
| `/advanced/sslmode`             | SSL Mode            | Overrides SSL connection behavior by setting the 'sslmode' parameter.                                                                       | string  |                            |
| `/advanced/discover_schemas` | Discovery Schema Selection | If this is specified, only tables in the selected schema(s) will be automatically discovered. | string array |  |
| `advanced/min_backfill_xid` | Minimum Backfill XID | Only backfill rows with XMIN values greater (in a 32-bit modular comparison) than the specified XID. Helpful for reducing re-backfill data volume in certain edge cases. | string |  |
| `/advanced_read_only_capture` | Read-Only Capture | When set, the capture will operate in read-only mode and avoid operations such as watermark writes. | boolean | `false` |
| `/advanced/capture_as_partitions` | Capture Partitioned Tables As Partitions | When set, the capture will discover and capture partitioned tables as individual partitions rather than as a single root table. This requires the publication to be created without `publish_via_partition_root`. | boolean | `false` |
| `/advanced/discover_unpublished_tables` | Discover Unpublished Tables | If `true`, the capture discovers all tables, even ones not currently in the publication. | boolean |  |
| `/advanced/source_tag` | Source Tag | This value is added as the property 'tag' in the source metadata of each document. | string |  |
| `/advanced/statement_timeout` | Statement Timeout | Overrides the default statement timeout used by the connector. The default of zero disables statement timeouts entirely. Options include `""`, `30s`, `1m`, `5m`, and `30m`. | string | `""` |

#### Bindings

| Property         | Title     | Description                                                                                | Type   | Required/Default |
|------------------|-----------|--------------------------------------------------------------------------------------------|--------|------------------|
| **`/namespace`** | Namespace | The [namespace/schema](https://www.postgresql.org/docs/9.1/ddl-schemas.html) of the table. | string | Required         |
| **`/stream`**    | Stream    | Table name.     | string | Required         |
| `/mode` | [Backfill Mode](/reference/backfilling-data/#resource-configuration-backfill-modes) | How the preexisting contents of the table should be backfilled. This should generally not be changed. | string | `""` |
| `/priority` | Backfill Priority | Optional priority for this binding. The highest priority binding(s) will be backfilled completely before any others. Negative priorities are allowed and will cause a binding to be backfilled after others. | integer | `0` |

#### SSL Mode

Certain managed PostgreSQL implementations may require you to explicitly set the [SSL Mode](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION) to connect with Estuary. One example is [Neon](https://neon.tech/docs/connect/connect-securely), which requires the setting `verify-full`. Check your managed PostgreSQL's documentation for details if you encounter errors related to the SSL mode configuration.

### Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:v3
        config:
          address: host:port
          database: postgres
          user: flow_capture
          credentials:
            auth_type: UserPassword
            password: <secret>
    bindings:
      - resource:
          stream: ${TABLE_NAME}
          namespace: ${TABLE_NAMESPACE}
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
or use the [Estuary UI](/concepts/web-app.md) to create your capture.
It uses [merge](/reference/reduction-strategies/merge) [reductions](/concepts/schemas.md#reductions)
to fill in the previous known TOASTed value in cases when that value is omitted from a row update.

However, due to the event-driven nature of certain tasks in Estuary, it's still possible to see unexpected results in your data flow, specifically:

- When you materialize the captured data to another system using a connector that requires [delta updates](/concepts/materialization/#delta-updates)
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
