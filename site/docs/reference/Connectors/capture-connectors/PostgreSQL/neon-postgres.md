# Neon PostgreSQL

Neon's logical replication feature allows you to replicate data from your Neon Postgres database to external destinations.

## Prerequisites

- An [Estuary Flow account](https://dashboard.estuary.dev/register) (start free, no credit card required)
- A [Neon account](https://console.neon.tech/)

## Setup

### 1. Enable Logical Replication in Neon

Enabling logical replication modifies the Postgres `wal_level` configuration parameter, changing it from `replica` to `logical` for all databases in your Neon project. Once the `wal_level` setting is changed to `logical`, it cannot be reverted. Enabling logical replication also restarts all computes in your Neon project, meaning active connections will be dropped and have to reconnect.

To enable logical replication in Neon:

1. Select your project in the Neon Console.
2. On the Neon **Dashboard**, select **Project settings**.
3. Select **Beta**.
4. Click **Enable** to enable logical replication.

You can verify that logical replication is enabled by running the following query from the [Neon SQL Editor](https://neon.tech/docs/get-started-with-neon/query-with-neon-sql-editor):

```sql
SHOW wal_level;
 wal_level
-----------
 logical
```

### 2. Create a Postgres Role for Replication

It is recommended that you create a dedicated Postgres role for replicating data. The role must have the `REPLICATION` privilege.
The default Postgres role created with your Neon project and roles created using the Neon Console, CLI, or API are granted membership in the neon_superuser role, which has the required `REPLICATION` privilege.

To create a role in the Neon Console:

1. Navigate to the [Neon Console](https://console.neon.tech).
2. Select a project.
3. Select **Roles**.
4. Select the branch where you want to create the role.
5. Click **New Role**.
6. In the role creation dialog, specify a role name.
7. Click **Create**. The role is created and you are provided with the password for the role.

Alternatively, the following CLI command creates a role. To view the CLI documentation for this command, see [Neon CLI commands — roles](https://api-docs.neon.tech/reference/createprojectbranchrole).

```bash
neon roles create --name <role>
```

As a third option, the following Neon API method also creates a role. To view the API documentation for this method, refer to the Neon API reference.

```bash
curl 'https://console.neon.tech/api/v2/projects/hidden-cell-763301/branches/br-blue-tooth-671580/roles' \
  -H 'Accept: application/json' \
  -H "Authorization: Bearer $NEON_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
  "role": {
    "name": "cdc_role"
  }
}' | jq
```

### 3. Grant Schema Access to Your Postgres Role

If your replication role does not own the schemas and tables you are replicating from, make sure to grant access. Run this commands for each schema:

```sql
GRANT pg_read_all_data TO cdc_role;
```

### 4. Create the watermarks table, grant privileges, and create publication:

```sql
CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks, <other_tables>;
```

The `publish_via_partition_root`
  setting is recommended (because most users will want changes to a partitioned table to be captured
  under the name of the root table) but is not required.

Refer to the [Postgres docs](https://www.postgresql.org/docs/current/sql-alterpublication.html) if you need to add or remove tables from your publication. Alternatively, you also can create a publication `FOR ALL TABLES`.

Upon start-up, the Estuary Flow connector for Postgres will automatically create the [replication slot](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS) required for ingesting data change events from Postgres. The slot's name will be prefixed with `estuary_`, followed by a unique identifier.

To prevent storage bloat, **Neon automatically removes _inactive_ replication slots after a period of time if there are other _active_ replication slots**. If you have or intend on having more than one replication slot, please see [Unused replication slots](https://docs.neon.tech/docs/logical-replication-neon#unused-replication-slots) to learn more.

## Allow Inbound Traffic

If you are using Neon's **IP Allow** feature to limit the IP addresses that can connect to Neon, you will need to allow inbound traffic from Estuary Flow's IP addresses.
Refer to the [Estuary Flow documentation](/reference/allow-ip-addresses) for the list of IPs that need to be allowlisted for the Estuary Flow region of your account.
For information about configuring allowed IPs in Neon, see [Configure IP Allow](https://neon.tech/docs/introduction/ip-allow).

## Create a Postgres Source Connector in Estuary Flow

1. In the Estuary Flow web UI, select **Sources** from the left navigation bar and click **New Capture**.
2. In the connector catalog, choose **Neon PostgreSQL** and click **Connect**.
3. Enter the connection details for your Neon database. You can get these details from your Neon connection string, which you'll find in the **Connection Details** widget on the **Dashboard** of your Neon project. Your connection string will look like this:

   ```bash
   postgres://cdc_role:AbC123dEf@ep-cool-darkness-123456.us-east-2.aws.neon.tech/dbname?sslmode=require
   ```
   Enter the details for **your connection string** into the source connector fields. Based on the sample connection string above, the values would be specified as shown below. Your values will differ.

   - Name: Name of the Capture connector
   - Server Address: ep-cool-darkness-123456.us-east-2.aws.neon.tech:5432
   - User: cdc_role
   - Password: `AbC123dEf` in the example, or your own value based on the connection string.
   - Database: dbname

3. Click **Next**. Estuary Flow will now scan the source database for all the tables that can be replicated. Select one or more table(s) by checking the checkbox next to their name.
Optionally, you can change the name of the destination name for each table. You can also take a look at the schema of each stream by clicking on the **Collection** tab.

4. Click **Save and Publish** to provision the connector and kick off the automated backfill process.

## Backfills and performance considerations

When the PostgreSQL capture is initiated, by default, the connector first *backfills*, or captures the targeted tables in their current state. It then transitions to capturing change events on an ongoing basis.

This is desirable in most cases, as it ensures that a complete view of your tables is captured into Flow.
However, you may find it appropriate to skip the backfill, especially for extremely large tables.

In this case, you may turn off backfilling on a per-table basis. See [properties](#properties) for details.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the PostgreSQL source connector.

### Properties

#### Endpoint

| Property                        | Title               | Description                                                                                                                                 | Type    | Required/Default           |
|---------------------------------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------|---------|----------------------------|
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
| `/advanced/sslmode`     | SSL Mode    | Overrides SSL connection behavior by setting the 'sslmode' parameter. | string  |  |

#### Bindings

| Property         | Title     | Description                                                                                | Type   | Required/Default |
|------------------|-----------|--------------------------------------------------------------------------------------------|--------|------------------|
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
