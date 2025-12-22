# PostgreSQL Batch Query Connector

This connector captures data from PostgreSQL databases into Estuary collections by
periodically executing queries and translating the results into JSON documents.

## When to use this connector

We recommend using our [PostgreSQL CDC Connector](http://go.estuary.dev/source-postgres)
instead when possible. CDC provides lower latency data capture, delete and
update events, and typically has a smaller impact on the source database.

However, the batch connector is the right choice when:

- Your PostgreSQL instance doesn't support logical replication
- You need to capture from a read replica on PostgreSQL \<=15
- You need to capture from database views
- You want to execute ad-hoc or custom queries

## Supported versions and platforms

This connector works with all supported PostgreSQL versions on major cloud
platforms (including Amazon RDS and Aurora, Google Cloud SQL, Azure Database
for PostgreSQL, and other managed services), as well as self-hosted instances.

:::tip Configuration Tip
To capture data from databases hosted on your internal network, you may need to
use [SSH tunneling](/guides/connect-network/). If you have a
[private deployment](/getting-started/deployment-options/#private-deployment),
you can also use private cloud networking features to reach your database.
:::

## Prerequisites

You'll need:

- A PostgreSQL database with a user that has `SELECT` permission on the tables
  you want to capture
- Network access to the database (direct or via SSH tunnel)

## Setup

### Creating a capture user

We recommend creating a dedicated user for Estuary captures:

```sql
CREATE USER flow_capture WITH PASSWORD 'secret';
```

Grant read permissions on the tables you want to capture:

```sql
-- For PostgreSQL 14 and later:
GRANT pg_read_all_data TO flow_capture;

-- For earlier versions:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to flow_capture;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
```

Replace `public` with the name of your schema, or grant permissions on multiple
schemas as needed.

## How it works

### Capture Modes

Each binding operates in one of two modes, automatically selected based on
the resource configuration:

#### XMIN Mode

When you configure `cursor: ["txid"]` for a table binding, the connector uses
PostgreSQL's `xmin` system column to identify new and updated rows. This mode
provides efficient incremental updates for regular tables.

The first poll performs a full backfill of the table, then subsequent polls
capture only rows with newer transaction IDs.

This is the recommended mode for capturing ordinary tables, and will be used
by default in discovered bindings.

#### Query Mode

For all other configurations, the connector uses query mode with a built-in
template. This supports three patterns:

- **Full-refresh** (no cursor): The entire table/view is re-read on each poll.
- **Cursor-incremental**: Captures rows where the cursor has advanced since we
  last polled, according to `WHERE cursor > $lastValue ORDER BY cursor`.
- **Custom query**: Override the built-in template to execute arbitrary SQL.
  This may be useful for filtering, aggregations, or subsets of your data.

:::warning Data Volume Consideration
Full-refresh bindings re-capture all data on each poll, which can generate
significant data volumes. Just a few megabytes polled every 5 minutes adds
up to gigabytes per day. Use cursors or longer polling intervals to manage
the data volume when capturing views.
:::

### Polling schedule

The connector executes queries on a configurable schedule, which may be set at
the capture level and/or overridden on a per-binding basis. When unset the
schedule defaults to polling every 5 minutes.

Polling intervals are written as strings in one of two formats:

- **Interval format**: `5m` (5 minutes), `1h` (1 hour), `24h` (24 hours), etc
- **Time-of-day format**: `daily at 12:34Z` (daily at 12:34 UTC)
  - Time-of-day polling schedules must specify the time in UTC with the 'Z'
    suffix, other timezones or offsets are not currently supported.

### Collection keys

Discovered tables with primary keys will use them as their collection keys.
Tables without a primary key use `/_meta/row_id` as the collection key.

## Configuration

Configure this connector in the Estuary web app or using YAML config files with
[flowctl CLI](/guides/flowctl/). See [connectors](/concepts/connectors/#using-connectors)
to learn more about using connectors.

### Endpoint Properties

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/address`** | Address | The host or host:port at which the database can be reached. | string | Required |
| **`/user`** | User | The database user to authenticate as. | string | Required, `"flow_capture"` |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| **`/database`** | Database | Logical database name to capture from. | string | Required, `"postgres"` |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object | |
| `/advanced/poll` | Default Polling Schedule | When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling, or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | `"5m"` |
| `/advanced/discover_views` | Discover Views | When set, views will be automatically discovered as resources. If unset, only tables will be discovered. | boolean | `false` |
| `/advanced/discover_schemas` | Discovery Schema Selection | If this is specified, only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas. | array | `[]` |
| `/advanced/sslmode` | SSL Mode | Overrides SSL connection behavior by setting the 'sslmode' parameter. | string | |
| `/advanced/source_tag` | Source Tag | When set, the capture will add this value as the property 'tag' in the source metadata of each document. | string | |
| `/advanced/statement_timeout` | Statement Timeout | Overrides the default statement timeout for queries. | string | |
| `/networkTunnel` | Network Tunnel | Connect to your system through an SSH server that acts as a bastion host for your network. | object | |

### Binding Properties

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/name`** | Resource Name | The unique name of this resource. | string | Required |
| `/schema` | Schema Name | The name of the schema in which the captured table lives. Must be set unless using a custom template. | string | |
| `/table` | Table Name | The name of the table to be captured. Must be set unless using a custom template. | string | |
| `/cursor` | Cursor Columns | The names of columns which should be persisted between query executions as a cursor. | array | `["txid"]` for tables, `[]` for views |
| `/poll` | Polling Schedule | When and how often to execute the fetch query (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling, or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | |
| `/template` | Query Template Override | Optionally overrides the query template which will be rendered and then executed. | string | |

## Type mapping

PostgreSQL types are mapped to JSON types as follows:

| PostgreSQL Type | JSON Type | Notes |
|----------------|-----------|-------|
| `BOOLEAN` | `boolean` | |
| `SMALLINT`, `INTEGER`, `BIGINT` | `integer` | |
| `NUMERIC`, `DECIMAL` | `string` | Formatted as number string to preserve precision |
| `REAL`, `DOUBLE PRECISION` | `number` or `string` | NaN, Infinity, -Infinity encoded as strings |
| `VARCHAR`, `TEXT`, `CHAR` | `string` | |
| `BYTEA` | `string` | Base64 encoded |
| `JSON`, `JSONB` | Native JSON | Passed through without modification |
| `UUID` | `string` | Format: `uuid` |
| `DATE`, `TIMESTAMP`, `TIMESTAMPTZ` | `string` | Format: `date-time` (RFC3339) |
| `TIME`, `TIMETZ` | `string` | |
| `INTERVAL` | `string` | |
| `ARRAY` types | `array` | |
| `ENUM` types | `string` | |
| Geometric types | `string` | `POINT`, `LINE`, `BOX`, etc. |
| Network types | `string` | `INET`, `CIDR`, `MACADDR` |
| Range types | `string` | |

## Query templates

Query templates use Go's template syntax to generate SQL queries. The connector
uses a default template which implements appropriate behavior for full-refresh
and cursor-incremental bindings, but you can override this for custom behavior.

Overriding the query template is best left to power-users or done at the direction
of Estuary support. Consult the [connector source](https://github.com/estuary/connectors/blob/main/source-postgres-batch/main.go)
for the current text of the default template.
