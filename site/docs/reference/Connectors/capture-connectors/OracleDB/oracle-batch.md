# Oracle Batch Query Connector

This connector captures data from Oracle databases into Flow collections by
periodically executing queries and translating the results into JSON documents.

## When to use this connector

We recommend using our [Oracle CDC Connector](http://go.estuary.dev/source-oracle)
instead when possible. CDC provides lower latency data capture, delete and
update events, and typically has a smaller impact on the source database.

However, the batch connector is the right choice when:

- Your Oracle instance doesn't support LogMiner (e.g., some managed services)
- You need to capture from database views
- You want to execute ad-hoc or custom queries
- You need to capture from a read replica that doesn't have LogMiner enabled

## Supported versions and platforms

This connector works with Oracle Database 11g and later on major cloud platforms
(including Amazon RDS, Oracle Cloud Infrastructure, and other managed services),
as well as self-hosted instances.

:::tip Configuration Tip
To capture data from databases hosted on your internal network, you must
use [SSH tunneling](/guides/connect-network/).
:::

## Prerequisites

You'll need:

- An Oracle database with a user that has `SELECT` permission on the tables
  you want to capture
- Network access to the database (direct or via SSH tunnel)

## Setup

### Creating a capture user

We recommend creating a dedicated user for Flow captures:

```sql
CREATE USER flow_capture IDENTIFIED BY secret;
```

Grant read permissions on the tables you want to capture:

```sql
-- Grant SELECT on a specific user's tables
GRANT SELECT ANY TABLE TO flow_capture;

-- Or grant SELECT on specific tables
GRANT SELECT ON schema_name.table_name TO flow_capture;
```

Replace `schema_name` and `table_name` with the names of the owners and tables
you want to capture, or grant permissions on multiple tables as needed.

:::tip Oracle Terminology
In Oracle, the "owner" of a table is effectively the schema. Every user account
is a schema, and tables are owned by user accounts. This is why you'll see
references to "owner" rather than "schema" throughout Oracle documentation.
:::

## Usage

### Query behaviors

The connector executes queries periodically to capture data. It defaults to a
built-in query template which supports four patterns of use:

- **ROWSCN-incremental**: For discovered tables, the connector automatically
  uses Oracle's `ORA_ROWSCN` system column to capture rows that have been
  modified since the last poll. The first poll performs a full backfill, then
  subsequent polls capture only new or updated rows.
- **Cursor-incremental**: Captures rows where the cursor has advanced since we
  last polled, according to `WHERE cursor > $lastValue ORDER BY cursor`.
- **Full-refresh** (no cursor): The entire table/view is re-read on each poll.
- **Custom query**: Override the built-in template to execute arbitrary SQL.
  This may be useful for filtering, aggregations, or subsets of your data.

The `ORA_ROWSCN` incremental capture mode is activated by setting the cursor for
a binding to `["TXID"]`. Other common cursor choices for manual configuration:

- **Update timestamps**: Best when available, as they capture both new rows and updates
- **Creation timestamps**: Work for append-only tables but won't detect updates
- **Auto-incrementing IDs**: Work for append-only tables but won't detect updates

When no cursor is configured, the entire table or view is re-read on each poll.

:::warning Data Volume Consideration
Full-refresh bindings re-capture all data on each poll, which can generate
significant data volumes. A few megabytes polled every 5 minutes adds up to
gigabytes per day. Use cursors or longer polling intervals to manage data
volume when capturing views or tables without suitable cursors.
:::

### Polling schedule

The connector executes queries on a configurable schedule, which may be set at
the capture level and/or overridden on a per-binding basis. When unset, the
schedule defaults to polling every 5 minutes.

Polling intervals are written as strings in one of two formats:

- **Interval format**: `5m` (5 minutes), `1h` (1 hour), `24h` (24 hours), etc
- **Time-of-day format**: `daily at 12:34Z` (daily at 12:34 UTC)
  - Time-of-day polling schedules must specify the time in UTC with the 'Z'
    suffix. Other timezones or offsets are not currently supported.

### Collection keys

Discovered tables with primary keys will use them as their collection keys.
Tables without a primary key use `["/_meta/polled", "/_meta/index"]` as the
collection key, which is based on the polling timestamp and result order.

## Configuration

Configure this connector in the Flow web app or using YAML config files with
[flowctl CLI](/guides/flowctl/). See [connectors](/concepts/connectors/#using-connectors)
to learn more about using connectors.

### Endpoint Properties

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/address`** | Server Address | The host or host:port at which the database can be reached. | string | Required |
| **`/user`** | User | The database user to authenticate as. | string | Required, `"flow_capture"` |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| `/database` | Database | Logical database name to capture from. | string | `"ORCL"` |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object | |
| `/advanced/poll` | Default Polling Schedule | When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling, or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | `"5m"` |
| `/advanced/discover_schemas` | Discovery Schema Selection | If this is specified, only tables in the selected schema(s) (owner accounts) will be automatically discovered. Omit all entries to discover tables from all schemas. | array | `[]` |
| `/advanced/sslmode` | SSL Mode | Overrides SSL connection behavior by setting the 'sslmode' parameter. | string | |
| `/advanced/source_tag` | Source Tag | When set, the capture will add this value as the property 'tag' in the source metadata of each document. | string | |
| `/networkTunnel` | Network Tunnel | Connect to your system through an SSH server that acts as a bastion host for your network. | object | |

### Binding Properties

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/name`** | Resource Name | The unique name of this resource. | string | Required |
| `/owner` | Owner | The name of the owner to which the captured table belongs. Must be set unless using a custom template. | string | |
| `/table` | Table Name | The name of the table to be captured. Must be set unless using a custom template. | string | |
| `/cursor` | Cursor Columns | The names of columns which should be persisted between query executions as a cursor. | array | `["TXID"]` for tables, `[]` for views |
| `/poll` | Polling Schedule | When and how often to execute the fetch query (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling, or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | |
| `/template` | Query Template Override | Optionally overrides the query template which will be rendered and then executed. | string | |

## Type mapping

Oracle types are mapped to JSON types as follows:

| Oracle Type | JSON Type | Notes |
|-------------|-----------|-------|
| `NUMBER` (no precision/scale) | `string` | Format: `number` to preserve full precision |
| `NUMBER` (scale=0) | `string` | Format: `integer` for whole numbers |
| `NUMBER`, `FLOAT` | `string` | Format: `number` to preserve precision |
| `CHAR`, `VARCHAR`, `VARCHAR2`, `NCHAR`, `NVARCHAR2` | `string` | |
| `CLOB`, `RAW` | `string` | |
| `DATE`, `TIMESTAMP` | `string` | |
| `TIMESTAMP WITH TIME ZONE` | `string` | Format: `date-time` (RFC3339) |
| `INTERVAL` types | `string` | |

## Query templates

Query templates use Go's template syntax to generate SQL queries. The connector
uses a default template which implements appropriate behavior for ORA_ROWSCN-incremental,
cursor-incremental, and full-refresh bindings, but you can override this for custom behavior.

Overriding the query template is best left to power-users or done at the direction
of Estuary support. Consult the [connector source](https://github.com/estuary/connectors/blob/main/source-oracle-batch/main.go)
for the current text of the default template.
