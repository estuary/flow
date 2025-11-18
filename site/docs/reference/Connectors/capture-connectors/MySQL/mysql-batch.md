# MySQL Batch Query Connector

This connector captures data from MySQL databases into Flow collections by
periodically executing queries and translating the results into JSON documents.

## When to use this connector

We recommend using our [MySQL CDC Connector](http://go.estuary.dev/source-mysql)
instead when possible. CDC provides lower latency data capture, delete and
update events, and typically has a smaller impact on the source database.

However, the batch connector is the right choice when:

- Your MySQL instance doesn't support binary log replication (e.g., some managed services)
- You need to capture from database views
- You want to execute ad-hoc or custom queries
- You need to capture from a read replica that doesn't have binary logging enabled

## Supported versions and platforms

This connector works with all supported MySQL versions on major cloud
platforms (including Amazon RDS and Aurora, Google Cloud SQL, Azure Database
for MySQL, and other managed services), as well as self-hosted instances.

:::tip Configuration Tip
To capture data from databases hosted on your internal network, you may need to
use [SSH tunneling](/guides/connect-network/). If you have a
[private deployment](/getting-started/deployment-options/#private-deployment),
you can also use private cloud networking features to reach your database.
:::

## Prerequisites

You'll need:

- A MySQL database with a user that has `SELECT` permission on the tables
  you want to capture
- Network access to the database (direct or via SSH tunnel)

## Setup

### Creating a capture user

We recommend creating a dedicated user for Flow captures:

```sql
CREATE USER flow_capture IDENTIFIED BY 'secret';
```

Grant read permissions on the tables you want to capture:

```sql
-- Grant SELECT on a specific database
GRANT SELECT ON my_database.* TO flow_capture;

-- Or grant SELECT on all databases
GRANT SELECT ON *.* TO flow_capture;
```

Replace `my_database` with the name of your database, or grant permissions on
multiple databases as needed.

## Usage

### Query behaviors

The connector executes queries periodically to capture data. It defaults to a
built-in query template which supports three patterns of use:

- **Full-refresh** (no cursor): The entire table/view is re-read on each poll.
- **Cursor-incremental**: Captures rows where the cursor has advanced since we
  last polled, according to `WHERE cursor > $lastValue ORDER BY cursor`.
- **Custom query**: Override the built-in template to execute arbitrary SQL.
  This may be useful for filtering, aggregations, or subsets of your data.

Common cursor choices:

- **Update timestamps**: Best when available, as they capture both new rows and updates
- **Creation timestamps**: Work for append-only tables but won't detect updates
- **Auto-incrementing IDs**: Work for append-only tables but won't detect updates

When no cursor is configured, the entire table or view is re-read on each poll.

:::warning Data Volume Consideration
Full-refresh bindings re-capture all data on each poll, which can generate
significant data volumes. A few megabytes polled every hour adds up to
gigabytes per day. Use cursors or longer polling intervals to manage data
volume when capturing views or tables without suitable cursors.
:::

### Polling schedule

The connector executes queries on a configurable schedule, which may be set at
the capture level and/or overridden on a per-binding basis. When unset, the
schedule defaults to polling every 24 hours.

Polling intervals are written as strings in one of two formats:

- **Interval format**: `5m` (5 minutes), `1h` (1 hour), `24h` (24 hours), etc
- **Time-of-day format**: `daily at 12:34Z` (daily at 12:34 UTC)
  - Time-of-day polling schedules must specify the time in UTC with the 'Z'
    suffix. Other timezones or offsets are not currently supported.

### Collection keys

Discovered tables with primary keys will use them as their collection keys.
Tables without a primary key use `/_meta/row_id` as the collection key.

When a full-refresh binding outputs to a collection keyed by `/_meta/row_id`,
the connector can infer deletions: if a refresh yields fewer rows than the
previous poll, deletion documents are emitted for the missing row IDs.

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
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object | |
| `/advanced/poll` | Default Polling Schedule | When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling, or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | `"24h"` |
| `/advanced/discover_views` | Discover Views | When set, views will be automatically discovered as resources. If unset, only tables will be discovered. | boolean | `false` |
| `/advanced/discover_schemas` | Discovery Schema Selection | If this is specified, only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas. | array | `[]` |
| `/advanced/dbname` | Database Name | The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access. | string | |
| `/advanced/source_tag` | Source Tag | When set, the capture will add this value as the property 'tag' in the source metadata of each document. | string | |
| `/networkTunnel` | Network Tunnel | Connect to your system through an SSH server that acts as a bastion host for your network. | object | |

### Binding Properties

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/name`** | Resource Name | The unique name of this resource. | string | Required |
| `/schema` | Schema Name | The name of the schema in which the captured table lives. Must be set unless using a custom template. | string | |
| `/table` | Table Name | The name of the table to be captured. Must be set unless using a custom template. | string | |
| `/cursor` | Cursor Columns | The names of columns which should be persisted between query executions as a cursor. | array | `[]` (full-refresh) |
| `/poll` | Polling Schedule | When and how often to execute the fetch query (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling, or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | |
| `/template` | Query Template Override | Optionally overrides the query template which will be rendered and then executed. | string | |

## Type mapping

MySQL types are mapped to JSON types as follows:

| MySQL Type | JSON Type | Notes |
|------------|-----------|-------|
| `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`, `BIT` | `integer` | |
| `FLOAT`, `DOUBLE` | `number` | |
| `DECIMAL`, `NUMERIC` | `string` | Formatted as number string with format `number` to preserve precision |
| `CHAR`, `VARCHAR`, `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | `string` | |
| `BINARY`, `VARBINARY`, `TINYBLOB`, `BLOB`, `MEDIUMBLOB`, `LONGBLOB` | `string` | Bytes encoded as base64 string |
| `ENUM`, `SET` | `string` | |
| `DATE` | `string` | |
| `DATETIME`, `TIMESTAMP` | `string` | |
| `TIME` | `string` | |
| `YEAR` | `integer` | |
| `JSON` | Native JSON | Passed through without modification |

## Query templates

Query templates use Go's template syntax to generate SQL queries. The connector
uses a default template which implements appropriate behavior for full-refresh
and cursor-incremental bindings, but you can override this for custom behavior.

Overriding the query template is best left to power-users or done at the direction
of Estuary support. Consult the [connector source](https://github.com/estuary/connectors/blob/main/source-mysql-batch/main.go)
for the current text of the default template.
