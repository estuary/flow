# IBM Db2 Batch Query Connector

This connector captures data from IBM Db2 databases into Estuary collections by
periodically executing queries and translating the results into JSON documents.

## Supported Versions and Platforms

This connector is developed and tested against IBM Db2 for Linux, UNIX, and
Windows (LUW).

It may also work with other Db2 platforms such as Db2 for i (iSeries/AS400)
and Db2 for z/OS, but these are not officially tested. Please [contact us](mailto:support@estuary.dev)
if you'd like to capture data from these platforms.

:::tip Configuration Tip
To capture data from databases hosted on your internal network, you may need to
use [SSH tunneling](/guides/connect-network/). If you have a
[private deployment](/getting-started/deployment-options/#private-deployment),
you can also use private cloud networking features to reach your database.
:::

## Prerequisites

- An IBM Db2 database with a user that has `SELECT` permission on the tables you want to capture
- Network access to the database (direct or via SSH tunnel)

## Setup

### User Creation

IBM Db2 typically uses OS-level or LDAP user management. The database user must
first exist as an operating system user (or LDAP user) on the database server.
Consult your Db2 and system administrator documentation for creating users on
your platform.

Once the OS/LDAP user exists, grant it the necessary database privileges:

```sql
-- Grant the user permission to connect to the database
GRANT CONNECT ON DATABASE TO USER flow_capture;

-- Grant SELECT on all tables (existing and future) in a schema
GRANT SELECTIN ON SCHEMA myschema TO USER flow_capture;

-- Or grant SELECT on individual tables
GRANT SELECT ON myschema.mytable TO USER flow_capture;
```

The `SELECTIN` privilege grants read access to all existing and future tables
and views in the specified schema. If you prefer more granular control, grant
`SELECT` on individual tables instead.

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

### Polling Schedule

The connector executes queries on a configurable schedule, which may be set at
the capture level and/or overridden on a per-binding basis. When unset, the
schedule defaults to polling every 24 hours.

Polling intervals are written as strings in one of two formats:

- **Interval format**: `5m` (5 minutes), `1h` (1 hour), `24h` (24 hours), etc
- **Time-of-day format**: `daily at 12:34Z` (daily at 12:34 UTC)
  - Time-of-day polling schedules must specify the time in UTC with the 'Z'
    suffix. Other timezones or offsets are not currently supported.

### Timezone Handling

IBM Db2 `TIMESTAMP` values don't have an associated time zone or offset. The
connector converts these to RFC3339 timestamps by interpreting them in the time
zone configured in `/advanced/timezone` (defaults to UTC).

### Collection keys

Discovered tables with primary keys will use them as their collection keys.
Tables without a primary key use `/_meta/row_id` as the collection key.

When a full-refresh binding outputs to a collection keyed by `/_meta/row_id`,
the connector can infer deletions: if a refresh yields fewer rows than the
previous poll, deletion documents are emitted for the missing row IDs.

## Configuration

Configure this connector in the Estuary web app or using YAML config files with
[flowctl CLI](/guides/flowctl/). See [connectors](/concepts/connectors/#using-connectors)
to learn more about using connectors.

### Endpoint Properties

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/address`** | Server Address | The host or host:port at which the database can be reached. | string | Required |
| **`/user`** | User | The database user to authenticate as. | string | `"flow_capture"` |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| **`/database`** | Database | Logical database name to capture from. | string | Required |
| `/advanced/timezone` | Time Zone | The IANA timezone name in which datetime columns will be converted to RFC3339 timestamps. | string | `"UTC"` |
| `/advanced/poll` | Default Polling Schedule | When and how often to execute fetch queries. | string | `"24h"` |
| `/advanced/discover_views` | Discover Views | When set, views will be automatically discovered as resources. | boolean | `false` |
| `/advanced/discover_schemas` | Discovery Schema Selection | If specified, only tables in the selected schema(s) will be discovered. | array | |
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

## Type Mapping

IBM Db2 types are mapped to JSON types as follows:

| Db2 Type | JSON Type | Notes |
|----------|-----------|-------|
| `BOOLEAN` | boolean | |
| `SMALLINT`, `INTEGER`, `BIGINT` | integer | |
| `REAL`, `DOUBLE` | number | |
| `DECIMAL`, `DECFLOAT` | string | Format: `number`. String representation preserves precision. |
| `CHAR`, `VARCHAR`, `CLOB` | string | |
| `GRAPHIC`, `VARGRAPHIC`, `DBCLOB` | string | Unicode/DBCS character types. |
| `BINARY`, `VARBINARY`, `BLOB` | string | Content encoding: `base64`. |
| `DATE` | string | Format: `date` (YYYY-MM-DD). |
| `TIME` | string | A string like `"HH:MM:SS"` |
| `TIMESTAMP` | string | Format: `date-time` (RFC3339). Interpreted in configured timezone. |
| `XML` | string | XML document serialized as string. |

## Query templates

Query templates use Go's template syntax to generate SQL queries. The connector
uses a default template which implements appropriate behavior for full-refresh
and cursor-incremental bindings, but you can override this for custom behavior.

Overriding the query template is best left to power-users or done at the direction
of Estuary support. Consult the [connector source](https://github.com/estuary/connectors/blob/main/source-db2-batch/main.go)
for the current text of the default template.

## Known Limitations

### DBCLOB Unicode Handling

Due to a [reported issue](https://github.com/ibmdb/go_ibm_db/issues/274) in the
IBM Db2 Go driver, `DBCLOB` columns containing non-ASCII characters (such as
emoji or CJK text) may not decode correctly. ASCII text in `DBCLOB` columns
works correctly. If you encounter this issue, you can work around it by using
a custom query template override that casts the column to `VARCHAR`.
