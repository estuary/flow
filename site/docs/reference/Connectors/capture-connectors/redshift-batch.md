# Amazon Redshift Batch Query Connector

This connector captures data from Amazon Redshift databases into Estuary collections by
periodically executing queries and translating the results into JSON documents.

## When to use this connector

This batch query connector is the recommended way to capture data from Amazon Redshift.

## Supported versions and platforms

This connector works with Amazon Redshift, AWS's managed data warehouse service based
on PostgreSQL. It supports all current Redshift versions and cluster configurations.

:::tip Configuration Tip
To capture data from Redshift clusters in a private VPC, you may need to
use [SSH tunneling](/guides/connect-network/). If you have a
[private deployment](/getting-started/deployment-options/#private-deployment),
you can also use private cloud networking features to reach your cluster.
:::

## Prerequisites

You'll need:

- An Amazon Redshift cluster with a user that has `SELECT` permission on the tables
  you want to capture
- Network access to the cluster (direct, via SSH tunnel, or through VPC networking)

## Setup

### Creating a capture user

We recommend creating a dedicated user for Estuary captures:

```sql
CREATE USER flow_capture WITH PASSWORD 'secret';
```

Grant read permissions on the schemas you want to capture:

```sql
-- Grant SELECT on a specific schema
GRANT USAGE ON SCHEMA my_schema TO flow_capture;
GRANT SELECT ON ALL TABLES IN SCHEMA my_schema TO flow_capture;

-- For future tables in the schema
ALTER DEFAULT PRIVILEGES IN SCHEMA my_schema GRANT SELECT ON TABLES TO flow_capture;
```

Replace `my_schema` with the name of your schema, and grant permissions on
multiple schemas as needed.

### Configuring network access

If you are not using private cloud networking, your Redshift cluster will need
to accept inbound connections from Estuary's data planes:

1. Set 'Publicly Accessible' to 'On' for your Redshift workgroup

2. Configure your VPC Security Group with an Inbound Rule permitting Redshift
   traffic (port 5439) from Estuary's IP addresses. See [IP addresses to allow](/reference/allow-ip-addresses/)
   for the current list based on your data plane.

3. Configure your VPC Route Table with a route for traffic to Estuary's IP
   addresses to egress via an Internet Gateway.

4. If your VPC uses network ACLs, configure them to permit inbound and outbound
   Redshift traffic.

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
volume as necessary.
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

## Configuration

Configure this connector in the Estuary web app or using YAML config files with
[flowctl CLI](/guides/flowctl/). See [connectors](/concepts/connectors/#using-connectors)
to learn more about using connectors.

### Endpoint Properties

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/address`** | Server Address | The host or host:port at which the database can be reached. | string | Required |
| **`/user`** | User | The database user to authenticate as. | string | Required, `"flow_capture"` |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| **`/database`** | Database | Logical database name to capture from. | string | Required, `"dev"` |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object | |
| `/advanced/poll` | Default Polling Schedule | When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling, or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | `"24h"` |
| `/advanced/discover_views` | Discover Views | When set, views will be automatically discovered as resources. If unset, only tables will be discovered. | boolean | `false` |
| `/advanced/discover_schemas` | Discovery Schema Selection | If this is specified, only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas. | array | `[]` |
| `/advanced/sslmode` | SSL Mode | Overrides SSL connection behavior by setting the 'sslmode' parameter. | string | |
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

Redshift types are mapped to JSON types as follows:

| Redshift Type | JSON Type | Notes |
|---------------|-----------|-------|
| `BOOLEAN`, `BOOL` | `boolean` | |
| `SMALLINT`, `INTEGER`, `BIGINT` | `integer` | |
| `NUMERIC`, `DECIMAL` | `string` | Format: `number` to preserve precision |
| `REAL`, `DOUBLE PRECISION` | `number` or `string` | NaN, Infinity, -Infinity encoded as strings |
| `VARCHAR`, `CHAR`, `TEXT` | `string` | |
| `DATE` | `string` | Format: `date-time` (RFC3339) with `00:00:00` time and `Z` time zone |
| `TIME` | `string` | Formatted as `HH:MM:SS` |
| `TIMETZ` | `string` | Format: `time` |
| `TIMESTAMP` | `string` | Format: `date-time` (RFC3339) |
| `TIMESTAMPTZ` | `string` | Format: `date-time` (RFC3339) |
| `VARBYTE` | `string` | Base64 encoded bytes |

## Query templates

Query templates use Go's template syntax to generate SQL queries. The connector
uses a default template which implements appropriate behavior for full-refresh
and cursor-incremental bindings, but you can override this for custom behavior.

Overriding the query template is best left to power-users or done at the direction
of Estuary support. Consult the [connector source](https://github.com/estuary/connectors/blob/main/source-redshift-batch/main.go)
for the current text of the default template.
