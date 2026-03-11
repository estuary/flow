# SingleStore Batch Query Connector

This connector captures data from SingleStore into Estuary collections by periodically executing queries and translating
the results into JSON documents. It leverages SingleStore's MySQL wire compatibility to interact with the database.

**This connector periodically re-executes the query**. The default polling interval is set
to 24 hours to minimize this behavior's impact, but depending on table size, it may lead to duplicated data being
processed.

If the dataset has a natural cursor that can identify only new or updated rows, it should be specified by editing the
`Cursor` property of the binding. Common examples of suitable cursors include:

- Update timestamps, which are typically the best choice since they can capture all changed rows, not just new rows.
- Creation timestamps, which work for identifying newly added rows in append-only datasets but won’t capture updates.
- Serially increasing IDs, which can be used to track newly added rows.

## Setup

1. Ensure that [Estuary's IP addresses are allowlisted](/reference/allow-ip-addresses) to allow access. You can do by
   following [these steps](https://docs.singlestore.com/cloud/reference/management-api/#control-access-to-the-api)
2. Grab the following details from the SingleStore workspace.
    1. Workspace URL
    2. Username
    3. Password
    4. Database
3. Configure the Connector with the appropriate values. Make sure to specify the database name under the "Advanced"
   section.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SingleStore batch source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/address`** | Server Address | The host or host:port at which the database can be reached. | string | Required |
| **`/user`** | User | The database user to authenticate as. | string | Required, `"flow_capture"` |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object | |
| `/advanced/poll` | Default Polling Schedule | When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling, or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | `"24h"` |
| `/advanced/discover_views` | Discover Views | When set, views will be automatically discovered as resources. If unset, only tables will be discovered. | boolean | `false` |
| `/advanced/discover_schemas` | Discovery Schema Selection | If this is specified, only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas. | array | `[]` |
| `/advanced/dbname` | Database Name | The name of the database to connect to. This is optional, as the connector can discover and capture from all databases it's authorized to access. | string | |
| `/advanced/source_tag` | Source Tag | When set, the capture will add this value as the property 'tag' in the source metadata of each document. | string | |
| `/networkTunnel` | Network Tunnel | Connect to your system through an SSH server that acts as a bastion host for your network. | object | |

#### Binding

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/name`** | Resource Name | The unique name of this resource. | string | Required |
| `/schema` | Schema Name | The name of the schema in which the captured table lives. Must be set unless using a custom template. | string | |
| `/table` | Table Name | The name of the table to be captured. Must be set unless using a custom template. | string | |
| `/cursor` | Cursor Columns | The names of columns which should be persisted between query executions as a cursor. | array | `[]` (full-refresh) |
| `/poll` | Polling Schedule | When and how often to execute the fetch query (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling, or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | |
| `/template` | Query Template Override | Optionally overrides the query template which will be rendered and then executed. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-singlestore-batch:v1
        config:
          address: host:port
          user: flow_capture
          password: secret
    bindings:
      - resource:
          name: main_orders
          schema: main
          table: orders
        target: ${PREFIX}/${COLLECTION_NAME}
```

## Query templates

Query templates use Go's template syntax to generate SQL queries. The connector
uses a default template which implements appropriate behavior for full-refresh
and cursor-incremental bindings, but you can override this for custom behavior.

Overriding the query template is best left to power-users or done at the direction
of Estuary support. Consult the [connector source](https://github.com/estuary/connectors/blob/main/source-mysql-batch/main.go)
for the current text of the default template.
