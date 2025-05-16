# SQL Server Batch Query Connector

This connector captures data from SQL Server into Flow collections by periodically
executing queries and translating the results into JSON documents.

For local development or open-source workflows, [`ghcr.io/estuary/source-sqlserver-batch:dev`](https://ghcr.io/estuary/source-sqlserver-batch:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

We recommend using our [SQL Server CDC Connector](./sqlserver.md) instead
if possible. Using CDC provides lower latency data capture, delete and update events, and usually
has a smaller impact on the source database.

However there are some circumstances where this might not be feasible. Perhaps you need
to capture from a managed SQL Server instance which doesn't support logical replication.
Or perhaps you need to capture the contents of a view or the result of an ad-hoc query.
That's the sort of situation this connector is intended for.

The number one caveat you need to be aware of when using this connector is that **it will
periodically execute its update query over and over**. For example, if you set the polling interval to
5 minutes, a naive `SELECT * FROM foo` query against a 100 MiB view will produce 30 GiB/day
of ingested data, most of it duplicated. The default polling interval is set
to 24 hours to minimize the impact of this behavior, but even then it could mean a lot of
duplicated data being processed depending on the size of your tables.

If you start editing these queries or manually adding capture bindings for views or to run
ad-hoc queries, you need to either have some way of restricting the query to "just the new
rows since last time" or else have your polling interval set high enough that the data rate
`<DatasetSize> / <PollingInterval>` is an amount of data you're willing to deal with.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SQL Server batch source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/address`** | Server Address | The host or host:port at which the database can be reached. | string | Required |
| **`/database`** | Database | Logical database name to capture from. | string | Required |
| **`/user`** | User | The database user to authenticate as. | string | Required, `"flow_capture"` |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object |  |
| `/advanced/discover_views` | Discover Views | When set views will be automatically discovered as resources. If unset only tables will be discovered. | boolean | `false` |
| `/advanced/timezone` | Timezone | The IANA timezone name in which datetime columns will be converted to RFC3339 timestamps. | string | `UTC` |
| `/advanced/poll` | Polling Schedule | When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. | string | `24h` |
| `/advanced/discover_schemas` | Discovery Schema Selection | If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas. | string[] |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/name`** | Name | The name of the resource | string | Required |
| `/schema` | Schema | Schema where the table is located | string |  |
| `/table` | Table | The name of the table to be captured | string |  |
| `/cursor` | Cursor | The names of columns which should be persisted between query executions as a cursor | string[] |  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-sqlserver-batch:dev"
        config:
          address: "<host>:1433"
          database: "my_db"
          user: "flow_capture"
          password: "secret"
    bindings:
      - resource:
          name: "transactions"
          schema: "main"
          table: "transactions"
          cursor:
            - "id"
        target: ${PREFIX}/${COLLECTION_NAME}
```
