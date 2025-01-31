# Amazon DocumentDB

This connector captures data from your Amazon DocumentDB collections into Flow collections.

[`ghcr.io/estuary/source-amazon-documentdb:dev`](https://ghcr.io/estuary/source-amazon-documentdb:dev) provides the
latest connector image. You can also follow the link in your browser to see past image versions.

## Data model

Amazon DocumentDB is a NoSQL database. It is compatible with [MongoDB's data
model](https://www.mongodb.com/docs/manual/core/data-modeling-introduction/), which consists of
**documents** (lightweight records that contain mappings of fields and values) organized in
**collections**. MongoDB documents have a mandatory `_id` field that is used as the key of the
collection.

## Prerequisites

You'll need:

- Credentials for connecting to your Amazon DocumentDB instance and database.

- Read access to your DocumentDB database(s). See [Database access using Role-Based Access
  Control](https://docs.aws.amazon.com/documentdb/latest/developerguide/role_based_access_control.html) for more information.

## Capture Modes

A "batch" mode of capturing documents can be used. The capture mode is configured on a per-collection level in the
**Bindings** configuration and can be one of the following:
- **Batch Snapshot**: Performs a "full refresh" by scanning the entire DocumentDB
  collection on a set schedule. A cursor field must be configured, which should
  usually be the `_id` field.
- **Batch Incremental**: Performs a scan on a set schedule where only documents
  having a higher cursor field value than previously observed are captured. This
  mode should be used for append-only collections, or where a field value is
  known to be strictly increasing for all document insertions and updates.

:::tip Using Cursor Fields
For best performance the selected cursor field should have an
[index](https://www.mongodb.com/docs/manual/indexes/). This ensures backfill
queries are able to be run efficiently, since they require sorting the
collection based on the cursor field.
:::

:::tip Time Series Collections
Time series collections do _not_ have a default index on the `_id`, but do have
an index on the `timeField` for the collection. This makes the `timeField` a
good choice for an incremental cursor if new documents are only ever added to
the collection with strictly increasing values for the `timeField`. The capture
connector will automatically discover time series collections in **Batch
Incremental** mode with the cursor set to the collection's `timeField`.
:::

**Batch Snapshot** will capture updates by virtue of it re-capturing the
entire source collection periodically. **Batch Incremental** _may_ capture
updates to documents if updated documents have strictly increasing values for
the cursor field.



## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification
file. See [connectors](../../../../concepts/connectors.md#using-connectors) to learn more about using
connectors. The values and specification sample below provide configuration details specific to the
Amazon DocumentDB source connector.

### Properties

#### Endpoint

| Property                | Title                                                              | Description                                                                                                                                                                                                                                                                                                     | Type    | Required/Default |
|-------------------------|--------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/address`**          | Address                                                            | Host and port of the database. Optionally can specify scheme for the URL such as mongodb+srv://host.                                                                                                                                                                                                            | string  | Required         |
| **`/user`**             | User                                                               | Database user to connect as.                                                                                                                                                                                                                                                                                    | string  | Required         |
| **`/password`**         | Password                                                           | Password for the specified database user.                                                                                                                                                                                                                                                                       | string  | Required         |
| `/database`             | Database                                                           | Optional comma-separated list of the databases to discover. If not provided will discover all available databases in the instance.                                                                                                                                                                              | string  |                  |
| `/batchAndChangeStream` | Capture Batch Collections in Addition to Change Stream Collections | Discover collections that can only be batch captured if the deployment supports change streams. Check this box to capture views and time series collections as well as change streams. All collections will be captured in batch mode if the server does not support change streams regardless of this setting. | boolean |                  |
| `/pollSchedule`         | Default Batch Collection Polling Schedule                          | When and how often to poll batch collections. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset                                                                 | string  |                  |

#### Bindings

| Property          | Title            | Description                                                                                                                                                                                                                                                                                | Type   | Required/Default |
|-------------------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/database`**   | Database         | Database name                                                                                                                                                                                                                                                                              | string | Required         |
| **`/collection`** | Stream           | Collection name                                                                                                                                                                                                                                                                            | string | Required         |
| `/captureMode`    | Capture Mode     | Either **Batch Snapshot**, or **Batch Incremental**                                                                                                                                                                                                         | string |                  |
| `/cursorField`    | Cursor Field     | The name of the field to use as a cursor for batch-mode bindings. For best performance this field should be indexed. When used with 'Batch Incremental' mode documents added to the collection are expected to always have the cursor field and for it to be strictly increasing.          | string |                  |
| `/pollSchedule`   | Polling Schedule | When and how often to poll batch collections (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset. | string |                  |


### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-amazon-documentdb:dev
        config:
          address: "mongo:27017"
          password: "flow"
          user: "flow"
    bindings:
      - resource:
          collection: users
          database: test
        target: ${PREFIX}/users
```

## SSH Tunneling

As an alternative to connecting to your DocumentDB instance directly, you can allow secure connections via [SSH tunneling](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html). To do so:

1. Refer to the [guide](../../../../../guides/connect-network/) to configure an SSH server on the cloud platform of your choice.

2. Configure your connector as described in the [configuration](#configuration) section above, with the addition of the `networkTunnel` stanza to enable the SSH tunnel, if using. See [Connecting to endpoints on secure networks](../../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.
