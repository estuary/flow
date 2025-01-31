# MongoDB

This connector captures data from your MongoDB collections into Flow collections.

[`ghcr.io/estuary/source-mongodb:dev`](https://ghcr.io/estuary/source-mongodb:dev) provides the
latest connector image. You can also follow the link in your browser to see past image versions.

## Supported platforms

The MongoDB connector has a couple variants to support additional document-based database options. Continue reading this page for standard MongoDB setup or see one of the following:

* [Amazon DocumentDB](./amazon-documentdb.md)
* [Azure Cosmos DB](./azure-cosmosdb.md)

## Data model

MongoDB is a NoSQL database. Its [data
model](https://www.mongodb.com/docs/manual/core/data-modeling-introduction/) consists of
**documents** (lightweight records that contain mappings of fields and values) organized in
**collections**. MongoDB documents have a mandatory `_id` field that is used as the key of the
collection.

## Prerequisites

You'll need:

- Credentials for connecting to your MongoDB instance and database.

- Read access to your MongoDB database(s), see [Role-Based Access
  Control](https://www.mongodb.com/docs/manual/core/authorization/) for more information.

:::tip Configuration Tip
If you are using a user with access to all databases, then in your mongodb address, you must specify
`?authSource=admin` parameter so that authentication is done through your admin database.
:::

- If you are using MongoDB Atlas, or your MongoDB provider requires allowlisting of IPs, you need to
  [allowlist the Estuary IP addresses](/reference/allow-ip-addresses).

## Capture Modes

MongoDB [change streams](https://www.mongodb.com/docs/manual/changeStreams/) are
the preferred way to capture on-going changes to collections. Change streams
allow capturing real-time events representing new documents in your collections,
updates to existing documents, and deletions of documents. If change streams are
enabled on the MongoDB instance/deployment you are connecting to, they will be
used preferentially for capturing changes.

An alternate "batch" mode of capturing documents can be used for deployments
that do not support change streams, and for MongoDB collection types that do not
support change streams ([views](https://www.mongodb.com/docs/manual/core/views/)
and [time
series](https://www.mongodb.com/docs/manual/core/timeseries-collections/)
collections). The capture mode is configured on a per-collection level in the
**Bindings** configuration and can be one of the following:
- **Change Stream Incremental**: This is the preferred mode and uses change streams to capture change events.
- **Batch Snapshot**: Performs a "full refresh" by scanning the entire MongoDB
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

Only the **Change Stream Incremental** mode is capable of capturing deletion
events. **Batch Snapshot** will capture updates by virtue of it re-capturing the
entire source collection periodically. **Batch Incremental** _may_ capture
updates to documents if updated documents have strictly increasing values for
the cursor field.


## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification
file. See [connectors](../../../../concepts/connectors.md#using-connectors) to learn more about using
connectors. The values and specification sample below provide configuration details specific to the
MongoDB source connector.

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
| `/captureMode`    | Capture Mode     | Either **Change Stream Incremental**, **Batch Snapshot**, or **Batch Incremental**                                                                                                                                                                                                         | string |                  |
| `/cursorField`    | Cursor Field     | The name of the field to use as a cursor for batch-mode bindings. For best performance this field should be indexed. When used with 'Batch Incremental' mode documents added to the collection are expected to always have the cursor field and for it to be strictly increasing.          | string |                  |
| `/pollSchedule`   | Polling Schedule | When and how often to poll batch collections (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset. | string |                  |


### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-mongodb:dev
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

As an alternative to connecting to your MongoDB instance directly, you can allow secure connections via SSH tunneling. To do so:

1. Refer to the [guide](../../../../../guides/connect-network/) to configure an SSH server on the cloud platform of your choice.

2. Configure your connector as described in the [configuration](#configuration) section above, with the addition of the `networkTunnel` stanza to enable the SSH tunnel, if using. See [Connecting to endpoints on secure networks](../../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

## Backfill and real-time updates

When performing the initial database snapshot, the connector continuously reads from [**change
streams**](https://www.mongodb.com/docs/manual/changeStreams/) to capture change events while
executing collection scans to backfill pre-existing documents. After the initial snapshot, the
connector continues to read from the change streams indefinitely to capture all changes going
forward.

If the connector's process is paused for a while, it will attempt to resume capturing change events
from where it left off, however the connector's ability to do this depends on the size of the
[replica set oplog](https://www.mongodb.com/docs/manual/core/replica-set-oplog/), and in certain
circumstances, when the pause has been long enough for the oplog to have evicted old change events,
the connector will need to re-do the backfill to ensure data consistency. In these cases it is
necessary to [resize your
oplog](https://www.mongodb.com/docs/manual/tutorial/change-oplog-size/#c.-change-the-oplog-size-of-the-replica-set-member)
or [set a minimum retention
period](https://www.mongodb.com/docs/manual/reference/command/replSetResizeOplog/#minimum-oplog-retention-period)
for your oplog to be able to reliably capture data. The recommended minimum retention period is at
least 24 hours, but we recommend higher values to improve reliability.

## Change Event Pre- and Post-Images

Captured documents for change events from `update` operations will always
include a full post-image, since the change stream is configured with the [`{
fullDocument: 'updateLookup' }`
setting](https://www.mongodb.com/docs/manual/changeStreams/#lookup-full-document-for-update-operations).

Pre-images for `update`, `replace`, and `delete` operations will be captured if
they are available. For these pre-images to be captured, the source MongoDB
collection must have `changeStreamPreAndPostImages` enabled. See the [official
MongoDB
documentation](https://www.mongodb.com/docs/manual/changeStreams/#change-streams-with-document-pre--and-post-images)
for more information on how to enable this setting.
