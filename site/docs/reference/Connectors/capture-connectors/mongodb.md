# MongoDB

This connector captures data from your MongoDB collections into Flow collections.

[`ghcr.io/estuary/source-mongodb:dev`](https://ghcr.io/estuary/source-mongodb:dev) provides the
latest connector image. You can also follow the link in your browser to see past image versions.

## Data model

MongoDB is a NoSQL database. Its [data
model](https://www.mongodb.com/docs/manual/core/data-modeling-introduction/) consists of
**documents** (lightweight records that contain mappings of fields and values) organized in
**collections**. MongoDB documents have a mandatory `_id` field that is used as the key of the
collection.

## Prerequisites

You'll need:

- Credentials for connecting to your MongoDB instance and database

- Read access to your MongoDB database(s), see [Role-Based Access
  Control](https://www.mongodb.com/docs/manual/core/authorization/) for more information.

:::tip Configuration Tip
If you are using a user with access to all databases, then in your mongodb address, you must specify
`?authSource=admin` parameter so that authentication is done through your admin database.
:::

- ReplicaSet enabled on your database, see [Deploy a Replica
  Set](https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set/).

- If you are using MongoDB Atlas, or your MongoDB provider requires allowlisting of IPs, you need to
  [allowlist the Estuary IP addresses](/reference/allow-ip-addresses).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification
file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using
connectors. The values and specification sample below provide configuration details specific to the
MongoDB source connector.

### Properties

#### Endpoint

| Property        | Title    | Description                                                                                                                        | Type   | Required/Default |
| --------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/address`**  | Address  | Host and port of the database. Optionally can specify scheme for the URL such as mongodb+srv://host.                               | string | Required         |
| **`/user`**     | User     | Database user to connect as.                                                                                                       | string | Required         |
| **`/password`** | Password | Password for the specified database user.                                                                                          | string | Required         |
| `/database`     | Database | Optional comma-separated list of the databases to discover. If not provided will discover all available databases in the instance. | string |                  |

#### Bindings

| Property          | Title    | Description     | Type   | Required/Default |
| ----------------- | -------- | --------------- | ------ | ---------------- |
| **`/database`**   | Database | Database name   | string | Required         |
| **`/collection`** | Stream   | Collection name | string | Required         |

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

1. Refer to the [guide](../../../../guides/connect-network/) to configure an SSH server on the cloud platform of your choice.

2. Configure your connector as described in the [configuration](#configuration) section above, with the addition of the `networkTunnel` stanza to enable the SSH tunnel, if using. See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

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
