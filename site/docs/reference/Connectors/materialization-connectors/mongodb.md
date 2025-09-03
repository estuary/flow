# MongoDB

This connector materializes data from your Flow collections to your MongoDB collections.

[`ghcr.io/estuary/materialize-mongodb:dev`](https://ghcr.io/estuary/materialize-mongodb:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Data model

MongoDB is a NoSQL database. Its [data
model](https://www.mongodb.com/docs/manual/core/data-modeling-introduction/)
consists of **documents** (lightweight records that contain mappings of fields
and values) organized in **collections**. MongoDB documents have a mandatory
`_id` field that is used as the key of the collection. Flow collection documents
are materialized as MongoDB documents with an `_id` field value based on the
Flow collection key.

:::info
If your Flow collection already has a field named `_id`, its value will
be present in the materialized MongoDB document as the field `_flow_id` to
prevent conflicts with the required `_id` field.
:::

## Prerequisites

You'll need:

- Credentials for connecting to your MongoDB instance and database.

- Read and write access to your MongoDB database and desired collections. See [Role-Based Access
  Control](https://www.mongodb.com/docs/manual/core/authorization/) for more information.

- If you are using MongoDB Atlas, or your MongoDB provider requires allowlisting
  of IPs, you need to [allowlist the Estuary IP addresses](/reference/allow-ip-addresses).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Firestore source connector.

### Properties

#### Endpoint

| Property        | Title    | Description                                                                                          | Type   | Required/Default |
| --------------- | -------- | ---------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/address`**  | Address  | Host and port of the database. Optionally can specify scheme for the URL such as mongodb+srv://host. | string | Required         |
| **`/database`** | Database | Name of the database to capture from.                                                                | string | Required         |
| **`/user`**     | User     | Database user to connect as.                                                                         | string | Required         |
| **`/password`** | Password | Password for the specified database user.                                                            | string | Required         |

#### Bindings

| Property          | Title        | Description                                             | Type    | Required/Default |
| ----------------- | ------------ | ------------------------------------------------------- | ------- | ---------------- |
| **`/collection`** | Stream       | Collection name                                         | string  | Required         |
| `/delta_updates`  | Delta Update | Should updates to this table be done via delta updates. | boolean | `false`          |

### Sample

```yaml
materializations:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-mongodb:dev
        config:
          address: "mongo:27017"
          database: "test"
          password: "flow"
          user: "flow"
    bindings:
      - resource:
          collection: users
          database: test
        source: ${PREFIX}/users
```

## SSH Tunneling

As an alternative to connecting to your MongoDB instance directly, you can allow secure connections via SSH tunneling. To do so:

1. Refer to the [guide](../../../../guides/connect-network/) to configure an SSH server on the cloud platform of your choice.

2. Configure your connector as described in the [configuration](#configuration) section above, with the addition of the `networkTunnel` stanza to enable the SSH tunnel, if using. See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

## Delta updates

This connector supports both standard (merge) and [delta updates](/concepts/materialization/#delta-updates).
The default is to use standard updates.
