---
sidebar_position: 4
---
# MongoDB

This connector captures data from your MongoDB collections into Flow collections.

[`ghcr.io/estuary/source-mongodb:dev`](https://ghcr.io/estuary/source-mongodb:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Data model

MongoDB is a NoSQL database. Its [data
model](https://www.mongodb.com/docs/manual/core/data-modeling-introduction/)
consists of **documents** (lightweight records that contain mappings of fields
and values) organized in **collections**. MongoDB documents have a mandatory
`_id` field that is used as the key of the collection.

## Prerequisites

You'll need:

* Credentials for connecting to your MongoDB instance and database

    * Read access to your MongoDB database and desired collections, see
      [Role-Based Access
      Control](https://www.mongodb.com/docs/manual/core/authorization/) for more
      information.

* ReplicaSet enabled on your database, see [Deploy a Replica
  Set](https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set/).

* If you are using MongoDB Atlas, or your MongoDB provider requires whitelisting
  of IPs, you need to whitelist Estuary's IP `34.121.207.128`.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Firestore source connector.

### Properties

#### Endpoint

| Property                        | Title               | Description                                                                                                                                 | Type    | Required/Default           |
|---------------------------------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------|---------|----------------------------|
| **`/address`**                  | Address             | Host and port of the database. Optionally can specify scheme for the URL such as mongodb+srv://host.                                        | string  | Required                   |
| **`/database`**                 | Database            | Name of the database to capture from.                                                                         | string  | Required                   |
| **`/user`**                     | User                | Database user to connect as.                                                                                   | string  | Required                   |
| **`/password`**                 | Password            | Password for the specified database user.                                                                                                   | string  | Required                   |

#### Bindings

| Property          | Title    | Description     | Type      | Required/Default |
| -------           | ------   | ------          | --------- | --------         |
| **`/database`**   | Database | Database name   | string    | Required         |
| **`/collection`** | Stream   | Collection name | string    | Required         |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-mongodb:dev
        config:
          address: "mongo:27017"
          database: "test"
          password: "flow"
          user: "flow"
    bindings:
      - resource:
          collection: users
          database: test
        target: ${PREFIX}/users
```

## Backfill and real-time updates

The connector starts by backfilling data from the specified collections until it
reaches the current time. Once all the data up to the current time has been
backfilled, the connector then uses [**change
streams**](https://www.mongodb.com/docs/manual/changeStreams/) to capture
change events from the database and emit those updates to the flow collection.

If the connector's process is paused for a while, it will attempt to resume
capturing change events since the last received change event, however the
connector's ability to do this depends on the size of the [replica set
oplog](https://www.mongodb.com/docs/manual/core/replica-set-oplog/), and in
certain circumstances, when the pause has been long enough for the oplog to have
evicted old change events, the connector will need to re-do the backfill to
ensure data consistency. In such cases, the connector will error out the first
time it is run with a message indicating that it is going to re-do a backfill on
the next run, and it will be restarted by Flow runtime. This new run of the
connector will do a backfill and once fully caught up, will start capturing
change events.
