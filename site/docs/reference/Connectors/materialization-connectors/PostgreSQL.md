
This connector materializes Flow collections into tables in a PostgreSQL database.

[`ghcr.io/estuary/materialize-postgres:dev`](https://ghcr.io/estuary/materialize-postgres:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A Postgres database to which to materialize, and user credentials.
  The connector will create new tables in the database per your specification. Tables created manually in advance are not supported.
* At least one Flow collection

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Postgres materialization, which will direct one or more of your Flow collections to your desired tables, or views, in the database.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/database` | Database | Name of the logical database to materialize to. | string |  |
| **`/host`** | Host | Host name of the database. | string | Required |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| `/port` | Port | Port on which to connect to the database. | integer |  |
| **`/user`** | User | Database user to connect as. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string | Required |

### Sample

```yaml
materializations:
  ${tenant}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-postgres:dev
        config:
          database: flow
          host: localhost
          password: flow
          port: 5432
          user: flow
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${TENANT}/${COLLECTION_NAME}
```

## PostgreSQL on managed cloud platforms

In addition to standard PostgreSQL, this connector supports cloud-based PostgreSQL instances.
To connect securely, you must use an SSH tunnel.

Google Cloud Platform, Amazon Web Service, and Microsoft Azure are currently supported.
You may use other cloud platforms, but Estuary doesn't guarantee performance.


### Setup

1. Refer to the [guide](../../../../guides/connect-network/) to configure an SSH server on the cloud platform of your choice.

2. Configure your connector as described in the [configuration](#configuration) section above,
with the additional of the `networkProxy` stanza to enable the SSH tunnel, if using.
See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
for additional details and a sample.

:::tip
You can find the values for `forwardHost` and `forwardPort` in the following locations in each platform's console:
* Amazon RDS: `forwardHost` as Endpoint; `forwardPort` as Port.
* Google Cloud SQL: `forwardHost` as Private IP Address; `forwardPort` is always `5432`. You may need to [configure private IP](https://cloud.google.com/sql/docs/postgres/configure-private-ip) on your database.
* Azure Database: `forwardHost` as Server Name; `forwardPort` under Connection Strings (usually `5432`).
:::