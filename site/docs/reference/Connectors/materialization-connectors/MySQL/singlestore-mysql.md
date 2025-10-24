
# SingleStore

This connector materializes Flow collections into tables in a SingleStore database.

It is available for use in the Flow web application. For local development or
open-source workflows,
[`ghcr.io/estuary/materialize-singlestore:dev`](https://ghcr.io/estuary/materialize-singlestore:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

- A SingleStore account with a database and database user
   - You will need to use [SSL](https://docs.singlestore.com/cloud/connect-to-singlestore/connect-with-mysql/connect-with-mysql-client/connect-to-singlestore-helios-using-tls-ssl/) to connect with a SingleStoreDB Cloud account
- At least one Flow collection

## Setup

To connect to your SingleStore database from Estuary, you must collect information on your SingleStore host, user, and password.

1. Select **Load Data** from the sidebar in the SingleStore dashboard.

2. Choose the option to connect with an external IDE or app.

3. Copy the displayed user credentials. Fill these into the **User** and **Password** fields in your Estuary connector configuration.

   Alternatively, you can create a new user in the **Access** tab of your workspace.

4. On the same screen, SingleStore will display a connection string. Copy the host and port and add them to Estuary as the **Address**.

   The host and port together should look something like: `svc-abc123.aws-region.svc.singlestore.com:3333`.

5. Copy your database name (ex. `db_name_123`) and add it to Estuary.

6. Download SingleStore's TLS/SSL certificate.

7. In the connector configuration in Estuary, expand the **Advanced Options** section.

8. Fill in SSL details. You can use the `verify_ca` SSL mode and supply SingleStore's PEM file as the SSL Server CA.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a SingleStore materialization, which will direct one or more of your Flow collections to your desired tables, or views, in the database.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/database`** | Database | Name of the logical database to materialize to. | string | Required |
| **`/address`** | Address | Host and port of the database. If only the host is specified, port will default to `3306`. | string | Required |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| **`/user`** | User | Database user to connect as. | string | Required |
| `/timezone` | Timezone | Timezone to use when materializing datetime columns. Should normally be left blank to use the database's 'time_zone' system variable. Only required if the 'time_zone' system variable cannot be read. Must be a valid IANA time zone name or +HH:MM offset. Takes precedence over the 'time_zone' system variable if both are set. | string |  |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object |  |
| `/advanced/sslmode` | SSL Mode | Overrides SSL connection behavior by setting the 'sslmode' parameter. | string |  |
| `/advanced/ssl_server_ca` | SSL Server CA | Optional server certificate authority to use when connecting with custom SSL mode | string |  |
| `/advanced/ssl_client_cert` | SSL Client Certificate | Optional client certificate to use when connecting with custom SSL mode. | string |  |
| `/advanced/ssl_client_key` | SSL Client Key | Optional client key to use when connecting with custom SSL mode. | string |  |

#### SSL Mode

Possible values:

- `disabled`: A plain unencrypted connection is established with the server
- `preferred`: Only use SSL connection if the server asks for it
- `required`: Connect using an SSL connection, but do not verify the server's
  certificate.
- `verify_ca`: Connect using an SSL connection, and verify the server's
  certificate against the given SSL Server CA, but does not verify the server's
  hostname. This option is most commonly used when connecting to an
  IP address which does not have a hostname to be verified. When using this mode, SSL Server
  CA must be provided.
- `verify_identity`: Connect using an SSL connection, verify the server's
  certificate and the server's hostname. This is the most secure option. When using this mode, SSL Server
  CA must be provided.

Optionally, SSL Client Certificate and Key can be provided if necessary to
authorize the client.

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/table`** | Table | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string | Required |
| `/delta_updates` | Delta Update | Should updates to this table be done via delta updates. | boolean | `false` |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-singlestore:dev
        config:
          database: flow
          address: svc-abc123.aws-region.svc.singlestore.com:3306
          password: flow
          user: flow
          advanced:
            sslmode: verify_ca
            ssl_server_ca: <singlestore-certificate>
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta updates

This connector supports both standard (merge) and [delta updates](/concepts/materialization/#delta-updates).
The default is to use standard updates.

## Reserved words

SingleStore uses MySQL, which has a list of reserved words that must be quoted in order to be used as an identifier.
You can find all the reserved words in the official [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/keywords.html).
