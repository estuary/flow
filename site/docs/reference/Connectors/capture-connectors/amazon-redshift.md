# Amazon Redshift (Deprecated)

This connector captures data from your Amazon Redshift cluster into Estuary collections.

:::warning
This connector is deprecated. For the best experience, we recommend using our native [Redshift batch connector](./redshift-batch.md) instead.
:::

## Prerequisites

To use this connector, you'll need:

- Access credentials for connecting to your Amazon Redshift cluster.
- Properly configured IAM roles for the necessary permissions.

## Configuration

You can configure the Redshift source connector either through the Estuary web app or by directly editing the Data Flow specification file. For more information on using this connector, see our guide on [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors). The values and specification sample below provide configuration details that are specific to the Amazon Redshift source connector.

### Properties

#### Endpoint

| Property        | Title         | Description                                                  | Type    | Required/Default       |
|-----------------|---------------|--------------------------------------------------------------|---------|------------------------|
| **`/host`**     | Host          | Hostname or IP address of your Redshift cluster.             | string  | Required               |
| **`/port`**     | Port          | Port number for the cluster.                                 | integer  | Default               |
| **`/database`** | Database Name | Name of the database to capture data from.                   | string  | Required               |
| **`/user`**     | User          | Database user with necessary permissions.                    | string  | Required               |
| **`/password`** | Password      | Password for the specified database user.                    | string  | Required               |
| **`/schemas`**     | Schemas         | List of schemas to include.                                  | string  |              |
| **`/jdbc_params`** | JDBC URL Params | Additional properties to pass to the JDBC URL string when connecting to the database formatted as 'key=value' pairs. | string |               |


#### Bindings

| Property          | Title      | Description                    | Type    | Required/Default       |
| ----------------- | ---------- | ------------------------------ | ------- | ---------------------- |
| **`/table`**      | Table Name | Name of the table to capture.   | string  | Required               |
| **`/cursor_field`**    | User-defined Cursor     | Field for incremental syncs. Uses ascending values to ensure queries are sequential. | string or integer | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-redshift:dev
        config:
          host: "example-redshift-cluster.us-east-2.redshift.amazonaws.com"
          port: 5439
          database: "sample_db"
          user: "sample_user"
          password: "sample_password"
          schemas: "public"
          jdbc_params: "key1=value1&key2=value2&key3=value3"
    bindings:
      - resource:
          table: users
          cursor_field: cursor
        target: ${PREFIX}/users
```
