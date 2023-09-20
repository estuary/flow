# Amazon Redshift

This connector captures data from your Amazon Redshift cluster into Flow collections.

[`ghcr.io/estuary/source-redshift:dev`](https://ghcr.io/estuary/source-redshift:dev) provides the latest connector image. You can access past image versions by following the link in your browser.

## Prerequisites

To use this connector, you'll need:

- Access credentials for connecting to your Amazon Redshift cluster.
- Properly configured IAM roles for the necessary permissions.

## Configuration

You can configure the Redshift source connector either through the Flow web app or by directly editing the Flow specification file. For more information on using this connector, see our guide on [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors). The values and specification sample below provide configuration details that are specific to the Amazon Redshift source connector.

### Properties

#### Endpoint

| Property        | Title         | Description                                                  | Type    | Required/Default       |
|-----------------|---------------|--------------------------------------------------------------|---------|------------------------|
| **`/host`**     | Host          | Hostname or IP address of your Redshift cluster.             | string  | Required               |
| **`/port`**     | Port          | Port number for the cluster.                                 | number  | Required               |
| **`/database`** | Database Name | Name of the database to capture data from.                   | string  | Required               |
| **`/user`**     | User          | Database user with necessary permissions.                    | string  | Required               |
| **`/password`** | Password      | Password for the specified database user.                    | string  | Required               |

### Bindings

| Property          | Title      | Description                    | Type    | Required/Default       |
| ----------------- | ---------- | ------------------------------ | ------- | ---------------------- |
| **`/table`**      | Table Name | Name of the table to capture.   | string  | Required               |
| **`/meta/op`**    | Operation  | Types of operation on records. | string | Optional              |

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
    bindings:
      - resource:
          schema: public
          table: users
        target: ${PREFIX}/users
      - field:
          source: /_meta/op
          target: ${PREFIX}/operation_type
```

## Flow CDC and the "/_meta/op" Field

Estuary Flow uses a special field called "/_meta/op" for the purposes of Change Data Capture (CDC). This is essential for capturing and processing data changes from your Amazon Redshift cluster. The "/_meta/op" field takes specific values based on the type of operation that has occurred on a record. It provides information about whether a record has been inserted, updated, or deleted within your source database.

### Values for "/_meta/op"

The "/_meta/op" field can take the following values:

- `insert`: Indicates that a new record has been added to the source database.
- `update`: An existing record in the source database has been modified.
- `delete`: A record has been removed from the source database.

### Using the "/_meta/op" Field

To use the "/_meta/op" field to configure the Redshift connector, you have to include it as part of your connector setup. This lets you tailor your data processing and integration workflows based on the type of operation performed on the source data from your Redshift collections.
