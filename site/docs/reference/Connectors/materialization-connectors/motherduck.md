
# MotherDuck

This connector materializes Flow collections into tables in a MotherDuck database.

The connector uses your AWS account to materialize to MotherDuck tables by way of files in an S3
bucket. The files in the bucket as as a temporary staging area for data storage and retrieval.

[`ghcr.io/estuary/materialize-motherduck:dev`](https://ghcr.io/estuary/materialize-motherduck:dev)
provides the latest connector image. You can also follow the link in your browser to see past image
versions.

## Prerequisites

To use this connector, you'll need:

* A [MotherDuck](https://motherduck.com/) account and [Service
  Token](https://motherduck.com/docs/authenticating-to-motherduck#fetching-the-service-token).
* An S3 bucket for staging temporary files. See [this
  guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) for
  instructions on setting up a new S3 bucket.
* An AWS root or IAM user with [read and write
  access](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html)
  to the S3 bucket. For this user, you'll need the **access key** and **secret access key**. See the
  [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding
  these credentials.

## Configuration

Use the below properties to configure MotherDuck materialization, which will direct one or
more of your Flow collections to your desired tables in the database.

### Properties

#### Endpoint

| Property                  | Title                    | Description                                                                                                                                                      | Type   | Required/Default |
|---------------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/token`**              | MotherDuck Service Token | Service token for authenticating with MotherDuck.                                                                                                                | string | Required         |
| **`/database`**           | Database                 | The database to materialize to.                                                                                                                                  | string | Required         |
| **`/schema`**             | Database Schema          | Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables. | string | Required         |
| **`/bucket`**             | S3 Staging Bucket        | Name of the S3 bucket to use for staging data loads.                                                                                                             | string | Required         |
| **`/awsAccessKeyId`**     | Access Key ID            | AWS Access Key ID for reading and writing data to the S3 staging bucket.                                                                                         | string | Required         |
| **`/awsSecretAccessKey`** | Secret Access Key        | AWS Secret Access Key for reading and writing data to the S3 staging bucket.                                                                                     | string | Required         |
| `/bucketPath`             | Bucket Path              | A prefix that will be used to store objects in S3.                                                                                                               | string |                  |

#### Bindings

| Property         | Title              | Description                                                                                                   | Type    | Required/Default |
|------------------|--------------------|---------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/table`**     | Table              | Name of the database table.                                                                                   | string  | Required         |
| `/delta_updates` | Delta Update       | Should updates to this table be done via delta updates. Currently this connector only supports delta updates. | boolean | `true`           |
| `/schema`        | Alternative Schema | Alternative schema for this table (optional).                                                                 | string  |                  |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/materialize-motherduck:dev"
        config:
          token: <motherduck_service_token>
          database: my_db
          schema: main
          bucket: my_bucket
          awsAccessKeyId: <access_key_id>
          awsSecretAccessKey: <secret_access_key>
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta updates

This connector currently supports only [delta updates](../../../../concepts/materialization/#delta-updates).
Future support for standard updates is planned.
