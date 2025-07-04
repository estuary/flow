
import ReactPlayer from "react-player";

# MotherDuck

This connector materializes Flow collections into tables in a MotherDuck database.

The connector uses your AWS account to materialize to MotherDuck tables by way of files in an S3
bucket. The files in the bucket are used as a temporary staging area for data storage and retrieval.

[`ghcr.io/estuary/materialize-motherduck:dev`](https://ghcr.io/estuary/materialize-motherduck:dev)
provides the latest connector image. You can also follow the link in your browser to see past image
versions.

<ReactPlayer controls url="https://www.youtube.com/watch?v=2flyH-rjmqI" />

## Prerequisites

To use this connector, you'll need:

* A [MotherDuck](https://motherduck.com/) account and [Service
  Token](https://motherduck.com/docs/authenticating-to-motherduck#fetching-the-service-token).
* An S3 bucket for staging temporary files, or a GCS bucket for staging
  temporary files. An S3 bucket in `us-east-1` is recommended for best
  performance and costs, since MotherDuck is currently hosted in that region.

To use an S3 bucket for staging temporary files:
* See [this
  guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)
  for instructions on setting up a new S3 bucket.
* Create a AWS root or IAM user with [read and write
  access](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html)
  to the S3 bucket. For this user, you'll need the **access key** and **secret
  access key**. See the [AWS
  blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for
  help finding these credentials.

To use a GCS bucket for staging temporary files:
* See [this guide](https://cloud.google.com/storage/docs/creating-buckets) for
  instructions on setting up a new GCS bucket.
* Create a Google Cloud [service
  account](https://cloud.google.com/docs/authentication/getting-started) with a
  key file generated and
  [`roles/storage.objectAdmin`](https://cloud.google.com/storage/docs/access-control/iam-roles#standard-roles)
  on the GCS bucket you want to use.
* Create an [HMAC
  Key](https://cloud.google.com/storage/docs/authentication/managing-hmackeys)
  for the service account. You'll need the **Access ID** and **Secret** for
  the key you create.

## Configuration

Use the below properties to configure MotherDuck materialization, which will direct one or
more of your Flow collections to your desired tables in the database.

### Properties

#### Endpoint

| Property                              | Title                    | Description                                                                                                                                                      | Type    | Required/Default |
|---------------------------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/token`**                          | MotherDuck Service Token | Service token for authenticating with MotherDuck.                                                                                                                | string  | Required         |
| **`/database`**                       | Database                 | The database to materialize to.                                                                                                                                  | string  | Required         |
| **`/schema`**                         | Database Schema          | Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables. | string  | Required         |
| `/hardDelete`                         | Hard Delete              | If enabled, items deleted in the source will also be deleted from the destination.                                                                               | boolean | `false`          |
| **`stagingBucket/stagingBucketType`** | Staging Bucket Type      | The type of staging bucket to use. Either S3 or GCS.                                                                                                             | string  | Required         |
| `stagingBucket/bucketS3`              | S3 Staging Bucket        | Name of the S3 bucket to use for staging data loads. Must not contain dots (.)                                                                                   | string  |                  |
| `stagingBucket/awsAccessKeyId`        | Access Key ID            | AWS Access Key ID for reading and writing data to the S3 staging bucket.                                                                                         | string  |                  |
| `stagingBucket/awsSecretAccessKey`    | Secret Access Key        | AWS Secret Access Key for reading and writing data to the S3 staging bucket.                                                                                     | string  |                  |
| `stagingBucket/region`                | S3 Bucket Region         | Region of the S3 staging bucket.                                                                                                                                 | string  |                  |
| `stagingBucket/bucketPathS3`          | Bucket Path              | A prefix that will be used to store objects in S3.                                                                                                               | string  |                  |
| `stagingBucket/bucketGCS`             | GCS Staging Bucket       | Name of the GCS bucket to use for staging data loads.                                                                                                            | string  |                  |
| `stagingBucket/credentialsJSON`       | Service Account JSON     | The JSON credentials of the service account to use for authorizing to the staging bucket.                                                                        | string  |                  |
| `stagingBucket/gcsHMACAccessID`       | HMAC Access ID           | HMAC access ID for the service account.                                                                                                                          | string  |                  |
| `stagingBucket/gcsHMACSecret`         | HMAC Secret              | HMAC secret for the service account.                                                                                                                             | string  |                  |
| `stagingBucket/bucketPathGCS`         | S3 Bucket Region         | An optional prefix that will be used to store objects in the GCS staging bucket.                                                                                 | string  |                  |

#### Bindings

| Property         | Title              | Description                                                                                                   | Type    | Required/Default |
|------------------|--------------------|---------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/table`**     | Table              | Name of the database table.                                                                                   | string  | Required         |
| `/delta_updates` | Delta Update       | Should updates to this table be done via delta updates.                                                       | boolean |                  |
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
          stagingBucket:
            stagingBucketType: S3
            bucketS3: my_bucket
            awsAccessKeyId: <access_key_id>
            awsSecretAccessKey: <secret_access_key>
            region: us-east-1
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Sync Schedule

This connector supports configuring a schedule for sync frequency. You can read
about how to configure this [here](../../materialization-sync-schedule.md).

## Delta updates

This connector supports both standard (merge) and [delta
updates](../../../concepts/materialization.md#delta-updates). The default is to
use standard updates.

Enabling delta updates will prevent Flow from querying for documents in your
MotherDuck table, which can reduce latency and costs for large datasets. If you're
certain that all events will have unique keys, enabling delta updates is a
simple way to improve performance with no effect on the output. However,
enabling delta updates is not suitable for all workflows, as the resulting table
in MotherDuck won't be fully reduced.

You can enable delta updates on a per-binding basis:

```yaml
    bindings:
  	- resource:
      	table: ${table_name}
        delta_updates: true
    source: ${PREFIX}/${COLLECTION_NAME}
```