
import ReactPlayer from "react-player";

# MotherDuck

This connector materializes Estuary collections into tables in a MotherDuck database.

The connector uses a supported object storage service to materialize to
MotherDuck tables. You can choose from S3 or S3-compatible, GCS, Azure Blob
Storage. The files in storage are used as a temporary staging area for data
storage and retrieval.

[`ghcr.io/estuary/materialize-motherduck:dev`](https://ghcr.io/estuary/materialize-motherduck:dev)
provides the latest connector image. You can also follow the link in your browser to see past image
versions.

<ReactPlayer controls url="https://www.youtube.com/watch?v=2flyH-rjmqI" />

## Prerequisites

To use this connector, you'll need:

* A [MotherDuck](https://motherduck.com/) account and [Service
  Token](https://motherduck.com/docs/authenticating-to-motherduck#fetching-the-service-token).
* An S3 bucket for staging temporary files, or a GCS bucket for staging
  temporary files.  Cloudflare R2 can also be used via its S3-compatible API.
  An S3 bucket in `us-east-1` is recommended for best performance and costs,
  since MotherDuck is currently hosted in that region.

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

To use a Cloudflare R2 bucket for staging temporary files:
* Create a new bucket following [this guide][r2-create-buckets].
* Create an [API token][r2-api-tokens] with read and write permission to the
  bucket.  Make sure to take note of the credentials for S3 clients, you will
  need the `Access Key ID` and `Secret Access Key`.
* Configure the connector to use S3 for the staging bucket, and set the
  `endpoint` to the S3 API URL from the R2 object storage overview page.  You
  can set the region to `auto` as this value is not used by R2.

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

To use Azure Blob Storage for staging temporary files:
* Create or select a storage account.
* Create a blob container.
* Use the access keys listed listed under "Security + networking" for authentication.

## Configuration

Use the below properties to configure MotherDuck materialization, which will direct one or
more of your Estuary collections to your desired tables in the database.

### Properties

#### Endpoint

| Property                              | Title                    | Description                                                                                                                                                      | Type    | Required/Default |
|---------------------------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/token`**                          | MotherDuck Service Token | Service token for authenticating with MotherDuck.                                                                                                                | string  | Required         |
| **`/database`**                       | Database                 | The database to materialize to.                                                                                                                                  | string  | Required         |
| **`/schema`**                         | Database Schema          | Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables. | string  | Required         |
| `/hardDelete`                         | Hard Delete              | If enabled, items deleted in the source will also be deleted from the destination.                                                                               | boolean | `false`          |
| **`/stagingBucket`**                  | Staging Bucket           | The type of staging bucket to use.                                                                                                    | [Staging Bucket](#staging-bucket)  | Required         |

#### Staging Bucket

| Property                    | Title                | Description                                                                               | Type   | Required/Default  |
| --------------------------- | -------------------- | ----------------------------------------------------------------------------------------- | ------ | ----------------- |
| **`/stagingBucketType`**    | Staging Bucket Type  | Use `S3` to stage files in S3 or compatible storage.                                      | string | Required: `S3`    |
| **`/bucketS3`**             | S3 Staging Bucket    | Name of the S3 bucket to use for staging data loads. Must not contain dots (.)            | string | Required          |
| **`/awsAccessKeyId`**       | Access Key ID        | AWS Access Key ID for reading and writing data to the S3 staging bucket.                  | string | Required          |
| **`/awsSecretAccessKey`**   | Secret Access Key    | AWS Secret Access Key for reading and writing data to the S3 staging bucket.              | string | Required          |
| **`/region`**               | S3 Bucket Region     | Region of the S3 staging bucket.                                                          | string | Required          |
| `/bucketPathS3`             | Bucket Path          | A prefix that will be used to store objects in S3.                                        | string |                   |
| `/endpoint`                 | Custom Endpoint      | Custom endpoint for S3-compatible storage.                                                | string |                   |

| Property                    | Title                | Description                                                                               | Type   | Required/Default  |
| --------------------------- | -------------------- | ----------------------------------------------------------------------------------------- | ------ | ----------------- |
| **`/stagingBucketType`**    | Staging Bucket Type  | Use `GCS` to stage files in GCS                                                           | string | Required: `GCS`   |
| **`/bucketGCS`**            | GCS Staging Bucket   | Name of the GCS bucket to use for staging data loads.                                     | string | Required          |
| **`/credentialsJSON`**      | Service Account JSON | The JSON credentials of the service account to use for authorizing to the staging bucket. | string | Required          |
| **`/gcsHMACAccessID`**      | HMAC Access ID       | HMAC access ID for the service account.                                                   | string | Required          |
| **`/gcsHMACSecret`**        | HMAC Secret          | HMAC secret for the service account.                                                      | string | Required          |
| `/bucketPathGCS`            | S3 Bucket Region     | An optional prefix that will be used to store objects in the GCS staging bucket.          | string |                   |

| Property                    | Title                | Description                                                                               | Type   | Required/Default  |
| --------------------------- | -------------------- | ----------------------------------------------------------------------------------------- | ------ | ----------------- |
| **`/stagingBucketType`**    | Staging Bucket Type  | Use `Azure` to stage files in Azure.                                                      | string | Required: `Azure` |
| **`/storageAccountName`**   | Storage Account Name | Name of the Azure storage account.                                                        | string | Required          |
| **`/storageAccountKey`**    | Storage Account Key  | Storage account key for authentication.                                                   | string | Required          |
| **`/containerName`**        | Container Name       | Name of the Azure Blob container to use for staging data loads.                           | string | Required          |
| `/bucketPathAzure`          | Bucket Path          | An optional prefix that will be used to store objects in the staging container.           | string | Required          |

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
about how to configure this [here](/reference/materialization-sync-schedule).

## Delta updates

This connector supports both standard (merge) and [delta
updates](/concepts/materialization/#delta-updates). The default is to
use standard updates.

Enabling delta updates will prevent Estuary from querying for documents in your
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

[r2-create-buckets]: https://developers.cloudflare.com/r2/buckets/create-buckets/
[r2-api-tokens]: https://developers.cloudflare.com/r2/api/tokens/
