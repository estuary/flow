---
description: This connector materializes delta updates of Estuary collections into files in an S3 bucket per the CSV format described in RFC-4180.
---

# CSV Files in Amazon S3

This connector materializes [delta updates](/concepts/materialization/#delta-updates) of
Estuary collections into files in an S3 bucket per the CSV format described in
[RFC-4180](https://www.rfc-editor.org/rfc/rfc4180.html). The CSV files are compressed using Gzip
compression and written to S3 as `.csv.gz` files.

The delta updates are batched within Estuary, converted to CSV files, and then pushed to the S3 bucket
at a time interval that you set. Files are limited to a configurable maximum size. Each materialized
Estuary collection will produce many separate files.

## Prerequisites

To use this connector, you'll need:

* An S3 bucket to write files to. See [this
  guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) for
  instructions on setting up a new S3 bucket.
* An AWS root, IAM user or role with the
  [`s3:PutObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) permission
  for the S3 bucket.

  When authenticating as user, you'll need the **access key** and **secret access key**. See the
  [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding
  these credentials.  When authenticating using a role, you'll need the **region** and the **role
  arn**.  Follow the steps in the [AWS IAM guide](/guides/iam-auth/aws.md) to setup the role.


## Configuration

Use the below properties to configure the materialization, which will direct one or more of your
Estuary collections to your bucket.

### Properties

#### Endpoint

| Property                             | Title                 | Description                                                                                                                                   | Type    | Required/Default |
|--------------------------------------|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/bucket`**                        | Bucket                | Bucket to store materialized objects.                                                                                                         | string  | Required         |
| **`/region`**                        | Region                | Region of the bucket to write to.                                                                                                             | string  | Required         |
| **`/uploadInterval`**                | Upload Interval       | Frequency at which files will be uploaded.                                                                                                    | string  | 5m               |
| **`/credentials/auth_type`**         | Auth Type             | Method to use for authentication.  Must be set to either AWSAccessKey or AWSIAM.                                                              | string  | AWSAccessKey     |
| `/credentials/awsAccessKeyId`        | AWS Access Key ID     | Access Key ID for writing data to the bucket.  Required when using the `AWSAccessKey` auth type.                                              | string  |                  |
| `/credentials/awsSecretAccessKey`    | AWS Secret Access key | Secret Access Key for writing data to the bucket.  Required when using the `AWSAccessKey` auth type.                                          | string  |                  |
| `/credentials/aws_role_arn`          | AWS Role ARN          | Role to assume for writing data to the bucket.  Required when using the `AWSIAM` auth type.                                                   | string  |                  |
| `/credentials/aws_region`            | Region                | Region of the bucket to write to.  Required when using the `AWSIAM` auth type.                                                                | string  |                  |
| `/prefix`                            | Prefix                | Optional prefix that will be used to store objects.  May contain [date patterns](#date-patterns).                                             | string  |                  |
| `/fileSizeLimit`                     | File Size Limit       | Approximate maximum size of materialized files in bytes. Defaults to 10737418240 (10 GiB) if blank.                                           | integer |                  |
| `/endpoint`                          | Custom S3 Endpoint    | The S3 endpoint URI to connect to. Use if you're materializing to a compatible API that isn't provided by AWS. Should normally be left blank. | string  |                  |
| `/csvConfig/skipHeaders`             | Skip Headers          | Do not write headers to files.                                                                                                                | integer |                  |

#### Bindings

| Property    | Title | Description                                    | Type   | Required/Default |
|-------------|-------|------------------------------------------------|--------|------------------|
| **`/path`** | Path  | The path that objects will be materialized to. | string | Required         |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/materialize-s3-csv:v1"
        config:
          bucket: bucket
          awsAccessKeyId: <access_key_id>
          awsSecretAccessKey: <secret_access_key>
          region: us-east-2
          uploadInterval: 5m
    bindings:
      - resource:
          path: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## File Names

Materialized files are named with monotonically increasing integer values, padded with leading 0's
so they remain lexically sortable. For example, a set of files may be materialized like this for a
given collection:

```
bucket/prefix/path/v0000000000/00000000000000000000.csv.gz
bucket/prefix/path/v0000000000/00000000000000000001.csv.gz
bucket/prefix/path/v0000000000/00000000000000000002.csv.gz
```

Here the values for **bucket** and **prefix** are from your endpoint configuration. The **path** is
specific to the binding configuration. **v0000000000** represents the current **backfill counter**
for binding and will be increased if the binding is re-backfilled, along with the file names
starting back over from 0.

### Date Patterns

The **prefix** option of the endpoint configuration can contain patterns that
are expanded using the time of the start of the transaction.

:::note The transaction time is always represented as UTC.
:::

The following patterns are available:
- `%Y`: The year as a 4-digit number. (2025, 2026)
- `%m`: The month as a 2-digit number. (01, 02, ..., 12)
- `%d`: The day as a 2-digit number. (01, 02, ..., 31)
- `%H`: The hour as a 2-digit number with a 24-hour clock. (01, 02, ..., 23)
- `%M`: The minute as a 2-digit number. (01, 02, ..., 59)
- `%S`: The second as a 2-digit number. (01, 02, ..., 59)
- `%Z`: The timezone abbreviation. (UTC)
- `%z`: The timezone as an HHMM offset. (+0000)

## Multipart Upload Cleanup

This materialization uses S3 multipart uploads to ensure exactly-once
semantics.  If the materialization shard is interrupted while processing a
transaction and the transaction must be re-started, there may be incomplete
multipart uploads left behind.

As a best practice it is recommended to add a [lifecycle rule to the bucket to
automatically remove incompleted uploads][abort-lifecycle].  A 1-day or greater
delay removing incomplete multipart uploads will be sufficient for the current
transaction to complete.

[abort-lifecycle]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html
