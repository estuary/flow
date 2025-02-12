---
description: This connector materializes delta updates of Flow collections into Apache Iceberg tables using Amazon S3 for object storage and AWS Glue as the Iceberg catalog.
---

import ReactPlayer from "react-player";

# Apache Iceberg Tables in Amazon S3

This connector materializes [delta updates](../../../concepts/materialization.md#delta-updates) of
Flow collections into Apache Iceberg tables using Amazon S3 for object storage and [AWS
Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) as
the Iceberg catalog.

The delta updates are batched within Flow, converted to parquet files, and then append to Iceberg
tables at a time interval that you set.

[`ghcr.io/estuary/materialize-s3-iceberg:dev`](https://ghcr.io/estuary/materialize-s3-iceberg:dev)
provides the latest connector image. You can also follow the link in your browser to see past image
versions.

<ReactPlayer controls url="https://www.youtube.com/watch?v=s0kGGp17pBg" />

## Prerequisites

To use this connector, you'll need:

* An S3 bucket to write files to. See [this
  guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) for
  instructions on setting up a new S3 bucket.
- An AWS root or IAM user with [read and write
  access](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html)
  to the S3 bucket. For this user, you'll need the **access key** and **secret
  access key**. See the [AWS
  blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for
  help finding these credentials.

If using the **AWS Glue Catalog**:

- The AWS root or IAM user must have access to AWS Glue. See [this
  guide](https://docs.aws.amazon.com/glue/latest/dg/set-up-iam.html) for
  instructions on setting up IAM permissions for a user to access AWS Glue.

If using the **REST Catalog**:

- The URI for connecting to the catalog.
- The name of the warehouse to connect to.
- Credentials for connecting to the catalog.

## Configuration

Use the below properties to configure the materialization, which will direct one or more of your
Flow collections to your tables.

### Properties

#### Endpoint

| Property                     | Title                 | Description                                                                                                 | Type   | Required/Default |
|------------------------------|-----------------------|-------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/aws_access_key_id`**     | AWS Access Key ID     | Access Key ID for accessing AWS services.                                                                   | string | Required         |
| **`/aws_secret_access_key`** | AWS Secret Access key | Secret Access Key for accessing AWS services.                                                               | string | Required         |
| **`/bucket`**                | Bucket                | The S3 bucket to write data files to.                                                                       | string | Required         |
| `/prefix`                    | Prefix                | Optional prefix that will be used to store objects.                                                         | string |                  |
| **`/region`**                | Region                | AWS Region.                                                                                                 | string | Required         |
| **`/namespace`**             | Namespace             | Namespace for bound collection tables (unless overridden within the binding resource configuration).        | string | Required         |
| `/upload_interval`           | Upload Interval       | Frequency at which files will be uploaded. Must be a valid ISO8601 duration string no greater than 4 hours. | string | PT5M             |
| `/upload_interval`           | Upload Interval       | Frequency at which files will be uploaded. Must be a valid ISO8601 duration string no greater than 4 hours. | string | PT5M             |
| **`/catalog/catalog_type`**  | Catalog Type          | Either "Iceberg REST Server" or "AWS Glue".                                                                 | string | Required         |
| **`/catalog/uri`**           | URI                   | URI identifying the REST catalog, in the format of 'https://yourserver.com/catalog'.                        | string | Required         |
| `/catalog/credential`        | Credential            | Credential for connecting to the REST catalog.                                                              | string |                  |
| `/catalog/token`             | Token                 | Token for connecting to the TEST catalog.                                                                   | string |                  |
| **`/catalog/warehouse`**     | Warehouse             | Warehouse to connect to in the REST catalog.                                                                | string | Required         |


#### Bindings

| Property         | Title                 | Description                                                                                                   | Type   | Required/Default |
|------------------|-----------------------|---------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/table`**     | Table                 | Name of the database table.                                                                                   | string | Required         |
| `/namespace`     | Alternative Namespace | Alternative namespace for this table (optional).                                                              | string |                  |
| `/delta_updates` | Delta Updates         | Should updates to this table be done via delta updates. Currently this connector only supports delta updates. | bool   | true             |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/materialize-s3-iceberg:dev"
        config:
          aws_access_key_id: <access_key_id>
          aws_secret_access_key: <secret_access_key>
          bucket: bucket
          region: us-east-2
          namespace: namespace
          upload_interval: PT5M
    bindings:
      - resource:
          table: ${COLLECTION_NAME}
          delta_updates: true
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Iceberg Column Types

Flow collection fields are written to Iceberg table columns based on the data type of the field.
Iceberg [V2 primitive type](https://iceberg.apache.org/spec/#primitive-types) columns are created
for these Flow collection fields:

| Collection Field Data Type                  | Iceberg Column Type                          |
|---------------------------------------------|----------------------------------------------|
| **array**                                   | **string**                                   |
| **object**                                  | **string**                                   |
| **boolean**                                 | **boolean**                                  |
| **integer**                                 | **long**                                     |
| **number**                                  | **double**                                   |
| **string** with `{contentEncoding: base64}` | **binary**                                   |
| **string** with `{format: date-time}`       | **timestamptz** (with microsecond precision) |
| **string** with `{format: date}`            | **date**                                     |
| **string** with `{format: integer}`         | **long**                                     |
| **string** with `{format: number}`          | **double**                                   |
| **string** (all others)                     | **string**                                   |

Flow collection fields with `{type: string, format: time}` and `{type: string, format: uuid}` are
materialized as **string** columns rather than **time** and **uuid** columns for compatibility with
Apache Spark. **[Nested types](https://iceberg.apache.org/spec/#nested-types)** are not currently
supported.

## Table Maintenance

To ensure optimal query performance, you should conduct [regular
maintenance](https://iceberg.apache.org/docs/latest/maintenance/) for your materialized tables since
the connector will not perform this maintenance automatically (support for automatic table
maintenance is planned).

If you're using the AWS Glue catalog, you can enable automatic data file compaction by following
[this guide](https://docs.aws.amazon.com/lake-formation/latest/dg/data-compaction.html).

## At-Least-Once Semantics

In rare cases, it may be possible for documents from a source collection to be appended to a target
table more than once. Users of materialized tables should take this possibility into consideration
when querying these tables.