---
sidebar_position: 1
---

# Apache Parquet in S3

This connector materializes [delta updates](#delta-updates) of Flow collections into an S3 bucket in the Apache Parquet format.

The delta updates are batched within Flow, converted to Parquet files, and the pushed to the S3 bucket at a time interval that you set.

[`ghcr.io/estuary/materialize-s3-parquet:dev`](https://ghcr.io/estuary/materialize-s3-parquet:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* An AWS root or IAM user with access to the S3 bucket. For this user, you'll need the **access key** and **secret access key**.
  See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.
* At least one Flow collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a  materialization, which will direct the contents of these Flow collections to Parquet files in S3.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/awsAccessKeyId`** | AWS Access Key ID | AWS credential used to connect to S3. | string | Required |
| **`/awsSecretAccessKey`** | AWS Secret Access Key | AWS credential used to connect to S3. | string | Required |
| **`/bucket`** | Bucket | Name of the S3 bucket. | string | Required |
| `/endpoint` | AWS Endpoint | The AWS endpoint URI to connect to, useful if you&#x27;re capturing from a S3-compatible API that isn&#x27;t provided by AWS | string. |  |
| `/region` |  AWS Region | The name of the AWS region where the S3 bucket is located. &quot;us-east-1&quot; is a popular default you can try, if you&#x27;re unsure what to put here. | string |  |
| **`/uploadIntervalInSeconds`** | Upload Interval in Seconds | Time interval, in seconds, at which to upload data from Flow to S3. | integer | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/compressionType` | Compression type | The method used to compress data in Parquet. | string |  |
| **`/pathPrefix`** | Path prefix | The desired Parquet file path within the bucket as determined by an S3 [prefix](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html). | string | Required |

The following compression types are supported:

* `snappy`
* `gzip`
* `lz4`
* `zstd`

### Sample
```yaml
materializations:
  tenant/mat_name:
	  endpoint:
        connector:
          config:
            awsAccessKeyId: AKIAIOSFODNN7EXAMPLE
            awsSecretAccessKey: wJalrXUtnFEMI/K7MDENG/bPxRfiCYSECRET
            bucket: my-bucket
            uploadIntervalInSeconds: 300
          # Path to the latest version of the connector, provided as a Docker image
          image: ghcr.io/estuary/materialize-s3-parquet:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
      - resource:
          pathPrefix: /my-prefix
      source: tenant/source_collection
```

## Delta updates

This connector uses only [delta updates](../../../concepts/materialization.md#delta-updates) mode.
Collection documents are converted to Parquet format and stored in their unmerged state.
