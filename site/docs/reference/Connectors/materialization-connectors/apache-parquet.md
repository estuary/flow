

# Apache Parquet in S3

This connector materializes [delta updates](#delta-updates) of Flow collections into an S3 bucket in the Apache Parquet format.

The delta updates are batched within Flow, converted to Parquet files, and then pushed to the S3 bucket at a time interval that you set.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-s3-parquet:dev`](https://ghcr.io/estuary/materialize-s3-parquet:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported field types

All possible field types in Flow collections are materialized into Parquet by default, with the exception of arrays.
By default, the connector makes its best effort to flatten fields of type object.

[You can override the default and materialize arrays and objects as JSON strings](#materializing-arrays-and-objects).

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
| `/advanced` |  | Options for advanced users. You should not typically need to modify these. | object |  |
| `/advanced/endpoint` | Endpoint | The endpoint URI to connect to. Useful if you&#x27;re connecting to a S3-compatible API that isn&#x27;t provided by AWS. | string |  |
| **`/awsAccessKeyId`** | Access Key ID | AWS credential used to connect to S3. | string | Required |
| **`/awsSecretAccessKey`** | Secret Access Key | AWS credential used to connect to S3. | string | Required |
| **`/bucket`** | Bucket | Name of the S3 bucket. | string | Required |
| **`/region`** | Region | The name of the AWS region where the S3 bucket is located. | string | Required |
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
  PREFIX/mat_name:
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
      source: PREFIX/source_collection
```

## Delta updates

This connector uses only [delta updates](../../../concepts/materialization.md#delta-updates) mode.
Collection documents are converted to Parquet format and stored in their unmerged state.

## Materializing arrays and objects

If your collection contains array or object fields, by default, the connector will:

* Skip arrays.
* Attempt to flatten objects.

Alternatively, you can materialize either of these field types as JSON strings.
You do so by editing the materialization specification and adding **projected fields**.

**Projections** are how Flow maps hierarchical JSON locations into fields.
By listing projected fields to include, you override the connector's default behavior.

[Learn more about how projected fields work](../../../concepts/materialization.md#projected-fields).

To materialize an array or object as a JSON string, do the following:

1. On the [collections page of the web app](https://dashboard.estuary.dev/collections),
locate the collection to be materialized and view its specification.
Note the names of arrays or objects you want to materialize as strings.

  For example, the collection `estuary/public/wikipedia/recentchange` (visible to all users in the web app)
  has many objects, but we want to materialize `"length"` and `"revision`" as strings.

2. Begin to set up your S3 Parquet materialization. After you initiate the connection with S3, the **Specification Editor** becomes available.

3. In the Specification Editor, locate the **binding** of the collection with the arrays or objects.

4. Add the `"fields"` object to the binding and list the objects or arrays in the following format:

```json
"bindings": [
    {
      "resource": {
        "pathPrefix": "recentchanges"
      },
      "source": "estuary/public/wikipedia/recentchange",
      "fields": {
        "include": {
          "length": {},
          "revision": {}
        },
        "recommended": true
      }
    }
  ],
```
5. Proceed to save and publish the materialization as usual.
