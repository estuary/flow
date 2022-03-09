---
sidebar_position: 2
---
# Amazon S3

This connector captures data from an Amazon S3 bucket.

[`ghcr.io/estuary/source-s3:dev`](https://ghcr.io/estuary/source-s3:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, either your S3 bucket must be public,
or you must have access via a root or [IAM user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html).

* For public buckets, verify that the [access policy](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-overview.html#access-control-resources-manage-permissions-basics) allows anonymous reads.
* For buckets accessed by a user account, you'll need the AWS **access key** and **secret access key** for the user.
See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.

## Configuration

There are various ways to configure connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and YAML sample in this section provide configuration details specific to the Amazon S3 source connector.

:::tip
You might organize your S3 bucket using [prefixes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html) to emulate a directory structure.
This connector can use prefixes in two ways: first, to perform the [**discovery**](../../../concepts/connectors.md#flowctl-discover) phase of setup, and later, when the capture is running.

* You can specify a prefix in the endpoint configuration to limit the overall scope of data discovery.
* You're required to specify prefixes on a per-binding basis. This allows you to map each prefix to a distinct Flow collection,
and informs how the capture will behave in production.

To capture the entire bucket, omit `prefix` in the endpoint configuration and set `stream` to the name of the bucket.
:::

### Values

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/ascendingKeys` | Ascending Keys | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. | boolean | `false` |
| `/awsAccessKeyId` | AWS Access Key ID | Part of the AWS credentials that will be used to connect to S3. Required unless the bucket is public and allows anonymous listings and reads. | string | `"example-aws-access-key-id"` |
| `/awsSecretAccessKey` | AWS Secret Access Key | Part of the AWS credentials that will be used to connect to S3. Required unless the bucket is public and allows anonymous listings and reads. | string | `"example-aws-secret-access-key"` |
| **`/bucket`** | Bucket | Name of the S3 bucket | string | Required |
| `/endpoint` | AWS Endpoint | The AWS endpoint URI to connect to, useful if you&#x27;re capturing from a S3-compatible API that isn&#x27;t provided by AWS | string |  |
| `/matchKeys` | Match Keys | Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use &quot;.&#x2A;&#x5C;.json&quot; to only capture json files. | string |  |
| `/prefix` | Prefix | Prefix within the bucket to capture from. | string |  |
| **`/region`** | AWS Region | The name of the AWS region where the S3 bucket is located. &quot;us-east-1&quot; is a popular default you can try, if you&#x27;re unsure what to put here. | string | Required, `"us-east-1"` |

#### Bindings

| Property | Title| Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Prefix | Path to dataset in the bucket, formatted as `bucket-name/prefix-name` | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental` | string | Required |

### Sample

A minimal capture definition within the catalog spec will look like the following:

```yaml
captures:
  ${TENANT}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-s3:dev
        config:
          bucket: "my-bucket"
          region: "us-east-1"
    bindings:
      - resource:
          stream: my-bucket/${PREFIX}
          syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}

```

Your capture definition may be more complex, with additional bindings for different S3 prefixes within the same bucket.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)