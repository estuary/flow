---
sidebar_position: 1
---
# Amazon Kinesis

This connector captures data from Amazon Kinesis streams.


[`ghcr.io/estuary/source-kinesis:dev`](https://github.com/estuary/connectors/pkgs/container/source-kinesis) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

You'll need one or more Amazon Kinesis streams. For a given capture, all streams must:

* Contain JSON data only
* Be accessible from a single root user or [IAM user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html) in AWS
* Be in the same [AWS region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions)

You'll also need the AWS **access key** and **secret access key** for the user.
See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.

:::info Beta
Your root or IAM user in AWS must have appropriate [permissions](https://aws.amazon.com/iam/features/manage-permissions/).
Additional details will be added to this article soon.
In the meantime, you can [contact Estuary Support](mailto:support@estuary.dev) if you encounter unexpected behavior.
:::

## Configuration

There are various ways to configure connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and YAML sample below provide configuration details specific to the Amazon Kinesis source connector.

### Values

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/awsAccessKeyId`** | AWS Access Key ID | Part of the AWS credentials that will be used to connect to Kinesis | string | Required, `"example-aws-access-key-id"` |
| **`/awsSecretAccessKey`** | AWS Secret Access Key | Part of the AWS credentials that will be used to connect to Kinesis | string | Required, `"example-aws-secret-access-key"` |
| `/endpoint` | AWS Endpoint | The AWS endpoint URI to connect to, useful if you&#x27;re capturing from a kinesis-compatible API that isn&#x27;t provided by AWS | string |  |
| **`/region`** | AWS Region | The name of the AWS region where the Kinesis stream is located | string | Required, `"us-east-1"` |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| **`/stream`** | Stream | Stream name | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental` | string | Required |

### Sample

A minimal capture definition within the catalog spec will look like the following:

```yaml
captures:
  ${TENANT}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-kinesis:dev
        config:
          awsAccessKeyId: "example-aws-access-key-id"
          awsSecretAccessKey: "example-aws-secret-access-key"
          region: "us-east-1"
    bindings:
      - resource:
          stream: ${STREAM_NAME}
          syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}

```

Your capture definition will likely be more complex, with additional bindings for each Kinesis stream.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures).