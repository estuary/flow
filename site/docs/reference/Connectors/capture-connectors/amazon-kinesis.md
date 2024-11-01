
# Amazon Kinesis

This connector captures data from Amazon Kinesis streams.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-kinesis:dev`](https://github.com/estuary/connectors/pkgs/container/source-kinesis) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* One or more Amazon Kinesis streams. For a given capture, all streams must:
  * Contain JSON data only
  * Be in the same [AWS region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions)

* An IAM user with the following [permissions](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonkinesis.html):
  * `ListShards` on all resources
  * `GetRecords` on all streams used
  * `GetShardIterator` on all streams used
  * `DescribeStream` on all streams used
  * `DescribeStreamSummary` on all streams used

  These permissions should be specified with the `kinesis:` prefix in an IAM policy document.
  For more details and examples, see [Controlling Access to Amazon Kinesis Data](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html) in the Amazon docs.

* The AWS **access key** and **secret access key** for the user.
See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Amazon Kinesis source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/awsAccessKeyId`** | AWS access key ID | Part of the AWS credentials that will be used to connect to Kinesis. | string | Required, `"example-aws-access-key-id"` |
| **`/awsSecretAccessKey`** | AWS secret access key | Part of the AWS credentials that will be used to connect to Kinesis. | string | Required, `"example-aws-secret-access-key"` |
| `/endpoint` | AWS endpoint | The AWS endpoint URI to connect to, useful if you&#x27;re capturing from a kinesis-compatible API that isn&#x27;t provided by AWS. | string |  |
| **`/region`** | AWS region | The name of the AWS region where the Kinesis stream is located. | string | Required, `"us-east-1"` |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| **`/stream`** | Stream | Stream name. | string | Required |

### Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
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
        target: ${PREFIX}/${COLLECTION_NAME}

```

Your capture definition will likely be more complex, with additional bindings for each Kinesis stream.

[Learn more about capture definitions.](../../../concepts/captures.md).
