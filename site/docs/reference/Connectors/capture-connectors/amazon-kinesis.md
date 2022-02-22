---
sidebar_position: 1
---
# Amazon Kinesis

This connector captures data from Amazon Kinesis streams.

`ghcr.io/estuary/source-kinesis:dev` provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites

You'll need one or more Amazon Kinesis streams. For a given capture, all streams must:

* Contain JSON data only
* Be accessible from a single root user or [IAM user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html) in AWS
* Be in the same [AWS region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions)

You'll also need the AWS **access key** and **secret access key** for the user.
See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.

## Configuration

There are various ways to configure and implement connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and code sample below provide configuration details specific to the Amazon Kinesis source connector.

### Values

| Value | Name| Description | Type | Required/Default |
|---|---|---|---|---|
| `awsAccessKeyId` | AWS Access Key ID | AWS credential used to connect to Kinesis | string | Required |
| `awsSecretAccessKey`| AWS Secret Access Key | AWS credential used to connect to Kinesis | string | Required |
| `endpoint` | AWS Endpoint | The AWS endpoint URI to connect to. Useful if you're capturing from a kinesis-compatible API that isn't provided by AWS. | string | |
| `region` | AWS Region | The name of the AWS region where the Kinesis stream is located | string | `"us-east-1"`, Required |

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
          namespace: ${STREAM_NAMESPACE} #FOR REVIEW: does namespace matter?/What does it do here?
          stream: ${STREAM_NAME}
          syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}

```