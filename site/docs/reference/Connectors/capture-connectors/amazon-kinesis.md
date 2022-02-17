---
sidebar_position: 1
---
# Amazon Kinesis

This connector captures data from Amazon Kinesis streams.

`ghcr.io/estuary/source-kinesis:dev` provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites

Maybe: setup IAM user to securely generate access key and secret access key https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/get-set-up-for-amazon-ec2.html#create-an-iam-user

## Configuration

There are various ways to configure and implement connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and code sample below provide configuration details specific to the Amazon Kinesis source connector.

### Values

| Value | Name| Description | Type | Required/Default |
|---|---|---|---|---|
| `awsAccessKeyId` | AWS Access Key ID | Part of the AWS credentials that will be used to connect to Kinesis | string | Required |
| `awsSecretAccessKey`| AWS Secret Access Key | Part of the AWS credentials that will be used to connect to Kinesis | string | Required |
| `endpoint` | AWS Endpoint | The AWS endpoint URI to connect to, useful if you're capturing from a kinesis-compatible API that isn't provided by AWS | string | |
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
          endpoint: "https://example-endpoint.amazonaws.com"
          region: "us-east-1"
    bindings:
      - resource:
          namespace: ${STREAM_NAMESPACE} #maybe delete this Olivia!!!!!!
          stream: ${STREAM_NAME}
          syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}

```