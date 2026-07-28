---
description: Capture Amazon SQS messages into Estuary collections from standard and FIFO queues, using IAM access keys or an assumed role. Messages are deleted only after they are durably committed.
---

# Amazon SQS

This connector captures messages from Amazon SQS queues into Estuary collections. It supports both **standard and FIFO queues**.

Messages are deleted from the queue after Estuary has durably committed them.

:::warning
This capture consumes the queue. Once a message is committed to the
collection, the connector deletes it from the queue. If other consumers
share the queue, each message will be processed by either this capture or
the other consumer, not both. To fan the same messages out to multiple
consumers, publish through an SNS topic with one SQS queue per consumer.
:::

## Prerequisites

- One or more SQS queue URLs, for example `https://sqs.us-east-1.amazonaws.com/123456789012/my-queue`. All queues in a capture must be in the same AWS region.
- AWS credentials, either an IAM user's access key pair or an IAM role for Estuary to assume.
- The credentials must allow the following actions:
  - `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and `sqs:GetQueueAttributes` on each captured queue.
  - `sqs:ListQueues` in order to discover queues.

## Captured document structure

If a message body is a single JSON object, its fields become the top-level fields of the captured document. Any other body, such as plain text, a JSON array or scalar, or multiple concatenated JSON records, is captured whole as a string under a `body` field.

Message metadata is captured under `_meta`:

- `queueUrl`: the queue the message was received from
- `messageId`: the SQS-assigned unique message ID
- `sentTimestamp`: when the message was sent to the queue
- `approximateReceiveCount`: how many times the message has been received.
- `messageAttributes`: message attributes attached by the producer, if any
- `messageGroupId`, `sequenceNumber`, `deduplicationId` (FIFO queues only): message sequencing and deduplication metadata

### Discovered collections and keys

Discovery lists the queues in the configured region and proposes one binding per queue, keyed by queue type.

- **Standard queues** use the key `/_meta/messageId`. SQS assigns the ID at send time.
- **FIFO queues** use the key `/_meta/messageGroupId, /_meta/sequenceNumber`.

## FIFO ordering

For FIFO queues, messages within a message group are captured and committed in queue order. Because SQS locks a message group while its messages await commit, FIFO throughput scales with the number of *active message groups*.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Amazon SQS source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/region`** | AWS Region | AWS region of the SQS queues (e.g. us-east-1). | string | Required |
| **`/credentials`** | Authentication | AWS credentials, discriminated by `auth_type`. | object | Required |
| `/credentials/auth_type` | Authentication type | `AWSAccessKey` for an access key pair, or `AWSIAM` for an IAM role assumed by Estuary. | string | `"AWSAccessKey"` |
| `/credentials/aws_access_key_id` | AWS Access Key ID | Access key ID, when using `AWSAccessKey`. | string | |
| `/credentials/aws_secret_access_key` | AWS Secret Access Key | Secret access key, when using `AWSAccessKey`. | string | |
| `/credentials/aws_region` | AWS Region | Region of the role, when using `AWSIAM`. | string | |
| `/credentials/aws_role_arn` | AWS Role ARN | Role for Estuary to assume, when using `AWSIAM`. | string | |
| `/advanced/endpoint` | AWS Endpoint | The AWS endpoint URI to connect to. Use for VPC interface endpoints or SQS-compatible APIs not provided by AWS. | string | |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/queueUrl`** | Queue URL | Full URL of the SQS queue (e.g. `https://sqs.us-east-1.amazonaws.com/123456789012/my-queue`). | string | Required |

### Sample

Access key authentication:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sqs:v1
        config:
          region: us-east-1
          credentials:
            auth_type: AWSAccessKey
            aws_access_key_id: AKIA...
            aws_secret_access_key: <secret>
    bindings:
      - resource:
          queueUrl: https://sqs.us-east-1.amazonaws.com/123456789012/my-queue
        target: ${PREFIX}/${COLLECTION_NAME}
```

AWS IAM role authentication:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sqs:v1
        config:
          region: us-east-1
          credentials:
            auth_type: AWSIAM
            aws_region: us-east-1
            aws_role_arn: arn:aws:iam::123456789012:role/estuary-sqs-capture
    bindings:
      - resource:
          queueUrl: https://sqs.us-east-1.amazonaws.com/123456789012/my-queue
        target: ${PREFIX}/${COLLECTION_NAME}
```
