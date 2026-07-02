---
description: Materialize Estuary data collection into Amazon Simple Notification Service (SNS) topics in this delta-updates connector.
---

# Amazon SNS

This connector materializes Estuary collections into topics in Amazon Simple Notification Service, or SNS.

## Prerequisites

To use this connector, you'll need:

* AWS credentials. One of the following types:
   * The AWS **access key** and **secret access key** for the user. See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.
   * To authenticate using an AWS Role, you'll need the **region** and the **role arn**. Follow the steps in the [AWS IAM guide](/guides/iam-auth/aws.md) to set up the role.
* At least one Estuary collection to materialize.

## Configuration

To use this connector, begin with data in one or more Estuary collections.
Use the properties below to configure an Amazon SNS materialization.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/region`** | AWS Region | Region of the SNS service. | string | Required |
| **`/credentials`** | Authentication | Credentials for authentication. | [Credentials](#credentials) | Required |
| `/advanced/endpoint` | AWS Endpoint | Override the AWS endpoint URL. Used to direct requests at a compatible API such as LocalStack. | string |  |

#### Credentials

Credentials for authenticating with AWS. Use one of the following sets of options:

**Access Key**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/auth_type`** | Auth Type | Method to use for authentication. | string | Required: `AWSAccessKey` |
| **`/credentials/aws_access_key_id`** | AWS Access Key ID | AWS Access Key ID for publishing to SNS. | string | Required |
| **`/credentials/aws_secret_access_key`** | AWS Secret Access Key | AWS Secret Access Key for publishing to SNS. | string | Required |

**AWS IAM**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/auth_type`** | Auth Type | Method to use for authentication. | string | Required: `AWSIAM` |
| **`/credentials/aws_role_arn`** | AWS Role ARN | IAM Role to assume. | string | Required |
| **`/credentials/aws_region`** | AWS Region | AWS Region to authenticate in. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/topic_name`** | Topic Name | Name of the SNS topic to publish to (without the ARN prefix). FIFO topics must end in `.fifo`. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${MATERIALIZATION_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-sns:v1
        config:
          region: us-east-1
          credentials:
            auth_type: AWSAccessKey
            aws_access_key_id: example-aws-access-key-id
            aws_secret_access_key: example-aws-secret-access-key
    bindings:
      - resource:
          topic_name: orders
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta updates

Because SNS is a write-only event-streaming system, it has no concept of stored rows or keys.
This connector only uses [delta updates](/concepts/materialization/#delta-updates) rather than merge updates.
Every document in the source collection results in one event published to the topic.
