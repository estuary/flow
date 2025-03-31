

# Amazon SQS

This connector captures data from Amazon Simple Queue Service (SQS) into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-amazon-sqs:dev`](https://ghcr.io/estuary/source-amazon-sqs:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites
* AWS IAM Access Key
* AWS IAM Secret Key
* AWS SQS Queue

## Setup
Follow these steps to set up the Amazon SQS connector:

1. [Create AWS IAM Keys](https://aws.amazon.com/premiumsupport/knowledge-center/create-access-key/)
2. [Create an SQS Queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-getting-started.html#step-create-queue)
3. Enter a Primary Key and Cursor Field using the standard form editor.  Note that these values currently have to be a string or timestamp.

:::note

**If Delete Messages After Read is false**, the IAM User only requires the `sqs:ReceiveMessage` permission in the AWS IAM Policy.
**If Delete Messages After Read is true**, both `sqs:ReceiveMessage` and `sqs:DeleteMessage` permissions are needed in the AWS IAM Policy.

:::

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the AmazonSQS source connector.

### Properties

#### Endpoint
| Property           | Title                      | Description                                                       | Type    | Required/Default |
| ------------------ | -------------------------- | ----------------------------------------------------------------- | ------- | ---------------- |
| `/queue_url`       | Queue URL                  | URL of the SQS Queue                                              | string  | Required         |
| `/region`          | AWS Region                 | AWS Region of the SQS Queue                                       | string  | Required         |
| `/access_key`      | AWS IAM Access Key ID      | The Access Key ID of the AWS IAM Role to use for pulling messages | string  |                  |
| `/secret_key`      | AWS IAM Secret Key         | The Secret Key of the AWS IAM Role to use for pulling messages    | string  |                  |
| `/delete_messages` | Delete Messages After Read | Delete messages from the SQS Queue after reading them             | boolean | Required         |

#### Bindings

| Property        | Title     | Description                                                              | Type   | Required/Default |
| --------------- | --------- | ------------------------------------------------------------------------ | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your Amazon SQS project from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                       | string | Required         |

### Sample

```json
{
  "properties": {
    "queue_url": {
      "order": 0
    },
    "region": {
      "order": 1
    },
    "access_key": {
      "order": 2
    },
    "secret_key": {
      "order": 3
    },
    "delete_messages": {
      "order": 4
    }
  }
}
```

### Performance Considerations
Consider the following performance aspects:

* **Max Batch Size:** Set the maximum number of messages to consume in a single poll.
* **Max Wait Time:** Define the maximum time (in seconds) to poll for messages before committing a batch.
* **Message Visibility Timeout:** Determine how long a message should be hidden from other consumers after being read.


### Data Loss Warning
When enabling Delete Messages After Read, messages are deleted from the SQS Queue after being read. However, there is no guarantee that the downstream destination has committed or persisted the message. Exercise caution before enabling this option to avoid permanent message loss.
