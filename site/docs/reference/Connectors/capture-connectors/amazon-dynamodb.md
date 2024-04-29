

# Amazon DynamoDB

This connector uses DynamoDB streams to continuously capture updates from DynamoDB tables into one or more Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-dynamodb:dev`](https://ghcr.io/estuary/source-dynamodb:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

- One or more DynamoDB tables with DynamoDB streams enabled. To enable DynamoDB streams for a table:
  1. Select the table in the AWS console
  2. Go to the **Exports and streams** tab
  3. Click **Turn on** in the DynamoDB stream details section
  4. Select **New and old images** for the **View type**
  5. Click **Turn on stream**

- An IAM user with the following [permissions](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazondynamodb.html):
  - `ListTables` on all resources
  - `DescribeTable` on all resources
  - `DescribeStream` on all resources
  - `Scan` on all tables used
  - `GetRecords` on all streams used
  - `GetShardIterator` on all streams used

  These permissions should be specified with the `dynamodb:` prefix in an IAM policy document. For more details and examples, see [Using identity-based policies with Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/using-identity-based-policies.html) in the Amazon docs.

- The AWS **access key** and **secret access key** for the user. See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the DynamoDB source connector.

### Properties

#### Endpoint

| Property                    | Title                   | Description                                                                                                   | Type    | Required/Default |
| --------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/awsAccessKeyId`**       | Access Key ID           | AWS Access Key ID for capturing from DynamoDB tables.                                                         | string  | Required         |
| **`/awsSecretAccessKey`**   | Secret Access Key       | AWS Secret Access Key for capturing from DynamoDB tables.                                                     | string  | Required         |
| **`/region`**               | AWS Region              | The name of the AWS region where the DynamoDB tables are located.                                             | string  | Required         |
| `advanced/backfillSegments` | Backfill Table Segments | Number of segments to use for backfill table scans. Has no effect if changed after the backfill has started.  | integer |                  |
| `advanced/endpoint`         | AWS Endpoint            | The AWS endpoint URI to connect to. Use if you're capturing from a compatible API that isn't provided by AWS. | string  |                  |
| `advanced/scanLimit`        | Scan Limit              | Limit the number of items to evaluate for each table backfill scan request.                                   | integer |                  |

#### Bindings

| Property         | Title          | Description                                                                                                                                                               | Type    | Required/Default |
| ---------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/table`**     | Table Name     | The name of the table to be captured.                                                                                                                                     | string  | Required         |
| `/rcuAllocation` | RCU Allocation | Read capacity units the capture will attempt to consume during the table backfill. Leave blank to automatically determine based on the provisioned capacity of the table. | integer |                  |

### Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-dynamodb:dev
        config:
          awsAccessKeyId: "example-aws-access-key-id"
          awsSecretAccessKey: "example-aws-secret-access-key"
          region: "us-east-1"
    bindings:
      - resource:
          table: ${TABLE_NAME}
        target: ${PREFIX}/${COLLECTION_NAME}
```

Your capture definition may be more complex, with additional bindings for each DynamoDB table.

[Learn more about capture definitions.](../../../concepts/captures.md#specification)
