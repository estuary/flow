

# Amazon DynamoDB

This connector materializes Estuary collections into tables in an Amazon DynamoDB.

It is available for use in the Estuary web application. For local development or open-source workflows,
[`ghcr.io/estuary/materialize-dynamodb:dev`](https://ghcr.io/estuary/materialize-dynamodb:dev)
provides the latest version of the connector as a Docker image. You can also follow the link in your
browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

- An IAM user with the following
  [permissions](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazondynamodb.html):
  - `BatchGetItem` on all resources
  - `BatchWriteItem` on all resources
  - `CreateTable` on all resources
  - `DescribeTable` on all resources

  These permissions should be specified with the `dynamodb:` prefix in an IAM policy document. For
  more details and examples, see [Using identity-based policies with Amazon
  DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/using-identity-based-policies.html)
  in the Amazon docs.

- AWS Credentials.  One of the following types:
  - The AWS **access key** and **secret access key** for the user. See the [AWS
    blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/)
    for help finding these credentials.
  - To authenticate using an AWS Role, you'll need the **region** and the
    **role arn**.  Follow the steps in the [AWS IAM
    guide](/guides/iam-auth/aws.md) to setup the role.

## Collection Requirements

Materialized collections can have at most 2 collection keys.

By default, the materialized tables will include the collection keys as the DynamoDB partition key
and sort key, and the root document. The root document is materialized as `"flow_document"` unless
an alternate [projection](../../../concepts/collections.md#projections) is configured for the source
collection. Additional fields may be included, but DynamoDB has a 400KB size limit on an individual
row so selecting too many fields of a collection with large documents will cause errors if the row
size exceeds that.

The root document is materialized as a DynamoDB `Map` type, and the fields of the document must be
valid DynamoDB `Map` keys.

To resolve issues with collections with more than 2 keys, excessively large documents, or
incompatible field names, use a [derivation](../../../concepts/derivations.md) to derive a new
collection and materialize that collection instead.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog
specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more
about using connectors. The values and specification sample below provide configuration details
specific to the DynamoDB materialization connector.

### Properties

#### Endpoint

| Property                  | Title             | Description                                                                                                     | Type   | Required/Default |
| ------------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/region`**             | AWS Region        | Region of the materialized tables.                                                                              | string | Required         |
| **`/credentials`**        | Credentials       | Credentials for authentication.                                                                                 | [Credentials](#credentials) | Required |
| `advanced/endpoint`       | AWS Endpoint      | The AWS endpoint URI to connect to. Use if you're materializing to a compatible API that isn't provided by AWS. | string |                  |

#### Credentials

Credentials for authenticating with AWS.  Use one of the following sets of options:

| Property                                 | Title                   | Description                                                                                               | Type    | Required/Default         |
| ---------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------------------- | ------- | ------------------------ |
| **`/credentials/auth_type`**             | Auth Type               | Method to use for authentication.                                                                         | string  | Required: `AWSAccessKey` |
| **`/credentials/aws_access_key_id`**     | AWS Access Key ID       | AWS Access Key ID for materializing to DynamoDB.                                                          | string  | Required                 |
| **`/credentials/aws_secret_access_key`** | AWS Secret Access key   | AWS Secret Access Key for materializing to DynamoDB.                                                      | string  | Required                 |

| Property                                 | Title                   | Description                                                                                               | Type    | Required/Default   |
| ---------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------------------- | ------- | ------------------ |
| **`/credentials/auth_type`**             | Auth Type               | Method to use for authentication.                                                                         | string  | Required: `AWSIAM` |
| **`/credentials/aws_role_arn`**          | AWS Role ARN            | IAM Role to assume.                                                                                       | string  | Required           |
| **`/credentials/aws_region`**            | AWS Region              | AWS Region to authenticate in.                                                                            | string  | Required           |

#### Bindings

| Property         | Title        | Description                                                               | Type    | Required/Default |
| ---------------- | ------------ | ------------------------------------------------------------------------- | ------- | ---------------- |
| **`/table`**     | Table Name   | The name of the table to be materialized to.                              | string  | Required         |

### Sample

```yaml
materializations:
  ${PREFIX}/${MATERIALIZATION_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-dynamodb:dev
        config:
          credentials:
            auth_type: "AWSAccessKey"
            aws_access_key_id: "example-aws-access-key-id"
            aws_secret_access_key: "example-aws-secret-access-key"
          region: "us-east-1"
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
