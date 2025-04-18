

# Amazon DynamoDB

This connector materializes Flow collections into tables in an Amazon DynamoDB.

It is available for use in the Flow web application. For local development or open-source workflows,
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

  These permissions should be specified with the `dynamodb:` prefix in an IAM policy document. For
  more details and examples, see [Using identity-based policies with Amazon
  DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/using-identity-based-policies.html)
  in the Amazon docs.

- The AWS **access key** and **secret access key** for the user. See the [AWS
  blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these
  credentials.

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

You configure connectors either in the Flow web app, or by directly editing the catalog
specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more
about using connectors. The values and specification sample below provide configuration details
specific to the DynamoDB materialization connector.

### Properties

#### Endpoint

| Property                  | Title             | Description                                                                                                     | Type   | Required/Default |
| ------------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/awsAccessKeyId`**     | Access Key ID     | AWS Access Key ID for materializing to DynamoDB.                                                                | string | Required         |
| **`/awsSecretAccessKey`** | Secret Access Key | AWS Secret Access Key for materializing to DynamoDB.                                                            | string | Required         |
| **`/region`**             | AWS Region        | Region of the materialized tables.                                                                              | string | Required         |
| `advanced/endpoint`       | AWS Endpoint      | The AWS endpoint URI to connect to. Use if you're materializing to a compatible API that isn't provided by AWS. | string |                  |

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
          awsAccessKeyId: "example-aws-access-key-id"
          awsSecretAccessKey: "example-aws-secret-access-key"
          region: "us-east-1"
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
