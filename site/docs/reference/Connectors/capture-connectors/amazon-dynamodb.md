# Amazon DynamoDB

This connector uses DynamoDB streams to continuously capture updates from DynamoDB tables into one or more Estuary collections.

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/source-dynamodb:dev`](https://ghcr.io/estuary/source-dynamodb:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

- One or more DynamoDB tables with DynamoDB streams enabled. To enable DynamoDB streams for a table:
  1. Select the table in the AWS console
  2. Go to the **Exports and streams** tab
  3. Click **Turn on** in the DynamoDB stream details section
  4. Select **New and old images** for the **View type**
  5. Click **Turn on stream**

- An IAM user with the following [permissions](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazondynamodb.html):
  - `ListTables` on all tables in the region
  - `DescribeTable` on all tables in the region
  - `DescribeStream` on all tables used
  - `Scan` on all tables used
  - `GetRecords` on all streams used
  - `GetShardIterator` on all streams used

  These permissions should be specified with the `dynamodb:` prefix in an IAM policy document. For more details and examples, see [Using identity-based policies with Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/using-identity-based-policies.html) in the Amazon docs.

  **Example IAM Policy:**

  ```json
  {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Effect": "Allow",
              "Action": [
                  "dynamodb:DescribeTable",
                  "dynamodb:DescribeStream",
                  "dynamodb:GetRecords",
                  "dynamodb:GetShardIterator",
                  "dynamodb:ListStreams",
                  "dynamodb:ListTables",
                  "dynamodb:Scan",
                  "dynamodb:Query"
              ],
              "Resource": [
                  "arn:aws:dynamodb:<REGION>:<ACCOUNT_ID>:table/*",
                  "arn:aws:dynamodb:<REGION>:<ACCOUNT_ID>:table/*/stream/*"
              ]
          }
      ]
  }
  ```

  **Example IAM Policy for Specific Tables:**

  If you want to limit access to specific tables rather than all tables, use this policy. Note that `ListTables` and `DescribeTable` require the `table/*` resource pattern and cannot be scoped to specific tables.

  ```json
  {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Sid": "SpecificTableAccess",
              "Effect": "Allow",
              "Action": [
                  "dynamodb:DescribeStream",
                  "dynamodb:GetShardIterator",
                  "dynamodb:GetRecords",
                  "dynamodb:Scan"
              ],
              "Resource": [
                  "arn:aws:dynamodb:<REGION>:<ACCOUNT_ID>:table/<TABLE_NAME>",
                  "arn:aws:dynamodb:<REGION>:<ACCOUNT_ID>:table/<TABLE_NAME>/stream/*"
              ]
          },
          {
              "Sid": "DiscoveryAccess",
              "Effect": "Allow",
              "Action": [
                  "dynamodb:ListTables",
                  "dynamodb:DescribeTable"
              ],
              "Resource": "arn:aws:dynamodb:<REGION>:<ACCOUNT_ID>:table/*"
          }
      ]
  }
  ```

  **Terraform Example with IAM Role:**

  When using Terraform to create both the OIDC provider and IAM role, you may encounter a circular dependency since the OIDC provider needs the role ARN as its audience, but the role needs the OIDC provider ARN in its trust policy. Use `locals` to construct the role ARN before creating it:

  ```hcl
  data "aws_caller_identity" "current" {}

  locals {
    role_name = "EstuaryDynamoDB"
    role_arn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.role_name}"
  }

  resource "aws_iam_openid_connect_provider" "estuary" {
    url             = "https://openid.estuary.dev/1234567890abcdef.dp.estuary-data.com"
    client_id_list  = [local.role_arn]
    thumbprint_list = ["<THUMBPRINT>"]
  }

  resource "aws_iam_role" "estuary_dynamodb" {
    name = local.role_name

    assume_role_policy = jsonencode({
      Version = "2012-10-17"
      Statement = [{
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.estuary.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "openid.estuary.dev/1234567890abcdef.dp.estuary-data.com:aud" = local.role_arn
          }
          StringLike = {
            "openid.estuary.dev/1234567890abcdef.dp.estuary-data.com:sub" = "your-tenant/*"
          }
        }
      }]
    })
  }
  ```

  Replace `1234567890abcdef.dp.estuary-data.com` with your data plane identifier from the Estuary dashboard.

- AWS Credentials.  One of the following types:
  - The AWS **access key** and **secret access key** for the user. See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.
  - To authenticate using an AWS Role, you'll need the **region** and the **role arn**.  Follow the steps in the [AWS IAM guide](/guides/iam-auth/aws.md) to setup the role.
    - The role's Max Session Duration should be set to 43200 seconds (12 hours). This is required; the default 1 hour will cause the capture to fail.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the DynamoDB source connector.

### Properties

#### Endpoint

| Property                             | Title                   | Description                                                                                                   | Type    | Required/Default |
| ------------------------------------ | ----------------------- | ------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/region`**                        | AWS Region              | The name of the AWS region where the DynamoDB tables are located.                                             | string  | Required         |
| **`/credentials`**                   | Credentials             | Credentials for authentication.                                                                               | [Credentials](#credentials) | Required |
| `advanced/backfillSegments`          | Backfill Table Segments | Number of segments to use for backfill table scans. Has no effect if changed after the backfill has started.  | integer |                  |
| `advanced/endpoint`                  | AWS Endpoint            | The AWS endpoint URI to connect to. Use if you're capturing from a compatible API that isn't provided by AWS. | string  |                  |
| `advanced/scanLimit`                 | Scan Limit              | Limit the number of items to evaluate for each table backfill scan request.                                   | integer |                  |

#### Credentials

Credentials for authenticating with AWS.  Use one of the following sets of options:

| Property                                 | Title                   | Description                                                                                               | Type    | Required/Default         |
| ---------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------------------- | ------- | ------------------------ |
| **`/credentials/auth_type`**             | Auth Type               | Method to use for authentication.                                                                         | string  | Required: `AWSAccessKey` |
| **`/credentials/aws_access_key_id`**     | AWS Access Key ID       | AWS Access Key ID for capturing from DynamoDB tables.                                                     | string  | Required                 |
| **`/credentials/aws_secret_access_key`** | AWS Secret Access key   | AWS Secret Access Key for capturing from DynamoDB tables.                                                 | string  | Required                 |

| Property                                 | Title                   | Description                                                                                               | Type    | Required/Default   |
| ---------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------------------- | ------- | ------------------ |
| **`/credentials/auth_type`**             | Auth Type               | Method to use for authentication.                                                                         | string  | Required: `AWSIAM` |
| **`/credentials/aws_role_arn`**          | AWS Role ARN            | IAM Role to assume.                                                                                       | string  | Required           |
| **`/credentials/aws_region`**            | AWS Region              | AWS Region to authenticate in.                                                                            | string  | Required           |

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
          credentials:
            auth_type: "AWSAccessKey"
            aws_access_key_id: "example-aws-access-key-id"
            aws_secret_access_key: "example-aws-secret-access-key"
          region: "us-east-1"
    bindings:
      - resource:
          table: ${TABLE_NAME}
        target: ${PREFIX}/${COLLECTION_NAME}
```

Your capture definition may be more complex, with additional bindings for each DynamoDB table.

[Learn more about capture definitions.](../../../concepts/captures.md#specification)

## Troubleshooting

### "No OpenIDConnect provider found"

The OIDC provider URL doesn't match what AWS expects. Verify:
- The URL in AWS matches exactly what's shown in the Estuary connector config
- Check for trailing slash mismatches (the URL should not have a trailing slash)

### "Not authorized to perform sts:AssumeRoleWithWebIdentity"

The trust policy conditions don't match. Verify:
- The `aud` claim matches your role ARN exactly
- The `sub` claim pattern matches your Estuary tenant prefix
- Wait 1-2 minutes after creating or updating the OIDC provider for AWS propagation

### Tables not appearing in bindings

If your DynamoDB tables don't appear when configuring the capture:
- Verify DynamoDB Streams is enabled on the table
- Verify the stream view type is set to "New and old images"
- Verify your IAM policy includes `ListTables` permission on `table/*`

### "dynamodb:ListTables... AccessDeniedException"

The `ListTables` action requires the `table/*` resource pattern. You cannot scope this permission to specific table names—it must have access to list all tables in the region.
