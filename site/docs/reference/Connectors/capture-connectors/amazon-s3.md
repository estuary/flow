

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Amazon S3

This connector captures data from an Amazon S3 bucket.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-s3:dev`](https://ghcr.io/estuary/source-s3:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

You can use this connector to capture data from an entire S3 bucket or for a [prefix](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html) within a bucket.
This bucket or prefix must be either be:

* Publicly accessible and allowing anonymous reads.

* Accessible via a root or [IAM user][] or [IAM role][].

[IAM user]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html
[IAM role]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html

In either case, you'll need an [access policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_controlling.html).
Policies in AWS are JSON objects that define permissions. You attach them to _resources_, which include both IAM users and S3 buckets.

See the steps below to set up access.

### Setup: Public buckets

For a public bucket, the bucket access policy must allow anonymous reads on the whole bucket or a specific prefix.

1. Create a bucket policy using the templates below.

<Tabs>
<TabItem value="Anonymous reads policy - Full bucket" default>

```json file=./policies/public-full-bucket.json
```

</TabItem>
<TabItem value="Anonymous reads policy - Specific prefix" default>

```json file=./policies/public-prefix-only.json
```

</TabItem>
</Tabs>

2. [Add the policy to your bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/add-bucket-policy.html). Paste over the existing policy and resolve any errors or warnings before saving.

3. Confirm that the **Block public access** setting on the bucket is [disabled](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteAccessPermissionsReqd.html).

### Setup: Accessing with a user or role.

For buckets accessed by a user account, you'll need the AWS **access key** and **secret access key** for the user.  For bucket access using a IAM role, you will need the **role ARN**.
You'll also need to attach an access policy to the user or role to grant access to the specific bucket or prefix.

1. [Create an IAM user][IAM create-user] or follow the [AWS IAM Guide](/guides/iam-auth/aws.md) to setup an IAM role and Identity Provider.  If you already have a user or role for use with Estuary, it can be reused.

2. Note the user's access key and secret access key.
See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.

3. [Create an IAM policy][IAM create-policy] using the templates below.

<Tabs>
<TabItem value="IAM user access policy - Full bucket" default>

```json file=./policies/iam-user-full-bucket.json
```

</TabItem>
<TabItem value="IAM user access policy - Specific prefix" default>

```json file=./policies/iam-user-prefix-only.json
```

</TabItem>
</Tabs>

5. [Attach the policy to the IAM user or role](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_manage-attach-detach.html#add-policies-console).

[IAM create-user]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html
[IAM create-policy]: https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_create-console.html#access_policies_create-json-editor

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the S3 source connector.

### Properties

#### Endpoint

| Property                  | Title                   | Description                                                                                                   | Type    | Required/Default |
| ------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/region`**             | AWS Region              | The name of the AWS region where the S3 bucket is located. `us-east-1` is a popular default you can try, if you're unsure what to put here. | string | Required, `"us-east-1"` |
| **`/bucket`**             | Bucket                  | Name of the S3 bucket                                                                                         | string  | Required |
| `/prefix`                 | Prefix                  | Prefix within the bucket to capture from. Use this to limit the data in your capture.                         | string  |  |
| `/matchKeys`              | Match Keys              | Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use `.*\\.json` to only capture json files. | string |  |
| **`/credentials`**        | Credentials             | Credentials for authentication.                                                                               | [Credentials](#credentials) | Required |
| `/parser`                 | Parser Configuration    | Configures how files are parsed (optional, see below)                                                         | [Parser](#parser) |  |
| `/parser/compression`     | Compression             | Determines how to decompress the contents. The default, 'Auto', will try to determine the compression automatically.   | null, string | `null` |
| `/parser/format`          | Format                  | Determines how to parse the contents. The default, 'Auto', will try to determine the format automatically based on the file extension or MIME type, if available. | object | `{"type":"auto"}` |
| `/parser/format/type`     | Type                    |                                                                                                               | string |  |
| `/advanced`               |                         | Options for advanced users. You should not typically need to modify these.                                    | object |  |
| `/advanced/ascendingKeys` | Ascending Keys          | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. If data is not ordered correctly, using ascending keys could cause errors.| boolean | `false` |
| `/advanced/endpoint`      | AWS Endpoint            | The AWS endpoint URI to connect to. Use if you're capturing from a S3-compatible API that isn't provided by AWS | string |  |

#### Credentials

Credentials for authenticating.  Use one of the following sets of options:

| Property                                 | Title                   | Description                                                    | Type    | Required/Default         |
| ---------------------------------------- | ----------------------- | -------------------------------------------------------------- | ------- | ------------------------ |
| **`/credentials/auth_type`**             | Auth Type               | Use `AWSAccessKey` to authenticate with a user account.        | string  | Required: `AWSAccessKey` |
| **`/credentials/aws_access_key_id`**     | AWS Access Key ID       | AWS Access Key ID.                                             | string  | Required                 |
| **`/credentials/aws_secret_access_key`** | AWS Secret Access key   | AWS Secret Access Key.                                         | string  | Required                 |

| Property                                 | Title                   | Description                                                    | Type    | Required/Default         |
| ---------------------------------------- | ----------------------- | -------------------------------------------------------------- | ------- | ------------------------ |
| **`/credentials/auth_type`**             | Auth Type               | Use `AWSIAM` to authenticate as an IAM role.                   | string  | Required: `AWSIAM`       |
| **`/credentials/aws_role_arn`**          | AWS Role ARN            | IAM Role to assume.                                            | string  | Required                 |
| **`/credentials/aws_region`**            | AWS Region              | AWS Region to authenticate in.                                 | string  | Required                 |

| Property                                 | Title                   | Description                                                    | Type    | Required/Default         |
| ---------------------------------------- | ----------------------- | -------------------------------------------------------------- | ------- | ------------------------ |
| **`/credentials/auth_type`**             | Auth Type               | Use `AWSAnonymous` to do anonymous authenciation               | string  | Required: `AWSAnonymous` |

Your capture definition may be more complex, with additional bindings for different S3 prefixes within the same bucket.

[Learn more about capture definitions.](../../../concepts/captures.md)

#### Parser

Cloud storage platforms like S3 can support a wider variety of file types
than other data source systems. For each of these file types, Flow must parse
and translate data into collections with defined fields and JSON schemas.

By default, the parser will automatically detect the type and shape of the data in your bucket,
so you won't need to change the parser configuration for most captures.

However, the automatic detection may be incorrect in some cases.
To fix or prevent this, you can provide explicit information in the parser configuration,
which is part of the [endpoint configuration](#endpoint) for this connector.

The parser configuration includes:

* **Compression**: Specify how the bucket contents are compressed.
If no compression type is specified, the connector will try to determine the compression type automatically.
Options are:

   * **zip**
   * **gzip**
   * **zstd**
   * **none**

* **Format**: Specify the data format, which determines how it will be parsed.
Options are:

   * **Auto**: If no format is specified, the connector will try to determine it automatically.
   * **Avro**
   * **CSV**
   * **JSON**
   * **Protobuf**
   * **W3C Extended Log**

   :::info
   At this time, Flow only supports S3 captures with data of a single file type.
   Support for multiple file types, which can be configured on a per-binding basis,
   will be added in the future.

   For now, use a prefix in the endpoint configuration to limit the scope of each capture to data of a single file type.
   :::

#### CSV configuration

CSV files include several additional properties that are important to the parser.
In most cases, Flow is able to automatically determine the correct values,
but you may need to specify for unusual datasets. These properties are:

* **Delimiter**. Options are:
  * Comma (`","`)
  * Pipe (`"|"`)
  * Space (`"0x20"`)
  * Semicolon (`";"`)
  * Tab (`"0x09"`)
  * Vertical tab (`"0x0B"`)
  * Unit separator (`"0x1F"`)
  * SOH (`"0x01"`)
  * Auto

* **Encoding** type, specified by its [WHATWG label](https://encoding.spec.whatwg.org/#names-and-labels).

* Optionally, an **Error threshold**, as an acceptable percentage of errors. If set to a number greater than zero, malformed rows that fall within the threshold will be excluded from the capture.

* **Escape characters**. Options are:
  * Backslash (`"\\"`)
  * Disable escapes (`""`)
  * Auto

* Optionally, a list of column **Headers**, if not already included in the first row of the CSV file.

  If any headers are provided, it is assumed that the provided list of headers is complete and authoritative.
  The first row of your CSV file will be assumed to be data (not headers), and you must provide a header value for every column in the file.

* **Line ending** values
  * CRLF (`"\\r\\n"`) (Windows)
  * CR (`"\\r"`)
  * LF (`"\\n"`)
  * Record Separator (`"0x1E"`)
  * Auto

* **Quote character**
  * Double Quote (`"\""`)
  * Single Quote (`"`)
  * Disable Quoting (`""`)
  * Auto

The sample specification [above](#sample) includes these fields.

#### Bindings

| Property | Title| Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Prefix | Path to dataset in the bucket, formatted as `bucket-name/prefix-name`. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-s3:dev
        config:
          bucket: "my-bucket"
          region: "us-east-1"
          credentials:
            auth_type: "AWSAccessKey"
            aws_access_key_id: "example-aws-access-key-id"
            aws_secret_access_key: "example-aws-secret-access-key"
          parser:
            compression: zip
            format:
              type: csv
              config:
                delimiter: ","
                encoding: UTF-8
                errorThreshold: 5
                headers: [ID, username, first_name, last_name]
                lineEnding: "\\r"
                quote: "\""
    bindings:
      - resource:
          stream: my-bucket/${PREFIX}
        target: ${PREFIX}/${COLLECTION_NAME}
```
