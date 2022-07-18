---
sidebar_position: 2
---
# Amazon S3

This connector captures data from an Amazon S3 bucket.

[`ghcr.io/estuary/source-s3:dev`](https://ghcr.io/estuary/source-s3:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, either your S3 bucket must be public,
or you must have access via a root or [IAM user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html).

* For public buckets, verify that the [access policy](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-overview.html#access-control-resources-manage-permissions-basics) allows anonymous reads.
* For buckets accessed by a user account, you'll need the AWS **access key** and **secret access key** for the user.
See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog spec YAML.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and YAML sample below provide configuration details specific to the S3 source connector.

:::tip
You might organize your S3 bucket using [prefixes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html) to emulate a directory structure.
This connector can use prefixes in two ways: first, to perform the [**discovery**](../../../concepts/connectors.md#flowctl-discover) phase of setup, and later, when the capture is running.

* You can specify a prefix in the endpoint configuration to limit the overall scope of data discovery.
* You're required to specify prefixes on a per-binding basis. This allows you to map each prefix to a distinct Flow collection,
and informs how the capture will behave in production.

To capture the entire bucket, omit `prefix` in the endpoint configuration and set `stream` to the name of the bucket.
:::

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/ascendingKeys` | Ascending Keys | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. | boolean | `false` |
| `/awsAccessKeyId` | AWS Access Key ID | Part of the AWS credentials that will be used to connect to S3. Required unless the bucket is public and allows anonymous listings and reads. | string |  |
| `/awsSecretAccessKey` | AWS Secret Access Key | Part of the AWS credentials that will be used to connect to S3. Required unless the bucket is public and allows anonymous listings and reads. | string |  |
| **`/bucket`** | Bucket | Name of the S3 bucket | string | Required |
| `/endpoint` | AWS Endpoint | The AWS endpoint URI to connect to. Use if you&#x27;re capturing from a S3-compatible API that isn&#x27;t provided by AWS | string |  |
| `/matchKeys` | Match Keys | Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use &quot;.&#x2A;&#x5C;.json&quot; to only capture json files. | string |  |
| `/parser` | Parser Configuration | Configures how files are parsed | object |  |
| `/parser/compression` | Compression | Determines how to decompress the contents. The default, &#x27;Auto&#x27;, will try to determine the compression automatically. | null, string | `null` |
| `/parser/format` |  | Determines how to parse the contents. The default, &#x27;Auto&#x27;, will try to determine the format automatically based on the file extension or MIME type, if available. | object | `{"auto":{}}` |
| `/parser/format/auto` |  |  | object | `{}` |
| `/parser/format/avro` |  |  | object | `{}` |
| `/parser/format/csv` |  |  | object |  |
| `/parser/format/csv/delimiter` | Delimiter | The delimiter that separates values within each row. Only single-byte delimiters are supported. | null, string | `null` |
| `/parser/format/csv/encoding` | Encoding | The character encoding of the source file. If unspecified, then the parser will make a best-effort guess based on peeking at a small portion of the beginning of the file. If known, it is best to specify. Encodings are specified by their WHATWG label. | null, string | `null` |
| `/parser/format/csv/errorThreshold` | Error Threshold | Allows a percentage of errors to be ignored without failing the entire parsing process. When this limit is exceeded, parsing halts. | integer | `0` |
| `/parser/format/csv/escape` | Escape Character | The escape character, used to escape quotes within fields. | null, string | `null` |
| `/parser/format/csv/headers` |  | Manually specified headers, which can be used in cases where the file itself doesn&#x27;t contain a header row. If specified, then the parser will assume that the first row is data, not column names, and the column names given here will be used. The column names will be matched with the columns in the file by the order in which they appear here. | array | `[]` |
| `/parser/format/csv/lineEnding` | Line Ending | The value that terminates a line. Only single-byte values are supported, with the exception of &quot;&#x5C;r&#x5C;n&quot; (CRLF), which will accept lines terminated by either a carriage return, a newline, or both. | null, string | `null` |
| `/parser/format/csv/quote` | Quote Character | The character used to quote fields. | null, string | `null` |
| `/parser/format/json` |  |  | object | `{}` |
| `/parser/format/w3cExtendedLog` |  |  | object | `{}` |
| `/prefix` | Prefix | Prefix within the bucket to capture from. | string |  |
| **`/region`** | AWS Region | The name of the AWS region where the S3 bucket is located. &quot;us-east-1&quot; is a popular default you can try, if you&#x27;re unsure what to put here. | string | Required, `"us-east-1"` |

#### Bindings

| Property | Title| Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Prefix | Path to dataset in the bucket, formatted as `bucket-name/prefix-name`. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-s3:dev
        config:
          bucket: "my-bucket"
          parser:
            compression: zip
            format: csv
              csv:
                delimiter: ","
                encoding: utf-8
                errorThreshold: 5
                headers: [ID, username, first_name, last_name]
                lineEnding: ""\r"
                quote: """
          region: "us-east-1"
    bindings:
      - resource:
          stream: my-bucket/${PREFIX}
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}

```

Your capture definition may be more complex, with additional bindings for different S3 prefixes within the same bucket.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)

### Advanced: Parsing cloud storage data

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
Options are **zip**, **gzip**, **zstd**, and **none**.

* **Format**: Specify the data format, which determines how it will be parsed.
Options are:

   * **Auto**: If no format is specified, the connector will try to determine it automatically.
   * **Avro**
   * **CSV**
   * **JSON**
   * **W3C Extended Log**

   :::info
   At this time, Flow only supports S3 captures with data of a single file type.
   Support for multiple file types, which can be configured on a per-binding basis,
   will be added in the future.

   For now, use a prefix in the endpoint configuration to limit the scope of each capture to data of a single file type.
   :::

Only CSV data requires further configuration. When capturing CSV data, you must specify:

* **Delimiter**
* **Encoding** type, specified by its [WHATWG label](https://encoding.spec.whatwg.org/#names-and-labels).
* Optionally, an **Error threshold**, as an acceptable percentage of errors.
* **Escape characters**
* Optionally, a list of column **Headers**, if not already included in the first row of the CSV file.
* **Line ending** values
* **Quote character**

Descriptions of these properties are included in the [table above](#endpoint).
