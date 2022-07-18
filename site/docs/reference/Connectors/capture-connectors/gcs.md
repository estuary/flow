---
sidebar_position: 8
---
# Google Cloud Storage

This connector captures data from a Google Cloud Storage (GCS) bucket.

[`ghcr.io/estuary/source-gcs:dev`](https://ghcr.io/estuary/source-gcs:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, either your GCS bucket must be public, or you must have access via a Google service account.

* For public buckets, verify that objects in the bucket are [publicly readable](https://cloud.google.com/storage/docs/access-control/making-data-public).
* For buckets accessed by a Google Service Account:
    * Ensure that the user has been assigned a [role](https://cloud.google.com/iam/docs/understanding-roles) with read access.
    * Create a [JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating). Google's [Application Default Credentials](https://cloud.google.com/docs/authentication/production) will use this file for authentication.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog spec YAML.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and YAML sample below provide configuration details specific to the GCS source connector.

:::tip
You might use [prefixes](https://cloud.google.com/storage/docs/samples/storage-list-files-with-prefix) to organize your GCS bucket
in a way that emulates a directory structure.
This connector can use prefixes in two ways: first, to perform the [**discovery**](../../../concepts/connectors.md#using-connectors) phase of setup, and later, when the capture is running.

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
| **`/bucket`** | Bucket | Name of the Google Cloud Storage bucket | string | Required |
| `/googleCredentials` | Google Service Account | Service account JSON key to use as Application Default Credentials | string |  |
| `/matchKeys` | Match Keys | Filter applied to all object keys under the prefix. If provided, only objects whose key (relative to the prefix) matches this regex will be read. For example, you can use &quot;.&#x2A;&#x5C;.json&quot; to only capture json files. | string |  |
| `/parser` | Parser Configuration | Configures how files are parsed | object |  |
| `/parser/compression` | Compression | Determines how to decompress the contents. The default, &#x27;Auto&#x27;, will try to determine the compression automatically. | null, string | `null` |
| `/parser/format` |  | Determines how to parse the contents. The default, &#x27;Auto&#x27;, will try to determine the format automatically based on the file extension or MIME type, if available. | object | `{"auto":{}}` |
| `/parser/format/auto` | Auto |  | object | `{}` |
| `/parser/format/avro` | Avro |  | object | `{}` |
| `/parser/format/csv` | CSV |  | object |  |
| `/parser/format/csv/delimiter` | Delimiter | The delimiter that separates values within each row. Only single-byte delimiters are supported. | null, string | `null` |
| `/parser/format/csv/encoding` | Encoding | The character encoding of the source file. If unspecified, then the parser will make a best-effort guess based on peeking at a small portion of the beginning of the file. If known, it is best to specify. Encodings are specified by their WHATWG label. | null, string | `null` |
| `/parser/format/csv/errorThreshold` | Error Threshold | Allows a percentage of errors to be ignored without failing the entire parsing process. When this limit is exceeded, parsing halts. | integer | `null` |
| `/parser/format/csv/escape` | Escape Character | The escape character, used to escape quotes within fields. | null, string | `null` |
| `/parser/format/csv/headers` | Headers | Manually specified headers, which can be used in cases where the file itself doesn&#x27;t contain a header row. If specified, then the parser will assume that the first row is data, not column names, and the column names given here will be used. The column names will be matched with the columns in the file by the order in which they appear here. | array | `[]` |
| `/parser/format/csv/lineEnding` | Line Ending | The value that terminates a line. Only single-byte values are supported, with the exception of &quot;&#x5C;r&#x5C;n&quot; (CRLF), which will accept lines terminated by either a carriage return, a newline, or both. | null, string | `null` |
| `/parser/format/csv/quote` | Quote Character | The character used to quote fields. | null, string | `null` |
| `/parser/format/json` | JSON |  | object | `{}` |
| `/parser/format/w3cExtendedLog` | W3C Extended Log |  | object | `{}` |
| `/prefix` | Prefix | Prefix within the bucket to capture from | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Prefix | Path to dataset in the bucket, formatted as `bucket-name/prefix-name` | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-gcs:dev
        config:
          bucket: my-bucket
          googleCredentials:
            "type": "service_account",
            "project_id": "project-id",
            "private_key_id": "key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n",
            "client_email": "service-account-email",
            "client_id": "client-id",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://accounts.google.com/o/oauth2/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account-email"
          parser:
            compression: zip
            format: csv
              csv:
                delimiter: ","
                encoding: utf-8
                errorThreshold: 5
                headers: [ID, username, first_name, last_name]
                lineEnding: "\r"
                quote: """
    bindings:
      - resource:
          stream: my-bucket/${PREFIX}
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}

```

Your capture definition may be more complex, with additional bindings for different GCS prefixes within the same bucket.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)

### Advanced: Parsing cloud storage data

Cloud storage platforms like GCS can support a wider variety of file types
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
   At this time, Flow only supports GCS captures with data of a single file type.
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

### Advanced: Configure Google service account impersonation

As part of your Google IAM management, you may have configured one service account to [impersonate another service account](https://cloud.google.com/iam/docs/impersonating-service-accounts).
You may find this useful when you want to easily control access to multiple service accounts with only one set of keys.

If necessary, you can configure this authorization model for a GCS capture in Flow using the GitOps workflow.
To do so, you'll enable sops encryption and impersonate the target account with JSON credentials.

Before you begin, make sure you're familiar with [how to encrypt credentials in Flow using sops](./../../../concepts/connectors.md#protecting-secrets).

Use the following sample as a guide to add the credentials JSON to the capture's endpoint configuration.
The sample uses the [encrypted suffix feature](../../../concepts/connectors.md#example-protect-portions-of-a-configuration) of sops to encrypt only the sensitive credentials, but you may choose to encrypt the entire configuration.

```yaml
config:
  bucket: <bucket-name>
  googleCredentials_sops:
    # URL containing the account to impersonate and the associated project
    service_account_impersonation_url: https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/<target-account>@<project>.iam.gserviceaccount.com:generateAccessToken
    # Credentials for the account that has been configured to impersonate the target.
    source_credentials:
        # In addition to the listed fields, copy and paste the rest of your JSON key file as your normally would
        # for the `googleCredentials` field
        client_email: <origin-account>@<anotherproject>.iam.gserviceaccount.com
        token_uri: https://oauth2.googleapis.com/token
        type: service_account
    type: impersonated_service_account
```
