---
sidebar_position: 10
---
# HTTP file

This connector captures data from an HTTP endpoint into a Flow collection.

[`ghcr.io/estuary/source-http-file:dev`](https://ghcr.io/estuary/source-http-file:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Supported data types

This connector automatically captures the data hosted at the specified URL into a single Flow collection.

The following file types are supported:

* Avro
* CSV
* JSON
* W3C Extended Log

The following compression methods are supported:

* ZIP
* GZIP
* ZSTD

By default, Flow automatically detects the file type and compression method.
If necessary, you can specify the correct file type, compression, and other properties (CSV only) using the optional [parser configuration](#advanced-parsing-http-hosted-data).

## Prerequisites

To use this connector, you'll need the URL to an HTTP endpoint that hosts data of one of the supported types.

Some HTTP endpoints require credentials for access.
If this is the case, have your username and password ready.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog spec YAML.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and YAML sample below provide configuration details specific to the HTTP file source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/credentials` | Credentials | User credentials, if required to access the data at the HTTP URL. | object |  |
| `/credentials/password` | Password | Password, if required to access the HTTP endpoint. | string |  |
| `/credentials/user` | User | Username, if required to access the HTTP endpoint. | string |  |
| `/parser` | Parser Configuration | Configures how files are parsed | object |  |
| `/parser/compression` | Compression | Determines how to decompress the contents. The default, &#x27;Auto&#x27;, will try to determine the compression automatically. | string | `auto` |
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
| **`/url`** | HTTP File URL | A valid HTTP url for downloading the source file. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Name of the dataset | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Set to `incremental` for real-time updates. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-http-file:dev
        config:
          url: https://my-site.com/my_hosted_dataset.json.zip
    bindings:
      - resource:
          stream: my_hosted_dataset.json.zip
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}

```

### Advanced: Parsing HTTP-hosted data

HTTP endpoints can support a variety of file types.
For each file type, Flow must parse
and translate data into collections with defined fields and JSON schemas.

By default, the parser will automatically detect the type and shape of the data at the HTTP endpoint,
so you won't need to change the parser configuration for most captures.

However, the automatic detection may be incorrect in some cases.
To fix or prevent this, you can provide explicit information in the parser configuration,
which is part of the [endpoint configuration](#endpoint) for this connector.

The parser configuration includes:

* **Compression**: Specify how the data is compressed.
If no compression type is specified, the connector will try to determine the compression type automatically.
Options are:

   * **zip**
   * **gzip**
   * **zstd**
   * **none**

* **Format**: Specify the data format, which determines how it will be parsed.
If no file type is specified, the connector will try to determine the file type automatically
Options are:

   * **Avro**
   * **CSV**
   * **JSON**
   * **W3C Extended Log**

Only CSV data requires further configuration. When capturing CSV data, you must specify:

* **Delimiter**
* **Encoding** type, specified by its [WHATWG label](https://encoding.spec.whatwg.org/#names-and-labels).
* Optionally, an **Error threshold**, as an acceptable percentage of errors.
* **Escape characters**
* Optionally, a list of column **Headers**, if not already included in the first row of the CSV file.
* **Line ending** values
* **Quote character**

Descriptions of these properties are included in the [table above](#endpoint).
