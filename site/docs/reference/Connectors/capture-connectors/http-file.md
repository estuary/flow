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
The HTTP endpoint must support [`HEAD`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/HEAD) HTTP requests, and the response to this request must include a [`Last-Modified`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) header.

:::tip
You can send a test `HEAD` request using Curl with the `-I` parameter, for example:
`curl -I https://my-site.com/my_hosted_dataset.json.zip`
Use [this online tool](https://reqbin.com/req/c-tmyvmbgu/curl-head-request-example) to easily do so in your browser.
:::

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
| `/parser/compression` | Compression | Determines how to decompress the contents. The default, &#x27;Auto&#x27;, will try to determine the compression automatically. | null, string | `null` |
| `/parser/format` | Format | Determines how to parse the contents. The default, &#x27;Auto&#x27;, will try to determine the format automatically based on the file extension or MIME type, if available. | object | `{"type":"auto"}` |
| `/parser/format/type` | Type |  | string |  |
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
which is part of the endpoint configuration for this connector.

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

#### CSV configuration

Only CSV data allows further configuration. When capturing CSV data, you may additionally specify:

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

* Optionally, an **Error threshold**, as an acceptable percentage of errors.

* **Escape characters**. Options are:
  * Backslash (`"\\"`)
  * Disable escapes (`""`)
  * Auto

* Optionally, a list of column **Headers**, if not already included in the first row of the CSV file.

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

The YAML sample [above](#sample) includes these fields.
