
# Dropbox

This connector captures data from a Dropbox account into a Flow collection.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-dropbox:dev`](https://ghcr.io/estuary/source-dropbox:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data types

This connector automatically captures the data within the specified Dropbox folder into a single Flow collection.

The following file types are supported:

* Avro
* CSV
* JSON
* Protobuf
* W3C Extended Log

The following compression methods are supported:

* ZIP
* GZIP
* ZSTD

By default, Flow automatically detects the file type and compression method.
If necessary, you can specify the correct file type, compression, and other properties (CSV only) using the optional [parser configuration](#advanced-parsing-dropbox-files).

## Prerequisites

To use this connector, make sure you have an active Dropbox account. Authentication is handled using OAuth2 in the Flow web app.

**Note:** This connector is designed for files located in a specific Dropbox folder.

## Configuration

You configure the Dropbox source connector in the Flow web app.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Dropbox source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/path`** | Path | The path to the Dropbox folder to read from. For example, `"/my_folder"`. | string | Required |
| `/matchKeys` | Match Keys | Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use `.*\.json` to only capture JSON files. | object |  |
| **`/credentials`** | Credentials | OAuth2 credentials for Dropbox. These are automatically handled by the Web UI. | object | Required |
| `/advanced` | Advanced       | Options for advanced users. You should not typically need to modify these. | object |  |
| `/advanced/ascendingKeys` | Ascending Keys | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. If data is not ordered correctly, using ascending keys could cause errors. | boolean | `false` |
| `/parser` | Parser Configuration | Configures how files are parsed (optional, see below) | object |  |
| `/parser/compression` | Compression | Determines how to decompress the contents. The default, &#x27;Auto&#x27;, will try to determine the compression automatically. | null, string | `null` |
| `/parser/format` | Format | Determines how to parse the contents. The default, &#x27;Auto&#x27;, will try to determine the format automatically based on the file extension or MIME type, if available. | object | `{"type":"auto"}` |
| `/parser/format/type` | Type |  | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Path | The path to the Dropbox folder. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-http-file:dev
        config:
          path: "/my_folder"
          matchKeys: ".*\.json"
    bindings:
      - resource:
          stream: /my_folder
        target: ${PREFIX}/${COLLECTION_NAME}

```

### Advanced: Parsing Dropbox files

Dropbox folders can contain a variety of file types.
For each file type, Flow must parse
and translate data into collections with defined fields and JSON schemas.

By default, the parser will automatically detect the type and shape of the data in the Dropbox folder,
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
   * **Protobuf**
   * **W3C Extended Log**

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
