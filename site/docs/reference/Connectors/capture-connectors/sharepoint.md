# SharePoint

This connector captures data from SharePoint document libraries in team sites and communication sites into Estuary collections.

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/source-sharepoint:dev`](https://ghcr.io/estuary/source-sharepoint:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data types

This connector automatically captures the data within the specified SharePoint folder into a single Estuary collection.

The following file types are supported:

- Avro
- CSV
- JSON
- Protobuf
- W3C Extended Log

The following compression methods are supported:

- ZIP
- GZIP
- ZSTD

By default, Estuary automatically detects the file type and compression method.
If necessary, you can specify the correct file type, compression, and other properties (CSV only) using the optional [parser configuration](#advanced-parsing-sharepoint-files).

## Prerequisites

To use this connector, you need:

- An active Microsoft 365 account with access to SharePoint Online
- Access to the SharePoint sites and document libraries you want to capture from

Authentication is handled using OAuth2 in the Estuary web app.

**Note:** This connector is designed for files located in SharePoint document libraries within team sites (`/teams/`) or communication sites (`/sites/`).

## Configuration

You configure the SharePoint source connector in the Estuary web app.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SharePoint source connector.

SharePoint offers two configuration methods for flexibility. Both methods provide the same functionality:

### URL Method

The simplest approach for most users. Provide a complete SharePoint URL pointing to your folder. E.g.: `https://contoso.sharepoint.com/sites/ProjectAlpha/Shared Documents/reports`

The connector automatically parses the URL to identify the site, document library, and folder path.

### Components Method

Useful when the required values are already known. Specify site ID, drive ID (document library), and folder path separately.

### Properties

#### Endpoint

| Property                         | Title                | Description                                                                                                                                                                                                                                                                                                                                      | Type         | Required/Default                    |
| -------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------ | ----------------------------------- |
| **`/credentials`**               | Credentials          | OAuth2 credentials for SharePoint. These are automatically handled by the Web UI.                                                                                                                                                                                                                                                                | object       | Required                            |
| **`/site_configuration`**        | Site Configuration   | Configuration for accessing a SharePoint site. Choose between URL method (simple) or Components method (advanced).                                                                                                                                                                                                                               | object       | Required                            |
| **`/site_configuration/method`** | Configuration Method | Choose "url" for URL-based configuration or "components" for component ID-based configuration.                                                                                                                                                                                                                                                   | string       | Required                            |
| `/site_configuration/site_url`   | Site URL             | Complete URL to SharePoint folder (URL method only). Example: `https://contoso.sharepoint.com/sites/Marketing/Documents/folder`. Supports both `/sites/` and `/teams/` prefixes.                                                                                                                                                                 | string       | Required if method is "url"         |
| `/site_configuration/site_id`    | Site ID              | The SharePoint site identifier (Components method only). Accepts formats: (1) Full ID from Graph API: `contoso.sharepoint.com,2C712604-1370-44E7-A1F5-426573FDA80A,2D2244C3-251A-49EA-93A8-39E1C3A060FE` or (2) Hostname with path: `contoso.sharepoint.com:/sites/Marketing`                                                                    | string       | Required if method is "components"  |
| `/site_configuration/drive_id`   | Drive ID             | The document library (drive) identifier (Components method only).                                                                                                                                                                                                                                                                                | string       | The site's default document library |
| `/site_configuration/path`       | Path                 | Folder path within the document library (Components method only). Must start with `/`.                                                                                                                                                                                                                                                           | string       | `/`                                 |
| `/matchKeys`                     | Match Keys           | Filter applied to all file paths. If provided, only files whose absolute path matches this regex will be read. Example: `.*\.json` to only capture JSON files.                                                                                                                                                                                   | string       |                                     |
| `/advanced`                      | Advanced             | Options for advanced users. You should not typically need to modify these.                                                                                                                                                                                                                                                                       | object       |                                     |
| `/advanced/ascendingKeys`        | Ascending Keys       | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire path. This requires that you write files in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. If data is not ordered correctly, using ascending keys could cause errors. | boolean      | `false`                             |
| `/parser`                        | Parser Configuration | Configures how files are parsed (optional, see below)                                                                                                                                                                                                                                                                                            | object       |                                     |
| `/parser/compression`            | Compression          | Determines how to decompress the contents. The default, 'Auto', will try to determine the compression automatically.                                                                                                                                                                                                                             | null, string | `null`                              |
| `/parser/format`                 | Format               | Determines how to parse the contents. The default, 'Auto', will try to determine the format automatically based on the file extension or MIME type, if available.                                                                                                                                                                                | object       | `{"type":"auto"}`                   |
| `/parser/format/type`            | Type                 |                                                                                                                                                                                                                                                                                                                                                  | string       |                                     |

#### Bindings

| Property      | Title  | Description                    | Type   | Required/Default |
| ------------- | ------ | ------------------------------ | ------ | ---------------- |
| **`/stream`** | Stream | The folder path being captured | string | Required         |

### Sample

#### Example 1: URL Method (Simple)

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sharepoint:dev
        config:
          site_configuration:
            method: url
            site_url: "https://contoso.sharepoint.com/sites/Marketing/Shared Documents/quarterly-reports"
          matchKeys: ".*\\.json"
    bindings:
      - resource:
          stream: "/quarterly-reports"
        target: ${PREFIX}/${COLLECTION_NAME}
```

#### Example 2: Components Method (Advanced) with CSV Parser

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sharepoint:dev
        config:
          site_configuration:
            method: components
            site_id: "contoso.sharepoint.com,2C712604-1370-44E7-A1F5-426573FDA80A,2D2244C3-251A-49EA-93A8-39E1C3A060FE"
            drive_id: "b!1a2b3c4d5e6f7g8h9i0j"
            path: "/data-exports"
          parser:
            compression: zip
            format:
              type: csv
              config:
                delimiter: ","
                encoding: UTF-8
                headers: ["id", "name", "date", "amount"]
    bindings:
      - resource:
          stream: "/data-exports"
        target: ${PREFIX}/${COLLECTION_NAME}
```

### Advanced: Parsing SharePoint files

SharePoint document libraries can contain a variety of file types.
For each file type, Estuary must parse
and translate data into collections with defined fields and JSON schemas.

By default, the parser will automatically detect the type and shape of the data in the SharePoint folder,
so you won't need to change the parser configuration for most captures.

However, the automatic detection may be incorrect in some cases.
To fix or prevent this, you can provide explicit information in the parser configuration,
which is part of the endpoint configuration for this connector.

The parser configuration includes:

- **Compression**: Specify how the data is compressed.
  If no compression type is specified, the connector will try to determine the compression type automatically.
  Options are:
  - **zip**
  - **gzip**
  - **zstd**
  - **none**

- **Format**: Specify the data format, which determines how it will be parsed.
  If no file type is specified, the connector will try to determine the file type automatically
  Options are:
  - **Avro**
  - **CSV**
  - **JSON**
  - **Protobuf**
  - **W3C Extended Log**

#### CSV configuration

CSV files include several additional properties that are important to the parser.
In most cases, Estuary is able to automatically determine the correct values,
but you may need to specify for unusual datasets. These properties are:

- **Delimiter**. Options are:
  - Comma (`","`)
  - Pipe (`"|"`)
  - Space (`"0x20"`)
  - Semicolon (`";"`)
  - Tab (`"0x09"`)
  - Vertical tab (`"0x0B"`)
  - Unit separator (`"0x1F"`)
  - SOH (`"0x01"`)
  - Auto

- **Encoding** type, specified by its [WHATWG label](https://encoding.spec.whatwg.org/#names-and-labels).

- Optionally, an **Error threshold**, as an acceptable percentage of errors. If set to a number greater than zero, malformed rows that fall within the threshold will be excluded from the capture.

- **Escape characters**. Options are:
  - Backslash (`"\\"`)
  - Disable escapes (`""`)
  - Auto

- Optionally, a list of column **Headers**, if not already included in the first row of the CSV file.

  If any headers are provided, it is assumed that the provided list of headers is complete and authoritative.
  The first row of your CSV file will be assumed to be data (not headers), and you must provide a header value for every column in the file.

- **Line ending** values
  - CRLF (`"\\r\\n"`) (Windows)
  - CR (`"\\r"`)
  - LF (`"\\n"`)
  - Record Separator (`"0x1E"`)
  - Auto

- **Quote character**
  - Double Quote (`"\""`)
  - Single Quote (`"`)
  - Disable Quoting (`""`)
  - Auto
