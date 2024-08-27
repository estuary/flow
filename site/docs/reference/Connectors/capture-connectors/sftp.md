# SFTP

This connector captures data from an SFTP server.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-sftp:dev`](https://ghcr.io/estuary/source-sftp:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

You'll need an SFTP server that can accept connections from the [Estuary Flow IP addresses](/reference/allow-ip-addresses) using password authentication.

## Subdirectories and Symbolic Links

The connector must be configured with a `Directory` to capture files from. It will also descend into and capture files in normal subdirectories of the configured `Directory`.

Symbolic links to files are captured from the referent files. Symbolic links to subdirectories are _not_ captured, although the configured `Directory` may itself be a symbolic link.

## File Capturing Order

The standard mode of operation for the connector is to capture files according to their modification time. All files available on the server will initially be captured, and on an on-going basis new files that are added to the server are captured incrementally. New files added to the server are captured based on their modification time: If the connector finds a file with a more recent modification time than any previously observed, it will be captured. This means that any actions that update the modification time of a file on the server may cause it to be re-captured. For symbolic links to files the modification time of referent file is used, not of the symbolic link.

Alternatively, the advanced option `Ascending Keys` may be set. In this mode of operation the connector processes files strictly based on their path. New files are captured if they have a path lexically greater than any previously captured file. Lexical ordering considers the full path of the file.

As an example, consider a directory structure like the following with a data file initially in the directory `/root/b/`:

```
/root/
  a/
  b/data.csv
  c/
```

- In the normal mode of operation (`Ascending Keys` not set) the path `/root/b/data.csv` will initially be captured. Any added files will be captured by the connector on an on-going basis as they have increasingly more recent modification times.
- With `Ascending Keys` set the path `/root/b/data.csv` will initially be captured, but after that only added files in a higher lexical order will be captured:
  - Any file added to the directory `/root/a/` will _not_ be captured, becuase `/root/a/` comes before `/root/b/`.
  - Any file added to the directory `/root/c/` _will_ captured, because `/root/c/` comes after `/root/b/`.
  - A file added to the directory `/root/b/` may be captured if its name comes after `data.csv`.
  - This ordering applies on an on-going basis. If a file is added to `/root/c/`, after that only files with a higher lexical ordering than that file to be captured.

Setting `Ascending Keys` is only recommended if you have strict control over the naming of files and can ensure they are added in increasing lexical ordering.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SFTP source connector.

#### Endpoint

| Property                  | Title                | Description                                                                                                                                                                                                                                                                                              | Type         | Required/Default  |
| ------------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ | ----------------- |
| **`/address`**            | Address              | Host and port of the SFTP server. Example: `myserver.com:22`                                                                                                                                                                                                                                             | string       | Required          |
| **`/username`**           | Username             | Username for authentication.                                                                                                                                                                                                                                                                             | string       | Required          |
| `/password`               | Password             | Password for authentication. Only one of Password or SSHKey must be provided.                                                                                                                                                                                                                            | string       |                   |
| `/sshKey`                 | SSH Key              | SSH Key for authentication. Only one of Password or SSHKey must be provided.                                                                                                                                                                                                                             | string       |                   |
| **`/directory`**          | Directory            | Directory to capture files from. All files in this directory and any subdirectories will be included.                                                                                                                                                                                                    | string       | Required          |
| `/matchFiles`             | Match Files Regex    | Filter applied to all file names in the directory. If provided, only files whose path (relative to the directory) matches this regex will be captured. For example, you can use `.*\.json` to only capture json files.                                                                                   | string       |                   |
| `/advanced`               |                      | Options for advanced users. You should not typically need to modify these.                                                                                                                                                                                                                               | object       |                   |
| `/advanced/ascendingKeys` | Ascending Keys       | May improve sync speeds by listing files from the end of the last sync, rather than listing all files in the configured directory. This requires that you write files in ascending lexicographic order, such as an RFC-3339 timestamp, so that lexical path ordering matches modification time ordering. | boolean      | `false`           |
| `/parser`                 | Parser Configuration | Configures how files are parsed (optional, see below)                                                                                                                                                                                                                                                    | object       |                   |
| `/parser/compression`     | Compression          | Determines how to decompress the contents. The default, 'Auto', will try to determine the compression automatically.                                                                                                                                                                                     | null, string | `null`            |
| `/parser/format`          | Format               | Determines how to parse the contents. The default, 'Auto', will try to determine the format automatically based on the file extension or MIME type, if available.                                                                                                                                        | object       | `{"type":"auto"}` |

#### Bindings

| Property      | Title  | Description                     | Type   | Required/Default |
| ------------- | ------ | ------------------------------- | ------ | ---------------- |
| **`/stream`** | Prefix | Path to the captured directory. | string | Required         |

### Sample

```yaml
captures:
  ${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-sftp:dev"
        config:
          address: myserver.com:22
          username: <SECRET>
          password: <SECRET>
          directory: /data
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
                quote: '"'
    bindings:
      - resource:
          stream: /data
        target: ${COLLECTION_NAME}
```

### Advanced: Parsing SFTP Files

SFTP servers can support a wider variety of file types than other data source systems. For each of
these file types, Flow must parse and translate data into collections with defined fields and JSON
schemas.

By default, the parser will automatically detect the type and shape of the data in your bucket,
so you won't need to change the parser configuration for most captures.

However, the automatic detection may be incorrect in some cases.
To fix or prevent this, you can provide explicit information in the parser configuration,
which is part of the [endpoint configuration](#endpoint) for this connector.

The parser configuration includes:

- **Compression**: Specify how the bucket contents are compressed.
  If no compression type is specified, the connector will try to determine the compression type automatically.
  Options are:

  - **zip**
  - **gzip**
  - **zstd**
  - **none**

- **Format**: Specify the data format, which determines how it will be parsed.
  Options are:

  - **Auto**: If no format is specified, the connector will try to determine it automatically.
  - **Avro**
  - **CSV**
  - **JSON**
  - **Protobuf**
  - **W3C Extended Log**

  :::info
  At this time, Flow only supports SFTP captures with data of a single file type.
  Support for multiple file types, which can be configured on a per-binding basis,
  will be added in the future.

  For now, use a prefix in the endpoint configuration to limit the scope of each capture to data of a single file type.
  :::

#### CSV configuration

CSV files include several additional properties that are important to the parser.
In most cases, Flow is able to automatically determine the correct values,
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

The sample specification [above](#sample) includes these fields.
