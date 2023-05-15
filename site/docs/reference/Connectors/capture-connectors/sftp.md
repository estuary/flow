---
sidebar_position: 1
---
# SFTP

This connector captures data from an SFTP server.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-sftp:dev`](https://ghcr.io/estuary/source-sftp:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

You'll need an SFTP server that can accept connections from the Estuary Flow IP address `34.121.207.128` using password authentication.

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

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/address`** | Address | Host and port of the SFTP server. Example: `myserver.com:22` | string | Required |
| **`/username`** | Username | Username for authentication. | string | Required |
| **`/password`** | Password | Password for authentication. | string | Required |
| **`/directory`** | Directory | Directory to capture files from. All files in this directory and any subdirectories will be included. | string | Required |
| `/matchFiles` | Match Files Regex | Filter applied to all file names in the directory. If provided, only files whose path (relative to the directory) matches this regex will be captured. For example, you can use `.*\.json` to only capture json files. | string | |
| `/advanced` | | Options for advanced users. You should not typically need to modify these. | object | |
| `/advanced/ascendingKeys` | Ascending Keys | May improve sync speeds by listing files from the end of the last sync, rather than listing all files in the configured directory. This requires that you write files in ascending lexicographic order, such as an RFC-3339 timestamp, so that lexical path ordering matches modification time ordering. | boolean | `false` |
| `/parser` | Parser Configuration  | Configures how files are parsed (optional, see below) | object | |
| `/parser/compression` | Compression | Determines how to decompress the contents. The default, 'Auto', will try to determine the compression automatically. | null, string | `null` |
| `/parser/format` | Format | Determines how to parse the contents. The default, 'Auto', will try to determine the format automatically based on the file extension or MIME type, if available. | object | `{"type":"auto"}` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Prefix | Path to the captured directory. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental`. | string | Required |

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
      bindings:
        - resource:
            stream: /data
            syncMode: incremental
          target: ${COLLECTION_NAME}
```
