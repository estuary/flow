# Google Drive

This connector lets you capture data from your Google Drive account into Flow collections.

[`ghcr.io/estuary/source-google-drive:dev`](https://ghcr.io/estuary/source-google-drive:dev) provides the latest connector image. For access to previous image versions, follow the link in your browser.

## Prerequisites

To use this connector, make sure you have the following:

- An active Google Drive account with access credentials.
- Properly configured permissions for your Google Drive resources.

**Note:** This connector is designed specifically for .csv files located in a specified Google Drive folder.

## Configuration

You can set up the Google Drive source connector either through the Flow web app or by editing the Flow specification file directly. For more information on setting up this connector, refer to our guide on using [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors).

The values and specification sample below provide configuration details specific to the Google Drive connector.

### Properties

#### Endpoint

| Property           | Title          | Description                                                        | Type   | Required/Default  |
|--------------------|----------------|--------------------------------------------------------------------|--------|-------------------|
| **`/credentials`** | Credentials    | JSON file containing authentication credentials for Google Drive.  | file   | Required         |

### Bindings

| Property       | Title       | Description                          | Type    | Required/Default  |
|----------------|-------------|--------------------------------------|---------|-------------------|
| **`/file_id`** | File ID     | Unique identifier of the Google Drive file. | string  | Required         |
| **`/path`**    | Path        | Path to the file or directory in Google Drive. | string  | Required         |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-google-drive:dev
        config:
          credentials: /path/to/your/credentials.json
    bindings:
      - resource:
          file_id: "your_google_drive_file_id"
          path: "/path/in/google/drive"
        target: ${PREFIX}/target_name
