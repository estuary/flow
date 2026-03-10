# Google Drive

This connector lets you capture data from your Google Drive account into Estuary collections.

## Prerequisites

To use this connector, make sure you have the following:

- An active Google Drive account with access credentials.
- Properly configured permissions for your Google Drive resources.

**Note:** This connector is designed specifically for .csv files located in a specified Google Drive folder.

:::tip Capture from multiple paths
This connector supports multiple bindings to capture from different files or folders within a single capture task. See [Capture Multiple Paths with File Source Connectors](/guides/flowctl/multiple-file-source-bindings) for a step-by-step guide.
:::

## Configuration

You can set up the Google Drive source connector either through the Estuary web app or by editing the Data Flow specification file directly. For more information on setting up this connector, refer to our guide on using [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors).

The values and specification sample below provide configuration details specific to the Google Drive connector.

### Properties

#### Endpoint

| Property           | Title          | Description                                                                                                                 | Type   | Required/Default |
|--------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/folderUrl`**   | Folder URL     | URL of the Google Drive folder to capture from. Must be `https://drive.google.com/drive/folders/FOLDER_ID`. If your URL contains `/u/0/` or `/u/1/` (from Google's account switcher), remove that segment. | string | Required         |
| **`/credentials`** | Credentials    | Google OAuth2 credentials or service account JSON for authentication.                                                        | object | Required         |
| `/matchKeys`       | Match Keys     | Filter applied to file paths under the folder. If provided, only files whose paths match this regex will be read.            | string |                  |
| `/parser`          | Parser Configuration | Configures how files are parsed (optional).                                                                             | object |                  |
| `/advanced/ascendingKeys` | Ascending Keys | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire folder.           | boolean | `false`         |

### Bindings

| Property      | Title  | Description                                      | Type   | Required/Default |
|---------------|--------|--------------------------------------------------|--------|------------------|
| **`/stream`** | Stream | Path prefix for the files captured by this binding. | string | Required         |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-google-drive:v1
        config:
          folderUrl: "https://drive.google.com/drive/folders/YOUR_FOLDER_ID"
          credentials:
            auth_type: Client
            client_id: <CLIENT_ID>
            client_secret: <CLIENT_SECRET>
            refresh_token: <REFRESH_TOKEN>
    bindings:
      - resource:
          stream: "YOUR_FOLDER_ID"
        target: ${PREFIX}/target_name
