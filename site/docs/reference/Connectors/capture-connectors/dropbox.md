
# Dropbox
This connector lets you capture data from your Dropbox account into Flow collections.

[ghcr.io/estuary/source-dropbox:dev](https://ghcr.io/estuary/source-dropbox:dev) provides the latest connector image. For access to previous image versions, follow the link in your browser.

## Prerequisites
To use this connector, make sure you have the following:

- An active Dropbox account with access credentials.

We just require permission to read the files content and metadata.

**Note:** This connector is designed for files located in a specified Dropbox folder.

## Configuration
You can set up the Dropbox source connector either through the Flow web app or by editing the Flow specification file directly. For more information on setting up this connector, refer to our guide on using [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors).

The values and specification sample below provide configuration details specific to the Dropbox connector.

### Properties

#### Endpoint
| Property                         | Title          | Description                                                                                                                                                                                                                                                                                                                                                 | Type    | Required/Default |
| -------------------------------- | -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/path`**                      | Path           | The path to the Dropbox folder to read from. For example, "/my_folder".                                                                                                                                                                                                                                                                                     | string  | required         |
| **`/credentials`**               | Credentials    | OAuth2 credentials for Dropbox. Those are automatically handled by the Web UI.                                                                                                                                                                                                                                                                              | object  | required         |
| **`/credentials/refresh_token`** | Refresh Token  | The refresh token for the Dropbox account.                                                                                                                                                                                                                                                                                                                  | string  | required         |
| **`/credentials/client_id`**     | Client ID      | The client ID for the Dropbox account.                                                                                                                                                                                                                                                                                                                      | string  | required         |
| **`/credentials/client_secret`** | Client Secret  | The client secret for the Dropbox account.                                                                                                                                                                                                                                                                                                                  | string  | required         |
| **`/matchKeys`**                 | Match Keys     | Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use `.*\\.json` to only capture JSON files.                                                                                                                                                         | object  |                  |
| **`/advanced`**                  | Advanced       | Options for advanced users. You should not typically need to modify these.                                                                                                                                                                                                                                                                                  | object  |                  |
| **`/advanced/ascendingKeys`**    | Ascending Keys | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. If data is not ordered correctly, using ascending keys could cause errors. | boolean | false            |

#### **Bindings**
| Property      | Title | Description                    | Type   | Required/Default |
| ------------- | ----- | ------------------------------ | ------ | ---------------- |
| **`/stream`** | Path  | The Path to the Dropbox folder | string | required         |



### Sample
```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-dropbox:local"
        config:
          path: /test
          credentials:
            refresh_token: AAAABBBBBCCCC
            client_id: 123abc
            client_secret: 123abc
    bindings:
      - resource:
          stream: /test
        target: ${PREFIX}/${COLLECTION_NAME}
```
