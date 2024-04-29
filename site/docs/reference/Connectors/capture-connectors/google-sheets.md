
# Google Sheets

This connector captures data from a Google Sheets spreadsheet.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-google-sheets:dev`](https://ghcr.io/estuary/source-google-sheets:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.
You can find their documentation [here](https://docs.airbyte.com/integrations/sources/google-sheets),
but keep in mind that the two versions may be significantly different.

## Prerequisites

There are two ways to authenticate with Google when capturing data from a Sheet: using OAuth2, and manually,by generating a service account key.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the service account key method is the only supported method using the command line.

### Using OAuth2 to authenticate with Google in the Flow web app

* A link to a Google spreadsheet. Simply copy the link from your browser.

* Your Google account username and password.

### Configuring the connector specification manually

* A link to a Google spreadsheet. Simply copy the link from your browser.

* Google Sheets and Google Drive APIs enabled on your Google account.

* A Google service account with:
  * A JSON key generated.
  * Access to the source spreadsheet.

Follow the steps below to meet these prerequisites:

1. [Enable](https://support.google.com/googleapi/answer/6158841?hl=en) the Google Sheets and Google Drive APIs
for the Google [project](https://cloud.google.com/storage/docs/projects) with which your spreadsheet is associated.
(Unless you actively develop with Google Cloud, you'll likely just have one option).

2. Create a [service account and generate a JSON key](https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount).
During setup, grant the account the **Viewer** role on your project.
You'll copy the contents of the downloaded key file into the Service Account Credentials parameter when you configure the connector.

3. Share your Google spreadsheet with the service account. You may either share the sheet so that anyone with the link can view it,
or share explicitly with the service account's email address.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors.
The values and specification sample below provide configuration details specific to the Google Sheets source connector.

### Properties

#### Endpoint

The following properties reflect the Service Account Key authentication method.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Credentials | Google API Credentials for connecting to Google Sheets and Google Drive APIs | object | Required |
| **`/credentials/auth_type`** | Authentication Type | Authentication method. Set to `Service`. | string | Required |
| **`credentials/service_account_info`** | Service Account Credentials | Contents of the JSON key file generated during setup. | string | Required |
| **`/spreadsheet_id`** | Spreadsheet Link | The link to your spreadsheet. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Sheet | Each sheet in your Google Sheets document. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `full_refresh`. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-google-sheets:dev
        config:
            credentials:
              auth_type: Service
              service_account_info: <secret>
            spreadsheet_id: https://docs.google.com/spreadsheets/...
    bindings:
      - resource:
          stream: Sheet1
           syncMode: full_refresh
        target: ${PREFIX}/${COLLECTION_NAME}
```

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)
