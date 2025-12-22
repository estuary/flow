

# Google Sheets

This connector materializes Estuary collections into sheets in a Google Sheets spreadsheet.

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-google-sheets:dev`](https://ghcr.io/estuary/materialize-google-sheets:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* At least one Estuary collection.

  If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.

:::caution
For performance reasons, this connector is limited to 1 million cells per materialized sheet.
If a bound collection has more than 1 million unique keys, the materialization will fail.

If you plan to materialize a collection with an unbounded number of keys,
you should first use a [derivation](../../../guides/flowctl/create-derivation.md) to summarize it
into a collection with a bounded set of keys.
:::

* The URL of a Google spreadsheet that *does not* contain the output of a prior Estuary materialization.

:::caution
Materializing data to a spreadsheet that already contains the output of another Estuary materialization can result in an error.
Use a new spreadsheet for each materialization, or completely clear the output of prior materializations from the spreadsheet before you continue.
:::

There are two ways to authenticate with Google when using this connector:
signing in with Google through OAuth in the web app, and configuring manually with a Google service account key.
OAuth is simpler, and is recommended when using the web app.
Only manual configuration is supported using the CLI.

Additional prerequisites depend on the authentication method you choose.

### OAuth authentication using the Estuary web app

You'll need:

* The username and password of a Google account with edit access to the destination spreadsheet.

### Manual authentication

You'll need:

* Google Sheets and Google Drive APIs enabled on your Google account.

* A Google service account with:
  * A JSON key generated.
  * Edit access to the destination spreadsheet.

Follow the steps below to meet these prerequisites:

1. [Enable](https://support.google.com/googleapi/answer/6158841?hl=en) the Google Sheets and Google Drive APIs
for the Google [project](https://cloud.google.com/storage/docs/projects) with which your spreadsheet is associated.
(Unless you actively develop with Google Cloud, you'll likely just have one option).

2. Create a [service account and generate a JSON key](https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount).
During setup, grant the account the **Editor** role on your project.
You'll copy the contents of the downloaded key file into the Service Account JSON parameter when you configure the connector.

3. Share your Google spreadsheet with the service account, granting edit access.

## Configuration

To use this connector, begin with data in one or more Estuary collections.
Use the below properties to configure a Google Sheets materialization.

### Properties

#### Endpoint

The following properties reflect the manual authentication method. If you're working in the Estuary web app, you can use [OAuth](#oauth-authentication-using-the-estuary-web-app), so some of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Authentication | Credentials used to authenticate with Google. | array, boolean, null, number, object, string | Required |
| `/credentials/auth_type` | Authentication type | Set to `Service` for manual authentication, or use OAuth in the web app.  | string |  |
| **`/credentials/credentials_json`** | Service Account JSON | The JSON key of the service account to use for authorization, when using the `Service` authentication method. | string | Required |
| **`/spreadsheetURL`** | Spreadsheet URL | URL of the spreadsheet to materialize into, which is shared with the service account. | string | Required |

#### Bindings

Configure a separate binding for each collection you want to materialize to a sheet.
Note that the connector will add an addition column to the beginning of each sheet;
this is to track the internal state of the data.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/sheet`** | Sheet Name | Name of the spreadsheet sheet to materialize into | string | Required |

### Sample

This sample reflects the [manual authentication](#manual-authentication) method using the CLI.

```yaml
materializations:
  ${PREFIX}/${mat_name}:
	  endpoint:
        connector:
          config:
            credentials:
              auth_type: Service
              credentials_json: <secret>
            spreadsheetURL: `https://docs.google.com/spreadsheets/d/<your_spreadsheet_ID>/edit
          image: ghcr.io/estuary/materialize-google-sheets:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
      - resource:
          sheet: my_sheet
      source: ${PREFIX}/${source_collection}
```
