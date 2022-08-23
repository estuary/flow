---
sidebar_position: 5
---

# Google Sheets

This connector materializes Flow collections into sheets in a Google Sheets spreadsheet.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-google-sheets:dev`](https://ghcr.io/estuary/materialize-google-sheets:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* At least one Flow collection.

  If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.

* The Google spreadsheet URL.

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
You'll copy the contents of the downloaded key file into the Google Service Account parameter when you configure the connector.

3. Share your Google spreadsheet with the service account, granting edit access.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Google Sheets materialization.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/googleCredentials`** | Google Service Account | Service account JSON key to use as Application Default Credentials | string | Required |
| **`/spreadsheetURL`** | Spreadsheet URL | URL of the spreadsheet to materialize into, which is shared with the service account. | string | Required |

#### Bindings

Configure a separate binding for each collection you want to materialize to a sheet.
Note that the connector will add an addition column to the beginning of each sheet;
this is to track the internal state of the data.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/sheet`** | Sheet Name | Name of the spreadsheet sheet to materialize into | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
	  endpoint:
        connector:
          config:
            googleCredentials: <secret>
            spreadsheetID: <string>
          image: ghcr.io/estuary/materialize-google-sheets:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
      - resource:
          sheet: my_sheet
      source: ${PREFIX}/${source_collection}
```