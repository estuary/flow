---
sidebar_position: 1
---
# BigQuery Connector Documentation

This connector captures data from BigQuery into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, ghcr.io/estuary/source-bigquery:dev provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites
To set up the BigQuery connector, you need the following prerequisites:

* A Google Cloud Project with BigQuery enabled
* A Google Cloud Service Account with the "BigQuery User" and "BigQuery Data Editor" roles in your GCP project
* A Service Account Key to authenticate into your Service Account
See the setup guide for more information about how to create the required resources.

## Setup
Follow the steps below to set up the BigQuery connector.

### Service Account
To sync data from BigQuery, you need credentials for a Service Account with the "BigQuery User" and "BigQuery Data Editor" roles. These roles grant the necessary permissions to run BigQuery jobs, write to BigQuery Datasets, and read table metadata. It is recommended to create a dedicated Service Account to facilitate permission management and auditing. However, if you already have a Service Account with the correct permissions, you can use it.

***Here's how to create a Service Account:***

1. Follow Google Cloud Platform's guide for Creating a Service Account.
2. Note down the ID of the Service Account as you will need to reference it later when granting roles. Service Account IDs typically follow the format <account-name>@<project-name>.iam.gserviceaccount.com.
3. Add the Service Account as a Member in your Google Cloud Project with the "BigQuery User" role. Refer to the instructions for Granting Access in the Google documentation. The email address of the member you add should be the same as the Service Account ID you created earlier.

By now, you should have a Service Account with the "BigQuery User" project-level permission.

### Service Account Key
Service Account Keys are used to authenticate as Google Service Accounts. To be able to utilize the permissions granted to the Service Account in the previous step, you'll need to provide its Service Account Key.

***Follow the steps below to create a key:***
1. Refer to the Google documentation for Creating and Managing Service Account Keys.
2. Make sure to create the key in JSON format.
3. Once you've created the key, download it immediately, as Google will allow you to see its contents only at that moment.

### Set up the BigQuery connector in Estuary Flow

1. Log into your Estuary Flow account.
2. In the left navigation bar, click on "Captures". In the top-left corner, click "Connector Search".
3. Enter the name for the WooCommerce connector and select "BigQuery" from the dropdown.
4. Enter a Primary Key using the standard form editor and mark it as "required" before doing so.
5. Enter the Project ID and Credentials JSON.

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the BigQuery source connector.

### Properties

#### Endpoint
| Property            | Title            | Description                                                               | Type   | Required/Default |
| ------------------- | ---------------- | ------------------------------------------------------------------------- | ------ | ---------------- |
| `/project_id`       | Project ID       | The GCP project ID for the project containing the target BigQuery dataset | string | Required         |
| `/credentials_json` | Credentials JSON | The contents of your Service Account Key JSON file.                       | string | Required         |

#### Bindings

| Property        | Title     | Description                                                            | Type   | Required/Default |
| --------------- | --------- | ---------------------------------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your BigQuery project from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                     | string | Required         |


### Sample

```json
{
  "properties": {
    "project_id": {
      "order": 1
    },
    "credentials_json": {
      "order": 2,
      "description": "The contents of your Service Account Key JSON file. See https://go.estuary.dev/bigquery for more information on how to obtain this key.",
      "multiline": true
    }
  }
}
```