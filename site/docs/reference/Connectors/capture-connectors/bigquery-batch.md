# BigQuery Batch Query Connector

This connector captures data from BigQuery into Flow collections by periodically
executing queries and translating the results into JSON documents.

## Prerequisites

To use this connector, you will need the following prerequisites:

* A Google Cloud Project with BigQuery enabled
* A Google Cloud Service Account with the "BigQuery User" and "BigQuery Data Viewer" roles in your GCP project
* A Service Account Key to authenticate into your Service Account

See the setup guide for more information about how to create the required resources.

## Setup

Follow the steps below to set up the BigQuery connector.

### Service Account

To sync data from BigQuery, you need credentials for a Service Account with the "BigQuery User" and "BigQuery Data Viewer" roles. These roles grant the necessary permissions to run BigQuery jobs, discover tables within the dataset, and read the contents of those tables. It is recommended to create a dedicated Service Account to facilitate permission management and auditing. However, if you already have a Service Account with the correct permissions, you can use it.

Here's how to provision a suitable service account:

1. Follow Google Cloud Platform's instructions for [Creating a Service Account](https://cloud.google.com/iam/docs/service-accounts-create#creating).
2. Note down the ID of the service account you just created. Service Account IDs typically follow the format `<account-name>@<project-name>.iam.gserviceaccount.com`.
3. Follow Google Cloud Platform's instructions for [Granting IAM Roles](https://cloud.google.com/iam/docs/grant-role-console#grant_an_iam_role) to the new service account. The "principal" email address should be the ID of the service account you just created, and the roles granted should be "BigQuery User" and "BigQuery Data Viewer".

### Service Account Key

Service Account Keys are used to authenticate as Google Service Accounts. To be able to utilize the permissions granted to the Service Account in the previous step, you'll need to provide its Service Account Key when creating the capture. It is a good practice, though not required, to create a new key for Flow even if you're reusing a preexisting account.

To create a new key for a service account, follow Google Cloud Platform's instructions for [Creating a Service Account Key](https://cloud.google.com/iam/docs/keys-create-delete#creating). Be sure to create the key in JSON format. Once the linked instructions have been followed you should have a key file, which will need to be uploaded to Flow when setting up your capture.

### Set up the BigQuery connector in Estuary Flow

1. Log into your Estuary Flow account.
2. In the left navigation bar, click on "Sources". In the top-left corner, click "New Capture".
3. Locate and select the "BigQuery" connector.
4. Enter a name and optional description for the capture task.
5. Enter the Project ID and Dataset name that you intend to capture from, and paste or upload the service account key in the appropriate field.
6. Click the "Next" button and wait while the connector automatically discovers the available tables in the specified project and dataset.
7. Select the tables you wish to capture from the bindings list.
8. For each binding you selected, you will likely wish to [specify cursor columns](#specifying-cursor-columns) and a shorter "Poll Interval" setting. Otherwise the default behavior will be to recapture the entire contents of the table, once per day.
9. Once you are satisfied with your binding selection, click the "Save and Publish" button.

### Specifying Cursor Columns

This connector operates by periodically executing a `SELECT * FROM table` query and
outputting the resulting rows as JSON documents into a Flow collection. In some cases
doing this once or twice a day is entirely sufficient, but when working with larger
tables (or if a faster update rate is desired) it pays to manually configure cursor
columns.

The cursor must be a column (or ordered tuple of columns) which is expected to strictly
increase for newly added or updated rows. Common examples of suitable cursors include:

  - Update timestamps, which are often the best choice if available since they can
    often be used to identify changed rows as well as new insertions.
  - Creation timestamps, which can be used to identify newly added rows in append-only
    datasets but won't help to identify changes to preexisting rows.
  - Monotonically increasing IDs, which are another way of identifying newly added rows
    but often don't help with update detection.

When a cursor is specified, the update query will take the form `SELECT * FROM $table WHERE $cursorName > $lastCursorValue ORDER BY $cursorName`
and the capture connector will keep track of the highest observed cursor value between polling intervals.
If multiple cursor columns are specified, they will be treated as an ordered tuple of columns which
collectively form the cursor, and the obvious lexicographic tuple ordering will apply.

Once you have specified a suitable cursor for a table, you will likely want to lower the
polling interval for that binding. The default polling interval is `"24h"` to keep data
volumes low, but once a cursor is specified there is usually no downside to frequent
polling, so you may wish to lower the interval to `"5m`" or even `"5s"` for that table.

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the BigQuery source connector.

### Properties

#### Endpoint

| Property            | Title            | Description                                                               | Type   | Required/Default |
| ------------------- | ---------------- | ------------------------------------------------------------------------- | ------ | ---------------- |
| `/project_id`       | Project ID       | The GCP project ID for the project containing the source BigQuery dataset | string | Required         |
| `/dataset`          | Dataset          | The BigQuery dataset to discover tables within                            | string | Required         |
| `/credentials_json` | Credentials JSON | The contents of your Service Account Key JSON file                        | string | Required         |
| `/advanced/poll`    | Poll Interval    | How often to poll bindings (may be overridden for a specific binding)     | string | `"24h"` |

#### Bindings

| Property        | Title          | Description                                                               | Type   | Required/Default |
| --------------- | -------------- | ------------------------------------------------------------------------- | ------ | ---------------- |
| **`/name`**     | Name           | A name which uniquely identifies this binding.                            | string | Required         |
| **`/cursor`**   | Cursor         | The column name(s) which should be used as the incremental capture cursor | array  | []               |
| **`/template`** | Template       | The query (template) which will be executed every polling interval        | string | Required         |
| **`/poll`**     | Poll Interval  | Override the global polling interval for this binding.                    | string | ""               |

### Query Templates

The query template property of a binding defines what query will be executed against
the database, given inputs describing the configured cursor columns and whether any prior
cursor state exists. The default template implements the behavior described in
[specifying cursor columns](#specifying-cursor-columns).

In principle you are free to modify this template to implement whatever query you need.
You could for instance create a new binding which queries a view, or which performs a
more complex analytics query. However this should not be combined with table auto-discovery
in a single capture, as this can produce some counterintuitive results. Instead create two
separate capture tasks from the same database, one for autodiscovered tables and a separate
one with the setting "Automatically Add New Collections" disabled for your custom bindings.