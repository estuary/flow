# Google Analytics 4 Bigquery Exports

This connector captures data from Google Analytics 4 BigQuery exports into
Estuary collections. It enumerates and captures the daily batch export tables
produced by GA4's BigQuery linkage (`events_YYYYMMDD`, `users_YYYYMMDD`, and
`pseudonymous_users_YYYYMMDD`).

## Prerequisites

- A Google Cloud Project with BigQuery enabled
- A GA4 to BigQuery export link configured for that property
- A Google Cloud Service Account with the "BigQuery User" and "BigQuery Data Viewer" roles
- A Service Account Key or a configured Workload Identity Pool to authenticate as that Service Account

## Setup

### Step 1: Link GA4 to BigQuery

1. **Access Google Analytics:** Sign in to your Google Analytics account at [analytics.google.com](https://analytics.google.com/).

2. **Navigate to BigQuery Linking:**
   - In the Admin section, find the property you want to link to BigQuery.
   - Under the property column, click on **BigQuery Linking**.

3. **Create a Link:**
   - Click on the **Link** button and follow the prompts.
   - Select the Google Cloud Project where your BigQuery dataset is located. You may need to provide necessary permissions if you haven't done so already.

4. **Configure Export Options:**
   - Choose between **Daily Export** and **Streaming Export** options.
   - This connector reads the daily `events_YYYYMMDD` tables, which both options produce.
   - Streaming Export additionally produces `events_intraday_*` tables, which this connector does not capture.

5. **Review and Submit:**
   - Review the settings, including your dataset name and export frequency.
   - Click **Submit** to complete the linking process.

For full details, refer to the official Google documentation on [Setting up BigQuery Export for GA4](https://support.google.com/analytics/answer/9358801).

### Step 2: Provision a Service Account

To sync data from BigQuery, the connector needs credentials for a Service Account with the "BigQuery User" and "BigQuery Data Viewer" roles. These roles grant the necessary permissions to run BigQuery jobs, discover tables within the dataset, and read the contents of those tables. It is recommended to create a dedicated Service Account to facilitate permission management and auditing. However, if you already have a Service Account with the correct permissions, you can use it.

Here's how to provision a suitable service account:

1. Follow Google Cloud Platform's instructions for [Creating a Service Account](https://cloud.google.com/iam/docs/service-accounts-create#creating).
2. Note down the ID of the service account you just created. Service Account IDs typically follow the format `<account-name>@<project-name>.iam.gserviceaccount.com`.
3. Follow Google Cloud Platform's instructions for [Granting IAM Roles](https://cloud.google.com/iam/docs/grant-role-console#grant_an_iam_role) to the new service account. The "principal" email address should be the ID of the service account you just created, and the roles granted should be "BigQuery User" and "BigQuery Data Viewer".

You can then authenticate the connector as this Service Account using either a Service Account Key or Google Cloud IAM.

#### Service Account Key Authentication

A Service Account Key is the simplest authentication method. You create a JSON key for the Service Account and paste its contents into the connector configuration. It is a good practice, though not required, to create a new key for Estuary even if you're reusing a preexisting service account.

To create a new key for a service account, follow Google Cloud Platform's instructions for [Creating a Service Account Key](https://cloud.google.com/iam/docs/keys-create-delete#creating). Be sure to create the key in JSON format. Once the linked instructions have been followed you should have a key file, which will need to be uploaded to Estuary when setting up your capture.

#### Google Cloud IAM Authentication

Google Cloud IAM authentication uses a Workload Identity Pool, which lets the connector authenticate without storing any long-lived secret. To use this method, follow the steps in the [GCP IAM guide](/guides/iam-auth/gcp/) and note the Workload Identity Pool audience and the Service Account email.

### Step 3: Set up the connector in Estuary

1. Log into your Estuary account.
2. In the left navigation bar, click **Sources**, then in the top-left corner click **New Capture**.
3. Locate and select the **Google Analytics 4 BigQuery** connector.
4. Enter a name and optional description for the capture task.
5. Enter the Project ID for the project containing the GA4 export dataset, and provide the Service Account credentials. Optionally specify a Dataset to restrict discovery to a single dataset, or leave it unset to discover GA4 exports across every dataset in the project.
6. Click **Next** and wait for the connector to discover available bindings. The connector emits one binding per (dataset, stream type) pair that has matching tables.
7. Select the bindings you want to capture and adjust per-binding settings if needed.
8. Once you are satisfied with your binding selection, click **Save and Publish**.

## Usage

The connector polls daily by default, at noon UTC. There is also an advanced option to set your own polling schedule. On each polling cycle it inspects the dataset's available daily tables and decides what to read:

- Tables outside the **live window** (the most recent N days, four by default)
  are captured exactly once and never re-queried.
- The newest table in the window is captured promptly, so fresh data lands
  quickly.
- The oldest table in the window is captured a second time when it rotates out
  of the window. This catches any late-arriving events that GA4 wrote into that
  day's table after the first capture. GA4 documents that meaningful late
  events arrive within 72 hours, which is why the default window is four days.
- Intermediate tables in the window (between the newest and oldest) are skipped
  by default.

This means each daily table is queried twice in the steady state (once when fresh, once as it ages out), and the cost of the second pass is bounded by the table size for that single day.

:::warning Limitations
- Streaming intraday tables (`events_intraday_*`) are not captured.
- GA360 Fresh Daily tables (`events_fresh_*`) are not captured.
- Tables that GA4 modifies after they have rotated out of the live window are not re-captured. Set a larger **Window Days** value if your property regularly produces meaningful updates beyond 72 hours.
- The initial polling cycle backfills every dataset table not excluded by **Minimum Date**, which can scan many terabytes for high-traffic properties. Estimate the cost before enabling the connector and use **Minimum Date** to bound it.
:::

### Capturing Intermediate Days

If you need fresher data on the days between the newest and oldest tables in the live window, set the **Capture Intermediate Days** advanced option to `true`. With this enabled, every table in the live window is re-queried on each polling cycle. This trades higher BigQuery scan cost (proportional to window size) for fresher intermediate-day data.

### Bounding Initial Backfill Cost

By default the connector will backfill every daily table that exists in the dataset. For high-traffic GA4 properties this can mean scanning many terabytes of historical data. To bound that cost, set the **Minimum Date** advanced option to a `YYYY-MM-DD` cutoff. Tables with dates before that cutoff are skipped entirely.

### Billing to a Different Project

By default, BigQuery jobs are billed to the same project that owns the dataset. To bill BigQuery jobs to a different project (for example, when reading from a dataset you do not own), set the **Billing Project ID** advanced option to that project's ID. The Service Account must have the "BigQuery User" role in the billing project.

## Configuration

Configure this connector in the Estuary web app or using YAML config files with
[flowctl CLI](/guides/flowctl/). See [connectors](/concepts/connectors/#using-connectors)
to learn more about using connectors.

### Endpoint Properties

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/project_id`** | Project ID | The GCP project ID that owns the BigQuery dataset(s) containing the GA4 exports. | string | Required |
| `/dataset` | Dataset | The specific BigQuery dataset containing GA4 exports. If unset, all datasets in the project are discovered for matching tables, in which case the credentials must have permission to list datasets in the project. | string | |
| **`/credentials`** | Authentication | Credentials for authenticating with GCP. | [Credentials](#credentials) | Required |
| `/advanced/poll` | Polling Schedule | When and how often to execute the polling cycle. Accepts a Go duration string like `"24h"` or a string like `"daily at 12:34Z"` to poll at a specific time (in UTC) every day. | string | `"daily at 12:00Z"` |
| `/advanced/window_days` | Window Days | Number of recent daily tables treated as the live window. Each cycle final-captures the oldest table in this window and primary-captures the newest. | integer | `4` |
| `/advanced/capture_intermediate` | Capture Intermediate Days | If enabled, the connector queries every table in the live window on each poll rather than just the newest and oldest. Trades higher BigQuery scan cost for fresher intermediate-day data. | boolean | `false` |
| `/advanced/min_date` | Minimum Date | Optional `YYYY-MM-DD` cutoff. Tables for dates strictly before this are skipped. Used to bound the cost of an initial backfill. | string | |
| `/advanced/source_tag` | Source Tag | When set, the capture adds this value as the `tag` property in the source metadata of each document. | string | |
| `/advanced/billing_project_id` | Billing Project ID | Project that BigQuery jobs are billed to. Defaults to Project ID if not specified. | string | |

### Credentials

Credentials for authenticating with GCP. Use one of the following sets of options:

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/auth_type`** | Auth Type | Method to use for authentication. | string | Required: `CredentialsJSON` |
| **`/credentials_json`** | Service Account JSON | The Service Account JSON credentials to use for authorization. | string | Required |

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/auth_type`** | Auth Type | Method to use for authentication. | string | Required: `GCPIAM` |
| **`/gcp_service_account_to_impersonate`** | Service Account | GCP Service Account email to impersonate. | string | Required |
| **`/gcp_workload_identity_pool_audience`** | Workload Identity Pool Audience | GCP Workload Identity Pool Audience in the format `https://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider`. | string | Required |

### Binding Properties

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/dataset`** | Dataset | The BigQuery dataset containing the GA4 export tables for this binding. | string | Required |
| **`/stream_type`** | Stream Type | Which GA4 logical stream this binding represents. One of `events`, `users`, or `pseudonymous_users`. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-ga4-bigquery:v1
        config:
          project_id: my-gcp-project
          dataset: analytics_1234
          credentials:
            auth_type: CredentialsJSON
            credentials_json: <service account JSON credentials>
    bindings:
      - resource:
          dataset: analytics_1234
          stream_type: events
        target: ${PREFIX}/${COLLECTION_NAME}
```
