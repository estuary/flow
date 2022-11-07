---
sidebar_position: 4
---
# Google BigQuery

This Flow connector materializes Flow collections into tables within a Google BigQuery dataset.
It allows both standard and [delta updates](#delta-updates).

The connector uses your Google Cloud service account to materialize to BigQuery tables by way of files in a Google Cloud Storage (GCS) bucket.
The tables in the bucket act as a temporary staging area for data storage and retrieval.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-bigquery:dev`](https://github.com/estuary/connectors/pkgs/container/materialize-bigquery) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Performance considerations

Like other Estuary connectors, this is a real-time connector that materializes documents using continuous [transactions](../../../concepts/advanced/shards.md#transactions).
However, in practice, there are speed limitations.
Standard BigQuery tables are [limited to 1500 operations per day](https://cloud.google.com/bigquery/quotas#standard_tables).
This means that the connector is limited 1500 transactions per day.

To avoid running up against this limit, you should set the minimum transaction time to a recommended value of 2 minutes,
or a minimum value of 1 minute. You do this by configuring the materialization's [task shard](../../Configuring-task-shards.md). This causes an apparent delay in the materialization, but is necessary to prevent error.
This also makes transactions more efficient, which reduces costs in BigQuery, especially for large datasets.

Instructions to set the minimum transaction time are detailed [below](#shard-configuration).

## Prerequisites

To use this connector, you'll need:

* A [new Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets) in the same region as the BigQuery destination dataset.
* A Google Cloud [service account](https://cloud.google.com/docs/authentication/getting-started) with a key file generated and the following roles:
    * [`roles/bigquery.dataEditor`](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataEditor) on the destination dataset
    * [`roles/bigquery.jobUser`](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser) on the
    project with which the BigQuery destination dataset is associated
    * [`roles/storage.objectAdmin`](https://cloud.google.com/storage/docs/access-control/iam-roles#standard-roles)
    on the GCS bucket created above

    See [Setup](#setup) for detailed steps to set up your service account.
* At least one Flow collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

### Setup

To configure your service account, complete the following steps.

1. Log into the Google Cloud console and [create a service account](https://cloud.google.com/docs/authentication/getting-started#creating_a_service_account).
During account creation:
   1. Grant the user access to the project.
   2. Grant the user roles `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`, and `roles/storage.objectAdmin`.
   3. Click **Done**.

2. Select the new service account from the list of service accounts. On the Keys tab, click **Add key** and create a new JSON key.

   The key is automatically downloaded. You'll use it to configure the connector.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a BigQuery materialization, which will direct one or more of your Flow collections to your desired tables within a BigQuery dataset.

A BigQuery dataset is the top-level container within a project, and comprises multiple tables.
You can think of a dataset as somewhat analogous to a schema in a relational database.
For a complete introduction to resource organization in Bigquery, see the [BigQuery docs](https://cloud.google.com/bigquery/docs/resource-hierarchy).

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/project_id`**| Project ID | The project ID for the Google Cloud Storage bucket and BigQuery dataset.| String | Required |
| `/billing_project_id` | Billing project ID | The project ID to which these operations are billed in BigQuery. Typically, you want this to be the same as `project_id` (the default). | String | Same as `project_id` |
| **`/dataset`** | Dataset | Name of the target BigQuery dataset. | String | Required |
| **`/region`** | Region | The GCS region. | String | Required |
| **`/bucket`** | Bucket | Name of the GCS bucket. | String | Required |
| `/bucket_path` | Bucket path | Base path within the GCS bucket. Also called "Folder" in the GCS console. | String | |
| **`/credentials_json`** | Service Account JSON | The JSON credentials of the service account to use for authorization. | String | Required |

To learn more about project billing, [see the BigQuery docs](https://cloud.google.com/billing/docs/how-to/verify-billing-enabled).

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Table name. | string | Required |
| `/delta_updates` | Delta updates. | Whether to use standard or [delta updates](#delta-updates) | boolean | false |

### Shard configuration

:::info Beta
UI controls for this workflow will be added to the Flow web app soon.
For now, you must edit the materialization config manually, either in the web app or using the CLI.
:::

To avoid exceeding your BigQuery tables' daily operation limits as discussed in [Performance considerations](#performance-considerations),
complete the following steps when configuring your materialization:

1. Using the [Flow web application](../../../guides/create-dataflow.md#create-a-materialization) or the flowctl CLI,
create a draft materialization. Don't publish it yet.

2. Add the [`shards` configuration](../../Configuring-task-shards.md) to the materialization at the same indentation level as `endpoint` and `resource`.
Set the `minTxnDuration` property to at least `1m` (we recommend `2m`).
In the web app, you do this in the catalog editor.

   ```yaml
   shards:
     minTxnDuration: 2m
   ```

   A full sample is included [below](#sample).

3. Continue to test and publish the materialization.

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        config:
          project_id: our-bigquery-project
          dataset: materialized-data
          region: US
          bucket: our-gcs-bucket
          bucket_path: bucket-path/
          credentials_json: <secret>
        image: ghcr.io/estuary/materialize-bigquery:dev
    bindings:
  	- resource:
      	table: ${table_name}
      source: ${PREFIX}/${source_collection}
    shards:
      minTxnDuration: 2m
```

## Delta updates

This connector supports both standard (merge) and [delta updates](../../../concepts/materialization.md#delta-updates).
The default is to use standard updates.

Enabling delta updates will prevent Flow from querying for documents in your BigQuery table, which can reduce latency and costs for large datasets.
If you're certain that all events will have unique keys, enabling delta updates is a simple way to improve
performance with no effect on the output.
However, enabling delta updates is not suitable for all workflows, as the resulting table in BigQuery won't be fully reduced.

You can enable delta updates on a per-binding basis:

```yaml
    bindings:
  	- resource:
      	table: ${table_name}
        delta_updates: true
    source: ${PREFIX}/${source_collection}
```
