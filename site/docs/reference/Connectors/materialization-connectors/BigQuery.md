---
sidebar_position: 4
---
# Google BigQuery

This Flow connector materializes Flow collections into tables within a Google BigQuery dataset.
It allows both standard and [delta updates](#delta-updates).

The connector uses your Google Cloud service account to materialize to BigQuery tables by way of files in a Google Cloud Storage (GCS) bucket.
The tables in the bucket act as a temporary staging area for data storage and retrieval.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-bigquery:dev`](https://github.com/estuary/connectors/pkgs/container/materialize-bigquery) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

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

The Flow collections you materialize must accommodate the following naming restrictions:

  * Field names may not contain hyphens (`-`), or the materialization will fail.
  * Field names must begin with a letter or underscore (`_`), or the materialization will fail.
  * Field names *may* contain non-alphanumeric characters, but these are replaced with underscores in the corresponding BigQuery column name.
  * If two field names become identical after special characters are replaced with underscores (for example, `field!` and `field$` both become `field_`), the materialization will fail.
  * Collection names *may* contain non-alphanumeric characters, but all such characters except hyphens are replaced with underscores in the BigQuery table name.

If necessary, you can add [projections](../../../concepts/advanced/projections.md) to your collection specification to change field names.

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
| **`/credentials_json`** | Service Account JSON | The JSON credentials of the service account to use for authorization. | String | Required |
| **`/region`** | Region | The GCS region. | String | Required |
| **`/dataset`** | Dataset | BigQuery dataset for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables. | String | Required |
| **`/bucket`** | Bucket | Name of the GCS bucket. | String | Required |
| `/bucket_path` | Bucket path | Base path within the GCS bucket. Also called "Folder" in the GCS console. | String | |
| `/billing_project_id` | Billing project ID | The project ID to which these operations are billed in BigQuery. Typically, you want this to be the same as `project_id` (the default). | String | Same as `project_id` |
| `/advanced`                     | Advanced Options    | Options for advanced users. You should not typically need to modify these.                                                                  | object  |                            |
| `/advanced/updateDelay`     | Update Delay    | Potentially reduce compute time by increasing the delay between updates. Defaults to 30 minutes if unset. | string  |  |

To learn more about project billing, [see the BigQuery docs](https://cloud.google.com/billing/docs/how-to/verify-billing-enabled).

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Table in the BigQuery dataset to store materialized result in. | string | Required |
| `/dataset` | Table | Alternative dataset for this table. Must be located in the region set in the endpoint configuration. | string |  |
| `/delta_updates` | Delta updates. | Whether to use standard or [delta updates](#delta-updates) | boolean | false |

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
