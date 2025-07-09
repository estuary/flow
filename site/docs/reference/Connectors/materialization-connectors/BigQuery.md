
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
    * [`roles/bigquery.readSessionUser`](https://cloud.google.com/bigquery/docs/access-control#bigquery.readSessionUser) on the
    project with which the BigQuery destination dataset is associated
    * [`roles/storage.objectAdmin`](https://cloud.google.com/storage/docs/access-control/iam-roles#standard-roles)
    on the GCS bucket created above

    See [Setup](#setup) for detailed steps to set up your service account.

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

### Setup

To configure your service account, complete the following steps.

1. Log into the Google Cloud console and [create a service account](https://cloud.google.com/docs/authentication/getting-started#creating_a_service_account).
During account creation:
   1. Grant the user access to the project.
   2. Grant the user roles `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`, `roles/bigquery.readSessionUser` and `roles/storage.objectAdmin`.
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
| `/advanced/disableFieldTruncation` | Disable Field Truncation | Disables truncation of large materialized fields | boolean | |

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

## Sync Schedule

This connector supports configuring a schedule for sync frequency. You can read
about how to configure this [here](../../materialization-sync-schedule.md).

## Storage Read API

This connector is able to use the [BigQuery Storage Read
API](https://cloud.google.com/bigquery/docs/reference/storage) for reading
results of queries executed for standard updates bindings. For optimal
performance, the **BigQuery Read Session User** role should be granted to the
configured service account to enable using the storage read API.

If the **BigQuery Read Session User** role is not available, slower mechanisms
will be used to read query results.

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

## Table Partitioning

Tables are automatically created with
[clustering](https://cloud.google.com/bigquery/docs/clustered-tables) based on the Flow collection
primary keys. Tables are not created with any other [partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables), but pre-existing partitioned tables can be materialized to.

It isn't possible to alter the partitioning of an existing table, but you can convert an existing table to one with partitioning by creating a new table and copying the data from the existing table into it. This can be done to tables that the connector is materializing to, as long as the materializing task is temporarily disabled while doing the conversion.

To convert an existing materialized table to one with different partitioning:
1. Pause your materialization by disabling it from the [UI](../../../concepts/web-app.md) or editing the task specification with the [CLI](../../../guides/flowctl/edit-specification-locally.md).
2. Create a new table with the partitioning you want from the data in the existing table:
```sql
create table <your_dataset>.<your_schema>.<your_table>_copy
partition by <your_partitioning>
as select * from <your_dataset>.<your_schema>.<your_table>;
```
3. Verify that the data in `<your_table>_copy` looks good, then drop the original table:
```sql
drop table <your_dataset>.<your_schema>.<your_table>;
```
4. "Rename" `<your_table>_copy` back to `<your_table>` by copying it as a new table with the original name of `<your_table>`:
```sql
create table <your_dataset>.<your_schema>.<your_table> copy <your_dataset>.<your_schema>.<your_table>_copy;
```
5. Verify that the data in `<your_table>` looks good, then drop the `<your_table>_copy` table:
```sql
drop table <your_dataset>.<your_schema>.<your_table>_copy;
```
6. Re-enable the materialization to continue materializing data to the now partitioned table.
