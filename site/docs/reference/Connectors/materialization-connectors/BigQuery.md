This Flow connector materializes Flow collections into Google BigQuery datasets. It allows both standard and [delta updates](../../../concepts/materialization.md#delta-updates).

The connector uses a service account to materialize to BigQuery by way of tables in Google Cloud Storage (GCS).
These tables act as a temporary staging area for data storage and retrieval.

`ghcr.io/estuary/materialize-bigquery:dev` provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* An existing catalog spec that includes at least one collection with its schema specified
* A [new Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets) in the same region as the BigQuery destination dataset.
* A Google Cloud [service account](https://cloud.google.com/docs/authentication/getting-started) with a key file generated and the following roles:
    * [`roles/bigquery.dataEditor`](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataEditor) on the destination dataset
    * [`roles/bigquery.jobUser`](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser) on each
    project with which the BigQuery destination is associated
    * ['roles/storage.objectAdmin'](https://cloud.google.com/storage/docs/access-control/iam-roles#standard-roles)
    on the GCS bucket created above