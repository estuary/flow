This Flow connector materializes Flow collections into tables within a Google BigQuery dataset.
It allows both standard and [delta updates](#delta-updates).

The connector uses your Google Cloud service account to materialize to BigQuery tables by way of files in a Google Cloud Storage (GCS) bucket.
The tables in the bucket act as a temporary staging area for data storage and retrieval.

`ghcr.io/estuary/materialize-bigquery:dev` provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* An existing catalog spec that includes at least one collection
* A [new Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets) in the same region as the BigQuery destination dataset.
* A Google Cloud [service account](https://cloud.google.com/docs/authentication/getting-started) with a key file generated and the following roles:
    * [`roles/bigquery.dataEditor`](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataEditor) on the destination dataset
    * [`roles/bigquery.jobUser`](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser) on the
    project with which the BigQuery destination dataset is associated
    * [`roles/storage.objectAdmin`](https://cloud.google.com/storage/docs/access-control/iam-roles#standard-roles)
    on the GCS bucket created above

## Configuration

To use this connector, begin with a Flow catalog that has at least one collection.
You'll add a BigQuery materialization, which will direct one or more of your Flow collections to your desired tables within a BigQuery dataset.
Follow the basic [materialization setup](../../../concepts/materialization.md#specification) and add the required BigQuery configuration values per the table below.

This configuration assumes a working knowledge of resource organization in BigQuery.
You can find introductory documentation in the [BigQuery docs](https://cloud.google.com/bigquery/docs/resource-hierarchy).

### Values

| Value | Name | Type | Required/Default | Details |
|-------|------|------|---------| --------|
| `project_id`| Project ID | String | Required | The project ID for the Google Cloud Storage bucket and BigQuery dataset|
| `billing_project_id` | Billing project ID | String | Same as `project_id` | The project ID to which these operations are billed in BigQuery* |
| `dataset` | Dataset | String | Required | Name of the target BigQuery dataset |
| `region` | Region | String | Required | The GCS region |
| `bucket` | Bucket | string | Required | Name of the GCS bucket |
| `bucket_path` | Bucket Path | String | Required | Base path within the GCS bucket |
| `credentials_json` | Credentials JSON | Byte | Required | Base64-encoded string of the full service account file |

*Typically, you want this to be the same as `project_id` (the default).
To learn more about project billing, [see the BigQuery docs](https://cloud.google.com/billing/docs/how-to/verify-billing-enabled).

### Sample

```yaml
# If this is the first materialization, add the section to your catalog spec
materializations:
  ${tenant}/${mat_name}:
	  endpoint:
  	    connector:
    	    config:
              project_ID: our-bigquery-project
              dataset: materialized-data
              region: US
              bucket: our-gcs-bucket
              bucket_path: bucket-path/
              credentials_json: SSBqdXN0IHdhbm5hIHRlbGwgeW91IGhvdyBJJ20gZmVlbGluZwpHb3R0YSBtYWtlIHlvdSB1bmRlcnN0YW5kCk5ldmVyIGdvbm5hIGdpdmUgeW91IHVwCk5ldmVyIGdvbm5hIGxldCB5b3UgZG93bgpOZXZlciBnb25uYSBydW4gYXJvdW5kIGFuZCBkZXNlcnQgeW91Ck5ldmVyIGdvbm5hIG1ha2UgeW91IGNyeQpOZXZlciBnb25uYSBzYXkgZ29vZGJ5ZQpOZXZlciBnb25uYSB0ZWxsIGEgbGllIGFuZCBodXJ0IHlvdQ==
    	    image: ghcr.io/estuary/materialize-bigquery:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	table: ${table_name}
    source: ${tenant}/${source_collection}
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
    source: ${tenant}/${source_collection}
```
