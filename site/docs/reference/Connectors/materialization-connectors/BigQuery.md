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
    * [`roles/bigquery.jobUser`](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser) on the
    project with which the BigQuery destination dataset is associated
    * [`roles/storage.objectAdmin`](https://cloud.google.com/storage/docs/access-control/iam-roles#standard-roles)
    on the GCS bucket created above

## Configuration

To use this connector, begin with a Flow catalog that has at least one **collection**. You'll add a BigQuery materialization, which will direct one or more of your Flow collections to your desired BigQuery datasets. Follow the basic [materialization setup](../../../concepts/materialization.md#specification) and add the required BigQuery configuration values per the table below.

### Values

| Value | Name | Type | Required/Default | Details |
|-------|------|------|---------| --------|
| `ProjectID`| Project ID | String | Required | The project ID for the Google Cloud Storage bucket and BigQuery dataset|
| `Dataset` | Dataset | String | Required | ??????|
| `Region` | Region | String | Required | The GCS region |
| `Bucket` | Bucket | string | Required | Name of the GCS bucket |
| `BucketPath` | Bucket Path | String | Required | Base path to the GCS bucket |
| `CredentialsFile` | Credentials File | String | * | Path to a JSON service account file |
| `CredentialsJSON` | Credentials JSON | Byte | * | Base64-encoded string of the full service account file |

*One of `CredentialsFile` or `CredentialsJSON` is required. If both are provided, the connector will try
to use `CredentialsFile` first.

### Sample

```yaml
# If this is the first materialization, add the section to your catalog spec
materializations:
  ${tenant}/${mat_name}:
	  endpoint:
  	    connector:
    	    config:
               FILL IN HERE!!!!!!!!!!!!!!
    	    image: ghcr.io/estuary/materialize-bigquery:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	workspace: ${namespace_name}
      	collection: ${table_name}
    source: ${tenant}/${source_collection}
    ```