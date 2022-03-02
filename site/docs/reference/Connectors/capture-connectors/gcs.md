---
sidebar_position: 4
---
# Google Cloud Storage

This connector captures data from an Google Cloud Storage (GCS) bucket.

[`ghcr.io/estuary/source-gcs:dev`](https://ghcr.io/estuary/source-gcs:dev) provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, either your GCS bucket must be public, or you must have access via a Google service account.

* For public buckets, verify that objects in the bucket are [publicly readable](https://cloud.google.com/storage/docs/access-control/making-data-public).
* For buckets accessed by a Google Service Account:
    * Ensure that the user has been assigned a [role](https://cloud.google.com/iam/docs/understanding-roles) with read access.
    * Create a [JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating). Google's [Application Default Credentials](https://cloud.google.com/docs/authentication/production) will use this file for authentication.

## Configuration

There are various ways to configure and implement connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and code sample in this section provide configuration details specific to the GCS source connector.

:::tip
You might use [prefixes](https://cloud.google.com/storage/docs/samples/storage-list-files-with-prefix) to organize your GCS bucket
in a way that emulates a directory structure.
This connector can use prefixes in various ways, giving you precise control over how datasets are captured into Flow.
You can specify a prefix in the endpoint configuration to limit the overall scope of the capture within your bucket.
You'll also specify prefixes on a per-binding basis, allowing you to map each prefix to a distinct Flow collection.

Alternatively, you can capture the entire bucket by omitting `prefix` in the endpoint configuration and
setting `stream` to the name of the bucket.
:::

### Values

#### Endpoint

| Value | Name| Description | Type | Required/Default |
|---|---|---|---|---|
| `ascendingKeys`| Ascending Keys | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix.* | boolean | false |
| `bucket` | Bucket | Name of the GCS bucket. | string | Required |
| `googleCredentials` | Google Service Account | Service account JSON file. Required unless the bucket is public.| object | |
| `matchKeys` | Match Keys | Regex filter applied to all object keys under the prefix. Only objects whose absolute path match are read. For example, the match key `".*\\.json\"` captures only JSON files. | string |  |
| `prefix` | Prefix | Prefix within the bucket to capture from. | string | |

*To use ascending keys, you must write objects in ascending lexicographic order, such as using RFC-3339 timestamps to record modification times.
This ensures that key ordering matches the order of changes.

#### Bindings

| Value | Name| Description | Type | Required/Default |
|---|---|---|---|---|
| `stream` | Prefix | Path to dataset in the bucket, formatted as `bucket-name/prefix-name` | string | Required |
| `syncMode` | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample

A minimal capture definition within the catalog spec will look like the following:

```yaml
captures:
  ${TENANT}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-gcs:dev
        config:
          bucket: "my-bucket"
    bindings:
      - resource:
          stream: my-bucket/${PREFIX}
          syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}

```

Your capture definition may be more complex, with additional bindings for different GCS prefixes within the same bucket.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)