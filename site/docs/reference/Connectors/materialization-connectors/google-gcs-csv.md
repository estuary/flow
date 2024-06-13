
# CSV Files in Google GCS

This connector materializes [delta updates](../../../concepts/materialization.md#delta-updates) of
Flow collections into files in a GCS bucket per the CSV format described in
[RFC-4180](https://www.rfc-editor.org/rfc/rfc4180.html). The CSV files are compressed using Gzip
compression.

The delta updates are batched within Flow, converted to CSV files, and then pushed to the S3 bucket
at a time interval that you set. Files are limited to a configurable maximum size. Each materialized
Flow collection will produce many separate files.

[`ghcr.io/estuary/materialize-gcs-csv:dev`](https://ghcr.io/estuary/materialize-gcs-csv:dev)
provides the latest connector image. You can also follow the link in your browser to see past image
versions.

## Prerequisites

To use this connector, you'll need:

* A GCS bucket to write files to. See [this
  guide](https://cloud.google.com/storage/docs/creating-buckets) for instructions on setting up a
  new GCS bucket.
* A Google Cloud [service account](https://cloud.google.com/docs/authentication/getting-started)
  with [`roles/storage.objectCreator`](https://cloud.google.com/storage/docs/access-control/iam-roles#standard-roles)
  for the GCS bucket created above.
* A key file for the service account.

## Configuration

Use the below properties to configure the materialization, which will direct one or more of your
Flow collections to your bucket.

### Properties

#### Endpoint

| Property                  | Title                 | Description                                                                                                                                   | Type    | Required/Default |
|---------------------------|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/bucket`**             | Bucket                | Bucket to store materialized objects.                                                                                                         | string  | Required         |
| **`/credentialsJson`**    | Service Account JSON  | The JSON credentials of the service account to use for authorization.                                                                         | string  | Required         |
| **`/uploadInterval`**     | Upload Interval       | Frequency at which files will be uploaded.                                                                                                    | string  | 5m               |
| `/prefix`                 | Prefix                | Optional prefix that will be used to store objects.                                                                                           | string  |                  |
| `/fileSizeLimit`          | File Size Limit       | Approximate maximum size of materialized files in bytes. Defaults to 10737418240 (10 GiB) if blank.                                           | integer |                  |
| `/csvConfig/delimiter`    | Delimiter             | Character to separate columns within a row. Defaults to a comma if blank. Must be a single character with a byte length of 1.                 | integer |                  |
| `/csvConfig/nullString`   | Null String           | String to use to represent NULL values. Defaults to an empty string if blank.                                                                 | integer |                  |
| `/csvConfig/skipHeaders`  | Skip Headers          | Do not write headers to files.                                                                                                                | integer |                  |

#### Bindings

| Property    | Title | Description                                    | Type   | Required/Default |
|-------------|-------|------------------------------------------------|--------|------------------|
| **`/path`** | Path  | The path that objects will be materialized to. | string | Required         |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/materialize-gcs-csv:dev"
        config:
          bucket: bucket
          credentialsJson: <credentialsJson>
          uploadInterval: 5m
    bindings:
      - resource:
          path: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## File Names

Materialized files are named with monotonically increasing integer values, padded with leading 0's
so they remain lexically sortable. For example, a set of files may be materialized like this for a
given collection:

```
bucket/prefix/path/v0000000000/00000000000000000000.csv
bucket/prefix/path/v0000000000/00000000000000000001.csv
bucket/prefix/path/v0000000000/00000000000000000002.csv
```

Here the values for **bucket** and **prefix** are from your endpoint configuration. The **path** is
specific to the binding configuration. **v0000000000** represents the current **backfill counter**
for binding and will be increased if the binding is re-backfilled, along with the file names
starting back over from 0.

## Eventual Consistency

In rare circumstances, recently materialized files may be re-written by files with the same name if
the materialization shard is interrupted in the middle of processing a Flow transaction and the
transaction must be re-started. Files that were committed as part of a completed transaction will
never be re-written. In this way, eventually all collection data will be written to files
effectively-once, although inconsistencies are possible when accessing the most recently written
data.