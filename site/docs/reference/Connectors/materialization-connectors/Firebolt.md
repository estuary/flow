---
sidebar_position: 3
---

# Firebolt

This Flow connector materializes [delta updates](../../../concepts/materialization.md#delta-updates) of Flow collections into Firebolt `FACT` or `DIMENSION` tables.

To interface between Flow and Firebolt, the connector uses Firebolt's method for [loading data](https://docs.firebolt.io/loading-data/loading-data.html):
First, it stores data as JSON documents in an S3 bucket.
It then references the S3 bucket to create a [Firebolt _external table_](https://docs.firebolt.io/loading-data/working-with-external-tables.html),
which acts as a SQL interface between the JSON documents and the destination table in Firebolt.

[`ghcr.io/estuary/materialize-firebolt:dev`](https://ghcr.io/estuary/materialize-firebolt:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A Firebolt database with at least one [engine](https://docs.firebolt.io/working-with-engines/working-with-engines.html)
* An S3 bucket where JSON documents will be stored prior to loading
* At least one Flow [collection](../../../concepts/collections.md)
* You may need the AWS **access key** and **secret access key** for the user.
See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials

:::tip
 If you haven't yet captured your data from its external source,
 start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md).
 You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Firebolt materialization, which will direct Flow data to your desired Firebolt tables via an external table.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/aws_key_id` | AWS key ID | AWS access key ID for accessing the S3 bucket. | string |  |
| `/aws_region` | AWS region | AWS region the bucket is in. | string |  |
| `/aws_secret_key` | AWS secret access key | AWS secret key for accessing the S3 bucket. | string |  |
| **`/database`** | Database | Name of the Firebolt database. | string | Required |
| **`/engine_url`** | Engine URL | Engine URL of the Firebolt database, in the format: `<engine-name>.<organization>.<region>.app.firebolt.io`. | string | Required |
| **`/password`** | Password | Firebolt password. | string | Required |
| **`/s3_bucket`** | S3 bucket | Name of S3 bucket where the intermediate files for external table will be stored. | string | Required |
| `/s3_prefix` | S3 prefix | A prefix for files stored in the bucket. | string |  |
| **`/username`** | Username | Firebolt username. | string | Required |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Name of the Firebolt table to store materialized results in. The external table will be named after this table with an `_external` suffix. | string | Required |
| **`/table_type`** | Table type | Type of the Firebolt table to store materialized results in. See the [Firebolt docs](https://docs.firebolt.io/working-with-tables.html) for more details. | string | Required |

### Sample

```yaml
materializations:
  ${tenant}/${mat_name}:
	  endpoint:
        connector:
          config:
            database: my-db
            engine_url: my-db-my-engine-name.my-organization.us-east-1.app.firebolt.io
            password: secret
            # For public S3 buckets, only the bucket name is required
            s3_bucket: my-bucket
            username: firebolt-user
          # Path to the latest version of the connector, provided as a Docker image
          image: ghcr.io/estuary/materialize-firebolt:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
      - resource:
          table: table-name
          table_type: fact
      source: ${tenant}/${source_collection}
```

## Delta updates

The Firebolt connector operates only in [delta updates](../../../concepts/materialization.md#delta-updates) mode.
Firebolt stores all deltas — the unmerged collection documents — directly.

In some cases, this will affect how materialized views look in Firebolt compared to other systems that use standard updates.
