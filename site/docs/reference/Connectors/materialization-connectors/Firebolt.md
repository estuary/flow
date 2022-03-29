# Firebolt

This Flow connector materializes [delta updates](../../../concepts/materialization.md#delta-updates) of Flow collections into Firebolt `FACT` or `DIMENSION` tables.

To interface with between the Flow and Firebolt, the connector creates an external table:
a virtual table in an S3 bucket used as an intermediate staging area.

[`ghcr.io/estuary/materialize-firebolt:dev`](https://ghcr.io/estuary/materialize-firebolt:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* An existing catalog spec that includes at least one collection
* An S3 bucket in which to stage the external table
TODO - Additional?

## Configuration

To use this connector, begin with a Flow catalog that has at least one collection.
You'll add a Firebolt materialization, which will direct one or more of your Flow collections to your desired Firebolt tables via an external table.
Follow the basic [materialization setup](../../../concepts/materialization.md#specification) and add the required Firebolt configuration values per the table below.

### Values

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/aws_key_id` | AWS Key ID | AWS Key ID for accessing the S3 bucket. | string |  |
| `/aws_region` | AWS Region | AWS Region the bucket is in. | string |  |
| `/aws_secret_key` | AWS Secret Key | AWS Secret Key for accessing the S3 bucket. | string |  |
| **`/database`** | Database | Name of the Firebolt database. | string | Required |
| **`/engine_url`** | Engine URL | Engine URL of the Firebolt database, in the format: &#x60;&lt;engine-name&gt;.&lt;organization&gt;.&lt;region&gt;.app.firebolt.io&#x60;. | string | Required |
| **`/password`** | Password | Firebolt password. | string | Required |
| **`/s3_bucket`** | S3 Bucket | Name of S3 bucket where the intermediate files for external table will be stored. | string | Required |
| `/s3_prefix` | S3 Prefix | A prefix for files stored in the bucket. | string |  |
| **`/username`** | Username | Firebolt username. | string | Required |

TODO- figure out why only some S3 info required

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Name of the Firebolt table to store materialized results in. The external table will be named after this table with an &#x60;&#x5F;external&#x60; suffix. | string | Required |
| **`/table_type`** | Table Type | Type of the Firebolt table to store materialized results in. See https:&#x2F;&#x2F;docs.firebolt.io&#x2F;working-with-tables.html for more details. | string | Required |

### Sample

```yaml
# If this is the first materialization, add the section to your catalog spec
materializations:
  ${tenant}/${mat_name}:
	  endpoint:
  	  connector:
    	    config:
               aws_key_id:
               aws_region:
               aws_secret_key:
               database:
               engine_url:
               password:
               s3_bucket:
               s3_prefix:
               username:
            # Path to the latest version of the connector, provided as a Docker image
    	    image: ghcr.io/estuary/materialize-firebolt:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	table:
      	table_type:
    source: ${tenant}/${source_collection}
```

## Delta updates

The Firebolt connector operates only in [delta updates](../../../concepts/materialization.md#delta-updates) mode.
This means that Firebolt, rather than Flow, performs the document merge. (FACT CHECK?)
In some cases, this will affect how materialized views look in Firebolt compared to other systems that use standard updates.(?????)

