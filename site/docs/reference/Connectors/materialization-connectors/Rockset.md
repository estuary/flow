This Flow connector materializes [delta updates](../../../concepts/catalog-entities/materialization.md#how-materializations-work-) of your Flow collections into Rockset collections.

`ghcr.io/estuary/materialize-rockset:dev` provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites
To use this connector, you'll need :
* An existing catalog spec that includes at least one collection with its schema specified
* A Rockset account with an [API key generated](https://rockset.com/docs/rest-api/#createapikey) from the web UI
* A Rockset workspace
    * Optional; if none exist, one will be created by the connector.
* A Rockset collection
    * Optional; if none exist, one will be created by the connector.

## Configuration
To use this connector, begin with a Flow catalog that has at least one **collection**. You'll add a Rockset materialization, which will direct one or more of your Flow collections to your desired Rockset collections. Follow the basic [materialization setup](../../../concepts/catalog-entities/materialization.md) and add the required Rockset configuration values per the table below.

### Values
| Value | Name | Type | Required/Default | Details |
|-------|------|------|---------| --------|
| `api_key` | API Key | String | Required | Rockset API key generated from the web UI. |
| `HttpLogging` | HTTP Logging | bool | false | Enable verbose logging of the HTTP calls to the Rockset API |
| `MaxConcurrentRequests` | Maximum Concurrent Requests | int | 1 | The upper limit on how many concurrent requests will be sent to Rockset. |
| `workspace` | Workspace | String | Required | For each binding, name of the Rockset workspace |
| `collection` | Rockset collection | String | Required| For each binding, the name of the destination Rockset table |

### Sample

```yaml
# If this is the first materialization, add the section to your catalog spec
materializations:
  ${tenant}/${mat_name}:
	  endpoint:
  	  flowSink:
    	    config:
               api_key: supersecret
            # Path to the latest version of the connector, provided as a Docker image
    	    image: ghcr.io/estuary/materialize-rockset:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	workspace: ${namespace_name}
      	collection: ${table_name}
    source: ${tenant}/${source_collection}
```
## Bulk ingestion for large backfills of historical data

You can backfill large amounts of historical data into Rockset using a *bulk ingestion*. Bulk ingestion must originate in S3 and require additional steps in your dataflow. Flow's Rockset connector supports this in multiple ways.

If you already have an [S3 integration](https://rockset.com/docs/amazon-s3/) in Rockset, you can simply direct the connector to it. The connector will create the collection from the given integration, process all objects in the S3 bucket first, and then begin writing additional any additional, newer data that is coming through your pipeline. This ensures that documents are always written to Rockset in the proper order.

To configure this, within the `resource` of each binding, add
`initializeFromS3` and fill out the S3 integration details as shown in the following example:

  ```yaml
  materializations:
    example/toRockset:
      endpoint:
        flowSink:
          image: ghcr.io/estuary/materialize-rockset:dev
          config:
            api_key: <your rockset API key here>
            max_concurrent_requests: 5
      bindings:
        - resource:
            workspace: <your rockset workspace name>
            collection: <your rockset collection name>
            initializeFromS3:
              integration: <rockset integration name>
              bucket: example-s3-bucket
              region: us-east-1
              prefix: example/s3-prefix/
          source: example/flow/collection
  ```

If you don't have an S3 integration set up with your historical data, you can use a two-step process to bulk ingest historical data from a Flow collection. First, you'll port your historical data into an S3 bucket and backfill it into Rockset using Estuary's [materialize-s3-parquet](../materialize-s3-parquet/) connector. Then, you'll implement the Rockset connector, which will pick up the dataflow where the S3-parquet connector left off.

import Mermaid from '@theme/Mermaid';

<Mermaid chart={`
	graph TD
    A[Create an S3 integration in Rockset] --> B
    B[Create Flow materialization into S3 bucket] --> C
    C[Wait for S3 materialization to catch up with historical data] -->|When ready to bulk ingest into Rockset| D
    D[Disable S3 materialization shards] --> E
    E[Update same materialization to use the Rockset connector with the integration created in first step] --> F
    F[Rockset connector automatically continues materializing after the bulk ingestion completes]
`}/>

To set this up, use the following procedure as a guide, substituting `example/flow/collection` for your collection:

1. Follow the [instructions here](https://rockset.com/docs/amazon-s3/#create-an-s3-integration) to create the integration, but _do not create the Rockset collection yet_.
2. Create and activate a materialization of `example/flow/collection` into a unique prefix within an S3 bucket of your choosing.
  ```yaml
  materializations:
    example/toRockset:
      endpoint:
        flowSink:
          image: ghcr.io/estuary/materialize-s3-parquet:dev
          config:
            bucket: example-s3-bucket
            region: us-east-1
            awsAccessKeyId: <your key>
            awsSecretAccessKey: <your secret>
            uploadIntervalInSeconds: 300
      bindings:
        - resource:
            pathPrefix: example/s3-prefix/
          source: example/flow/collection
  ```
3. Once the  S3 materialization is caught up with your historical data, you'll switch to the Rockset write API for your future data. To make the switch, first disable the S3 materialization by setting shards to disabled in the definition, and re-deploy the catalog. This is necessary to ensure correct ordering of documents written to Rockset.
  ```yaml
  materializations:
    example/toRockset:
      shards:
        disable: true
      # ...the remainder of the materialization yaml remains the same as above
  ```
4. Update the materialization to use the `materialize-rockset` connector, and re-enable the shards. Here you'll provide the name of the Rockset S3 integration you created above, as well as the bucket and prefix that you previously materialized into. **It's critical that the name of the materialization remains the same as it was for materializing into S3.**
  ```yaml
  materializations:
    example/toRockset:
      endpoint:
        flowSink:
          image: ghcr.io/estuary/materialize-rockset:dev
          config:
            api_key: <your rockset API key here>
            max_concurrent_requests: 5
      bindings:
        - resource:
            workspace: <your rockset workspace name>
            collection: <your rockset collection name>
            initializeFromS3:
              integration: <rockset integration name>
              bucket: example-s3-bucket
              region: us-east-1
              prefix: example/s3-prefix/
          source: example/flow/collection
  ```
5. When you activate the new materialization, the connector will create the Rockset collection using the given integration, and wait for it to ingest all of the data from S3 before it continues. During this time, the Flow shards will remain in `STANDBY` status, so `flowctl deploy` is expected to block until the bulk ingestion completes. Once this completes, the materialize-rockset connector will automatically switch over to using the Rockset write API.

## Potential improvements

There are a number of additional parameters that users may want to control when creating Rockset collections. The following parameters from the [Rockset API docs](https://rockset.com/docs/rest-api/#createcollection) are potential candidates for inclusion in the connector/resource configs.

```
retention_secs
time_partition_resolution_secs
event_time_info
field_mappings
field_mapping_query
clustering_key
field_schemas
inverted_index_group_encoding_options
```