This Flow connector materializes [delta updates](../../../concepts/materialization.md#delta-updates) of your Flow collections into Rockset collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-rockset:dev`](https://github.com/estuary/connectors/pkgs/container/materialize-rockset) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A Rockset [API key generated](https://rockset.com/docs/rest-api/#createapikey)
    * The API key must have the **Member** or **Admin** [role](https://rockset.com/docs/iam/#users-api-keys-and-roles).
* A Rockset workspace
    * Optional; if none exist, one will be created by the connector.
* A Rockset collection
    * Optional; if none exist, one will be created by the connector.
* At least one Flow collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Rockset materialization, which will direct one or more of your Flow collections to your desired Rockset collections.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/api_key`** | Rockset API Key | The key used to authenticate to the Rockset API. Must have role of admin or member. | string | Required |
| **`/region_base_url`** | Region Base URL | The base URL to connect to your Rockset deployment. Example: api.usw2a1.rockset.com (do not include the protocol). [See supported options and how to find yours](https://rockset.com/docs/rest-api/).  | string | Required |


#### Bindings

The binding configuration for this connector includes two optional sections.
**Backfill from S3** allows you to backfill historical data from an S3 bucket, [as detailed below](#bulk-ingestion-for-large-backfills-of-historical-data).
**Advanced collection settings** includes settings that may help optimize your resulting Rockset collections:

* **Clustering fields**: You can specify clustering fields
for your Rockset collection's columnar index to help optimize specific query patterns.
See the [Rockset docs](https://rockset.com/docs/query-composition/#data-clustering) for more information.
* **Retention period**: Amount of time before data is purged, in seconds.
A low value will keep the amount of data indexed in Rockset smaller.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/advancedCollectionSettings` | Advanced Collection Settings |  | object |  |
| `/advancedCollectionSettings/clustering_key` | Clustering Key | List of clustering fields | array |  |
| _`/advancedCollectionSettings/clustering_key/-/field_name`_ | Field Name | The name of a field | string |  |
| `/advancedCollectionSettings/retention_secs` | Retention Period | Number of seconds after which data is purged based on event time | integer |  |
| **`/collection`** | Rockset Collection | The name of the Rockset collection (will be created if it does not exist) | string | Required |
| `/initializeFromS3` | Backfill from S3 |  | object |  |
| `/initializeFromS3/bucket` | Bucket | The name of the S3 bucket to load data from. | string |  |
| `/initializeFromS3/integration` | Integration Name | The name of the integration that was previously created in the Rockset UI | string |  |
| `/initializeFromS3/pattern` | Pattern | A regex that is used to match objects to be ingested | string |  |
| `/initializeFromS3/prefix` | Prefix | Prefix of the data within the S3 bucket. All files under this prefix will be loaded. Optional. Must not be set if &#x27;pattern&#x27; is defined. | string |  |
| `/initializeFromS3/region` | Region | The AWS region in which the bucket resides. Optional. | string |  |
| **`/workspace`** | Workspace | The name of the Rockset workspace (will be created if it does not exist) | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
	  endpoint:
  	  connector:
    	    config:
               region_base_url: api.usw2a1.rockset.com
               api_key: supersecret
            # Path to the latest version of the connector, provided as a Docker image
    	    image: ghcr.io/estuary/materialize-rockset:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	workspace: ${namespace_name}
      	collection: ${table_name}
    source: ${PREFIX}/${source_collection}
```

## Delta updates and reduction strategies

The Rockset connector operates only in [delta updates](../../../concepts/materialization.md#delta-updates) mode.
This means that Rockset, rather than Flow, performs the document merge.
In some cases, this will affect how materialized views look in Rockset compared to other systems that use standard updates.

Rockset merges documents by the key defined in the Flow collection schema, and always uses the semantics of [RFC 7396 - JSON merge](https://datatracker.ietf.org/doc/html/rfc7396).
This differs from how Flow would reduce documents, most notably in that Rockset will _not_ honor any reduction strategies defined in your Flow schema.
For consistent output of a given collection across Rockset and other materialization endpoints, it's important that that collection's reduction annotations
in Flow mirror Rockset's semantics.

To accomplish this, ensure that your collection schema has the following [data reductions](../../../concepts/schemas.md#reductions) defined in its schema:

* A top-level reduction strategy of [merge](../../reduction-strategies/merge.md)
* A strategy of [lastWriteWins](../../reduction-strategies/firstwritewins-and-lastwritewins.md) for all nested values (this is the default)


## Bulk ingestion for large backfills of historical data

:::info Beta
This feature is currently being updated as part of Flow's beta release,
and may not work as expected at this time. If you require this feature,
[contact the Estuary team](mailto:support@estuary.dev).
:::

You can backfill large amounts of historical data into Rockset using a *bulk ingestion*. Bulk ingestion must originate in S3 and requires additional steps in your dataflow.
This workflow is supported using the [flowctl](../../../concepts/flowctl.md) CLI.

### Prerequisites

This is an intermediate workflow: after you use the web app's GUI to create the initial materialization, you must manually
update the [materialization specification](../../../concepts/materialization.md#specification).

To do this, you'll need :
* A local [installation of `flowctl`](../../../concepts/flowctl.md#installation-and-setup).
* Familiarity with [working with drafts in flowctl](../../../concepts/flowctl.md#working-with-drafts). If you're not familiar, read through [this tutorial](../../../guides/create-derivation.md) as a guide.

### How to perform a bulk ingestion

A bulk ingestion from a Flow collection into Rockset is essentially a two-step process. First, Flow writes your historical data into an S3 bucket using Estuary's [S3-Parquet materialization](./Parquet.md) connector. Once the data is caught up, it uses the Rockset connector to backfill the data from S3 into Rockset and then switches to the Rockset Write API for the continuous materialization of new data.

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

1. You'll need an [S3 integration](https://rockset.com/docs/amazon-s3/) in Rockset. To create one, follow the [instructions here](https://rockset.com/docs/amazon-s3/#create-an-s3-integration), but _do not create the Rockset collection yet_.

2. In the Flow web app, use the [S3-Parquet materialization](./Parquet.md) to create and publish a materialization of `example/flow/collection` into a unique prefix within an S3 bucket of your choosing.

3. On the **Materializations** tab, note the full name of the materialization; for example `myOrg/example/toRockset`.

4. Switch to flowctl. Create a new draft and add the materialization into your draft.

   ```console
   flowctl draft create
   ```
   ```console
   flowctl catalog draft --name myOrg/example/toRockset
   ```

5. Pull the draft locally.
   ```
   flowctl draft develop
   ```

   In a directory that shares the name of your materialization, you'll find a file called `flow.yaml`.
   Open it in your preferred editor.

   It will look similar to:

  ```yaml
  materializations:
    example/toRockset:
      endpoint:
        connector:
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
6. By now, this  S3 materialization has caught up with your historical data, so you'll switch to the Rockset write API for your future data.

   To make the switch, first disable the S3 materialization by setting shards to disabled in the definition. This is necessary to ensure correct ordering of documents written to Rockset.

  ```yaml
  materializations:
    example/toRockset:
      shards:
        disable: true
      # ...the remainder of the materialization yaml remains the same as above
  ```

7. Re-deploy the materialization.

   ```console
   flowctl draft author --source flow.yaml
   ```
   ```console
   flowctl draft publish
   ```

4. Back in your local editor, update the materialization to use the `materialize-rockset` connector, and re-enable the shards. Here you'll provide the name of the Rockset S3 integration you created above, as well as the bucket and prefix that you previously materialized into. **It's critical that the name of the materialization remains the same as it was for materializing into S3.**
  ```yaml
  materializations:
    example/toRockset:
      endpoint:
        connector:
          image: ghcr.io/estuary/materialize-rockset:dev
          config:
             region_base_url: api.usw2a1.rockset.com
             api_key: <your rockset API key here>
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
5. Re-deploy the materialization as described in step 7.

   When you activate the new materialization, the connector will create the Rockset collection using the given integration, and wait for it to ingest all of the historical data from S3 before it continues. Once this completes, the Rockset connector will automatically switch over to the incoming stream of new data.

## Changelog

The changelog includes a list of breaking changes made to this connector. Backwards-compatible changes are not listed.

**Proceed with caution when editing materializations created with previous versions of this connector;
editing always upgrades your materialization to the latest connector version.**

#### V2: 2022-12-06

* Region Base URL was added and is now required as part of the endpoint configuration.
* Event Time fields and the Insert Only option were removed from the advanced collection settings.
