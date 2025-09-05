
# Rockset

This Flow connector materializes [delta updates](/concepts/materialization/#delta-updates) of your Flow collections into Rockset collections.

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

The binding configuration includes the optional **Advanced collection settings** section.
These settings can help optimize your output Rockset collections:

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

The Rockset connector operates only in [delta updates](/concepts/materialization/#delta-updates) mode.
This means that Rockset, rather than Flow, performs the document merge.
In some cases, this will affect how materialized views look in Rockset compared to other systems that use standard updates.

Rockset merges documents by the key defined in the Flow collection schema, and always uses the semantics of [RFC 7396 - JSON merge](https://datatracker.ietf.org/doc/html/rfc7396).
This differs from how Flow would reduce documents, most notably in that Rockset will _not_ honor any reduction strategies defined in your Flow schema.
For consistent output of a given collection across Rockset and other materialization endpoints, it's important that that collection's reduction annotations
in Flow mirror Rockset's semantics.

To accomplish this, ensure that your collection schema has the following [data reductions](../../../concepts/schemas.md#reductions) defined in its schema:

* A top-level reduction strategy of [merge](../../reduction-strategies/merge.md)
* A strategy of [lastWriteWins](../../reduction-strategies/firstwritewins-and-lastwritewins.md) for all nested values (this is the default)

## Changelog

The changelog includes a list of breaking changes made to this connector. Backwards-compatible changes are not listed.

**Proceed with caution when editing materializations created with previous versions of this connector;
editing always upgrades your materialization to the latest connector version.**

#### V2: 2022-12-06

* Region Base URL was added and is now required as part of the endpoint configuration.
* Event Time fields and the Insert Only option were removed from the advanced collection settings.
