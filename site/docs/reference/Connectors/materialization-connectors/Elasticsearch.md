---
sidebar_position: 3
---

# Elasticsearch

This connector materializes Flow collections into indices in an Elasticsearch cluster.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-elasticsearch:v2`](https://ghcr.io/estuary/materialize-elasticsearch:v2) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* An Elastic cluster with a known [endpoint](https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html#send-requests-to-elasticsearch)
  * If the cluster is on the Elastic Cloud, you'll need an Elastic user with a role that grants all privileges on indices you plan to materialize to within the cluster.
    See Elastic's documentation on [defining roles](https://www.elastic.co/guide/en/elasticsearch/reference/current/defining-roles.html#roles-indices-priv) and
    [security privileges](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-privileges.html#privileges-list-indices).
* At least one Flow collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::


## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure an Elasticsearch materialization, which will direct the contents of these Flow collections into Elasticsearch indices.

**Authentication**

You can authenticate to Elasticsearch using either a username and password, or using an API key.

The connector will automatically create an Elasticsearch index for each binding of the materialization. It uses the last component of the collection name as the name of the index by default. You can customize the name of the index using the `index` property in the resource configuration for each binding. You can also create the indices yourself and just enter the name as the `index`. This allows you full control over the index creation properties.

**Elasticsearch Mappings**

If you let the connector create the indices automatically, it will configure it to use [dynamic runtime mappings](https://www.elastic.co/guide/en/elasticsearch/reference/current/runtime.html). This allows you to search based on any fields in your source documents, and avoids any issues that could be caused by the creation of too many mappings. It's recommended that you [add explicit mappings](https://www.elastic.co/guide/en/elasticsearch/reference/current/explicit-mapping.html#add-field-mapping) as you identify the need, in order to keep your Elasticsearch queries fast.


### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **** | Elasticsearch Connection |  | object | Required |
| **`/endpoint`** | Endpoint | Endpoint host or URL. If using Elastic Cloud this follows the format https:&#x2F;&#x2F;CLUSTER&#x5F;ID.REGION.CLOUD&#x5F;PLATFORM.DOMAIN:PORT | string | Required |
| `/credentials` |  |  | object |  |
| `/credentials/username` | Username | Username to use for authenticating with Elasticsearch | string |  |
| `/credentials/password` | Password | Password to use for authenticating with Elasticsearch | string |  |
| `/credentials/apiKey` | API Key | API Key to use for authenticating with Elasticsearch | string |  |


#### Bindings

| Property                                                      | Title                  | Description                                                                                                                                                                                                                              | Type    | Required/Default |
|---------------------------------------------------------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/delta_updates`**                                          | Delta updates          | Whether to use standard or [delta updates](#delta-updates)                                                                                                                                                                               | boolean | `false`          |
| **`/index`**                                                  | index                  | Name of the ElasticSearch index to store the materialization results.                                                                                                                                                                    | string  | Required         |
| `/number_of_replicas`                                         | Number of replicas     | The number of replicas in ElasticSearch index. If not set, default to be 0. For single-node clusters, this must be 0. For production systems, a value of 1 or more is recommended                            | integer | `0`              |
| `/number_of_shards`                                           | Number of shards       | The number of shards in ElasticSearch index. Must be greater than 0.                                                                                                                                                              | integer | `1`              |

### Sample

```yaml
materializations:
  PREFIX/mat_name:
    endpoint:
      connector:
         # Path to the latest version of the connector, provided as a Docker image
        image: ghcr.io/estuary/materialize-elasticsearch:dev
        config:
          endpoint: https://ec47fc4d2c53414e1307e85726d4b9bb.us-east-1.aws.found.io:9243
          credentials:
            username: flow_user
            password: secret
  	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
        bindings:
          - resource:
              index: my-elasticsearch-index
            source: PREFIX/source_collection
```

## Delta updates

This connector supports both standard and delta updates. You must choose an option for each binding.

[Learn more about delta updates](../../../concepts/materialization.md#delta-updates) and the implications of using each update type.
