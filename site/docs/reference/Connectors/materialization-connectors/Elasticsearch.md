# Elasticsearch

This connector materializes Flow collections into indices in an Elasticsearch cluster.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-elasticsearch:dev`](https://ghcr.io/estuary/materialize-elasticsearch:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

- An Elastic cluster with a known [endpoint](https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html#send-requests-to-elasticsearch)
- The role used to connect to Elasticsearch must have at least the following privileges (see Elastic's documentation on [defining roles](https://www.elastic.co/guide/en/elasticsearch/reference/current/defining-roles.html#roles-indices-priv) and [security privileges](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-privileges.html#privileges-list-indices)):
  - **Cluster privilege** of `monitor`
  - For each index to be created: `read`, `write`, `view_index_metadata`, and `create_index`. When creating **Index privileges**, you can use a wildcard `"*"` to grant the privileges to all indices.
- At least one Flow collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure an Elasticsearch materialization, which will direct the contents of these Flow collections into Elasticsearch indices.

**Authentication**

You can authenticate to Elasticsearch using either a username and password, or using an API key.

The connector will automatically create an Elasticsearch index for each binding of the materialization with index mappings for each selected field of the binding. It uses the last component of the collection name as the name of the index by default. You can customize the name of the index using the `index` property in the resource configuration for each binding.

### Properties

#### Endpoint

| Property                      | Title          | Description                                                                                                                                                                                             | Type    | Required/Default |
| ----------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/endpoint`**               | Endpoint       | Endpoint host or URL. Must start with http:// or https://. If using Elastic Cloud this follows the format https://CLUSTER_ID.REGION.CLOUD_PLATFORM.DOMAIN:PORT                                          | string  | Required         |
| **`/credentials`**            |                |                                                                                                                                                                                                         | object  | Required         |
| `/credentials/username`       | Username       | Username to use for authenticating with Elasticsearch.                                                                                                                                                  | string  |                  |
| `/credentials/password`       | Password       | Password to use for authenticating with Elasticsearch.                                                                                                                                                  | string  |                  |
| `/credentials/apiKey`         | API Key        | API key for authenticating with the Elasticsearch API. Must be the 'encoded' API key credentials, which is the Base64-encoding of the UTF-8 representation of the id and api_key joined by a colon (:). | string  |                  |
| `advanced/number_of_replicas` | Index Replicas | The number of replicas to create new indices with. Leave blank to use the cluster default.                                                                                                              | integer |                  |

#### Bindings

| Property             | Title            | Description                                                                            | Type    | Required/Default |
| -------------------- | ---------------- | -------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/index`**         | index            | Name of the Elasticsearch index to store the materialization results.                  | string  | Required         |
| **`/delta_updates`** | Delta updates    | Whether to use standard or [delta updates](#delta-updates).                            | boolean | `false`          |
| `/number_of_shards`  | Number of shards | The number of shards to create the index with. Leave blank to use the cluster default. | integer | `1`              |

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

## Setup

You must configure your Elasticsearch cluster to allow connections from Estuary. It may be necessary to whitelist Estuary Flow's IP addresses `34.121.207.128, 35.226.75.135, 34.68.62.148`.

Alternatively, you can allow secure connections via SSH tunneling. To do so:

1. Refer to the [guide](../../../../guides/connect-network/) to configure an SSH server on the cloud platform of your choice.

2. Configure your connector as described in the [configuration](#configuration) section above, with the addition of the `networkTunnel` stanza to enable the SSH tunnel, if using. See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

## Delta updates

This connector supports both standard and delta updates. You must choose an option for each binding.

[Learn more about delta updates](../../../concepts/materialization.md#delta-updates) and the implications of using each update type.

## Keyword Fields

Collection fields with `type: string` will have `keyword` index mappings created for them if they
are part of the collection key, and `text` mappings for them if they are not part of the collection
key.

To materialize a collection field with `type: string` as a `keyword` mapping instead of a `text`
mapping, configure the [field selection](../../../concepts/materialization.md#projected-fields) for
the binding to indicate which fields should having keyword mappings created for them using the key
and value of `"keyword": true`. This can be changed by updating the JSON in the **Advanced
Specification Editor** in the web app or by using `flowctl` to edit the specification directly, see
[edit a materialization](../../../guides/edit-data-flows.md#edit-a-materialization) for more details.

An example JSON configuration for a binding that materializes `stringField` as a `keyword` mapping
is shown below:

```json
{
  "bindings": [
    {
      "resource": {
        "index": "my-elasticsearch-index"
      },
      "source": "PREFIX/source_collection",
      "fields": {
        "include": {
          "stringField": {
            "keyword": true
          }
        },
        "recommended": true
      }
    }
  ]
}
```

## Changelog

The changelog includes a list of breaking changes made to this connector. Backwards-compatible changes are not listed.

**Proceed with caution when editing materializations created with previous versions of this connector; editing always upgrades your materialization to the latest connector version.**

#### V3: 2023-08-21

- Index mappings will now be created based on the selected fields of the materialization. Previously only dynamic runtime mappings were created, and the entire root document was always materialized.

- Moved "number of replicas" configuration for new indices to an advanced, optional, endpoint-level configuration.

- The "number of shards" resource configuration is now optional.
