---
description: Materialize data collections into indices in an OpenSearch cluster. Configure connector authentication, hard deletes, number of replicas, and delta updates.
---

# OpenSearch

This connector materializes Estuary collections into indices in an OpenSearch cluster.

## Prerequisites

To use this connector, you'll need:

- An OpenSearch cluster with a known endpoint
- The role used to connect to OpenSearch must have at least the following privileges:
  - **Cluster privilege** of `monitor`
  - For each index to be created: `read`, `write`, `view_index_metadata`, and `create_index`. When creating **Index privileges**, you can use a wildcard `"*"` to grant the privileges to all indices.
- At least one Estuary collection

## Configuration

To use this connector, begin with data in one or more Estuary collections.
Use the properties below to configure an OpenSearch materialization, which will direct the contents of these Estuary collections into OpenSearch indices.

**Authentication**

You can authenticate to OpenSearch using either a username and password, or using an API key.

The connector will automatically create an OpenSearch index for each binding of the materialization with index mappings for each selected field of the binding. It uses the last component of the collection name as the name of the index by default. You can customize the name of the index using the `index` property in the resource configuration for each binding.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/endpoint`** | Endpoint | Endpoint host or URL. Must start with http:// or https:// and may include the port. | string | Required |
| `/hardDelete` | Hard Delete | If enabled, items deleted in the source will also be deleted from the destination. By default, deletions are tracked via `_meta/op` (soft delete). | boolean | `false` |
| **`/credentials`** | Credentials | Either must include username/password or API key properties. | object | Required |
| `/credentials/username` | Username | Username for authentication. | string |  |
| `/credentials/password` | Password | Password for authentication. | string |  |
| `/credentials/apiKey` | API Key | API key for authentication. Must be the 'encoded' API key credentials, which is the Base64-encoding of the UTF-8 representation of the id and api_key joined by a colon (:). | string |  |
| `/advanced/number_of_replicas` | Index Replicas | The number of replicas to create new indices with. Leave blank to use the cluster default. | integer |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- |
| **`/index`** | index | Name of the OpenSearch index to store the materialization results. | string | Required |
| `/delta_updates` | Delta updates | Whether to use standard or [delta updates](#delta-updates). | boolean | `false` |
| `/number_of_shards` | Number of shards | The number of shards to create the index with. Leave blank to use the cluster default. | integer | `1` |

### Sample

```yaml
materializations:
  PREFIX/mat_name:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-opensearch:v3
        config:
          endpoint: https://cluster-id.us-east-1.aws.domain:1234
          credentials:
            username: estuary_user
            password: secret
        bindings:
          - resource:
              index: my-opensearch-index
            source: PREFIX/source_collection
```

## Setup

You must configure your OpenSearch cluster to allow connections from Estuary. It may be necessary to [allowlist the Estuary IP addresses](/reference/allow-ip-addresses).

Alternatively, you can allow secure connections via SSH tunneling. To do so:

1. Refer to the [guide](/guides/connect-network/) to configure an SSH server on the cloud platform of your choice.

2. Configure your connector as described in the [configuration](#configuration) section above, with the addition of the `networkTunnel` stanza to enable the SSH tunnel, if using. See [Connecting to endpoints on secure networks](/concepts/connectors/#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

## Delta updates

This connector supports both standard and delta updates. You must choose an option for each binding.

[Learn more about delta updates](/concepts/materialization/#delta-updates) and the implications of using each update type.

## Field Configuration

Materialized fields can be configured through their [field
selection](/concepts/materialization/#projected-fields). This can be
changed by updating the JSON in the **Advanced Specification Editor** in the
dashboard or by using `flowctl` to edit the specification directly. See [edit
a materialization](/guides/edit-data-flows/#edit-a-materialization) for
more details.

The options supported currently are:
- **routing**: A single key field may be selected for routing documents to index
  shards. The value of this field is used as the `routing` parameter in all
  operations performed by the connector.
- **mapping**: This object can be used to set the `type` and optionally a
  `format` for a field to be used for the index mapping.  This can be useful to
  map a string field as a `keyword` or to set the `format` for a `date` field.

:::note
Fields with `type: string` will have `keyword` index mappings created for
them only if they are part of the key.

OpenSearch also uses the `flat_object` type mapping instead of `flattened`.
:::

An example JSON configuration for this field configuration is shown below:

```json
{
  "bindings": [
    {
      "resource": {
        "index": "my-opensearch-index"
      },
      "source": "PREFIX/source_collection",
      "fields": {
        "require": {
          "myKey": {
            "routing": true
          },
          "stringOrIntegerField": {
            "mapping": {
              "type": "keyword"
            }
          },
          "unixTimestamp": {
            "mapping": {
              "type": "date",
              "format": "epoch_seconds"
            }
          }
        },
        "recommended": true
      }
    }
  ]
}
```
