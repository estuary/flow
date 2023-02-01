---
sidebar_position: 3
---

# Elasticsearch

This connector materializes Flow collections into indices in an Elasticsearch cluster.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-elasticsearch:dev`](https://ghcr.io/estuary/materialize-elasticsearch:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

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

By default, the connector attempts to map each field in the Flow collection to the most appropriate Elasticsearch [field type](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html).
However, because each JSON field type can map to multiple Elasticsearch field types,
you may want to override the defaults.
You can configure this by adding `field_overrides` to the collection's [binding](#bindings) in the materialization specification.
To do so, provide a JSON pointer to the field in the collection schema, choose the output field type, and specify additional properties, if necessary.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/endpoint`** | Endpoint | Endpoint host or URL. If using Elastic Cloud, this follows the format `https://CLUSTER_ID.REGION.CLOUD_PLATFORM.DOMAIN:PORT`. | string | Required |
| `/password` | Password | Password to connect to the endpoint. | string |  |
| `/username` | Username | User to connect to the endpoint. | string |  |

#### Bindings

| Property                                                      | Title                  | Description                                                                                                                                                                                                                              | Type    | Required/Default |
|---------------------------------------------------------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/delta_updates`**                                          | Delta updates          | Whether to use standard or [delta updates](#delta-updates)                                                                                                                                                                               | boolean | `false`          |
| `/field_overrides`                                            | Field overrides        | Assign Elastic field type to each collection field.                                                                                                                                                                                      | array   |                  |
| _`/field_overrides/-/es_type`_                                | Elasticsearch type     | The overriding Elasticsearch data type of the field.                                                                                                                                                                                     | object  |                  |
| _`/field_overrides/-/es_type/date_spec`_                      | Date specifications    | Configuration for the date field, effective if field&#x5F;type is &#x27;date&#x27;. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html).                                                 | object  |                  |
| _`/field_overrides/-/es_type/date_spec/format`_               | Date format            | Format of the date. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html).                                                                                                  | string  |                  |
| _`/field_overrides/-/es_type/field_type`_                     | Field type             | The Elasticsearch field data types. Supported types include: boolean, date, double, geo&#x5F;point, geo&#x5F;shape, keyword, long, null, text.                                                                                           | string  |                  |
| _`/field_overrides/-/es_type/keyword_spec`_                   | Keyword specifications | Configuration for the keyword field, effective if field&#x5F;type is &#x27;keyword&#x27;. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html)                                         | object  |                  |
| _`/field_overrides/-/es_type/keyword_spec/ignore_above`_      | Ignore above           | Strings longer than the ignore&#x5F;above setting will not be indexed or stored. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html)                                             | integer |                  |
| _`/field_overrides/-/es_type/text_spec`_                      | Text specifications    | Configuration for the text field, effective if field&#x5F;type is &#x27;text&#x27;.                                                                                                                                                      | object  |                  |
| _`/field_overrides/-/es_type/text_spec/dual_keyword`_         | Dual keyword           | Whether or not to specify the field as text&#x2F;keyword dual field.                                                                                                                                                                     | boolean |                  |
| _`/field_overrides/-/es_type/text_spec/keyword_ignore_above`_ | Ignore above           | Effective only if Dual Keyword is enabled. Strings longer than the ignore&#x5F;above setting will not be indexed or stored. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html). | integer |                  |
| _`/field_overrides/-/pointer`_                                | Pointer                | A &#x27;&#x2F;&#x27;-delimited json pointer to the location of the overridden field.                                                                                                                                                     | string  |                  |
| **`/index`**                                                  | index                  | Name of the ElasticSearch index to store the materialization results.                                                                                                                                                                    | string  | Required         |
| `/number_of_replicas`                                         | Number of replicas     | The number of replicas in ElasticSearch index. If not set, default to be 0. For single-node clusters, make sure this field is 0, because the Elastic search needs to allocate replicas on different nodes.                               | integer | `0`              |
| `/number_of_shards`                                           | Number of shards       | The number of shards in ElasticSearch index. Must set to be greater than 0.                                                                                                                                                              | integer | `1`              |

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
          username: flow_user
          password: secret
  	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
        bindings:
          - resource:
              index: last-updated
              delta_updates: false
              field_overrides:
                  - pointer: /updated-date
                    es_type:
                      field_type: date
                        date_spec:
                          format: yyyy-MM-dd
            source: PREFIX/source_collection
```
## Delta updates

This connector supports both standard and delta updates. You must choose an option for each binding.

[Learn more about delta updates](../../../concepts/materialization.md#delta-updates) and the implications of using each update type.
