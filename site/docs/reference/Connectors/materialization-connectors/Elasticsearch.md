# Elasticsearch

This connector materializes Flow collections into indices in Elasticsearch.

TODO: other high-level details about credentials used, mechanism, etc

[`ghcr.io/estuary/materialize-<ENDPOINT-NAME>:dev`](https://ghcr.io/estuary/materialize-<ENDPOINT-NAME>:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

TODO: additional?
* At least one Flow collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::


## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Elasticsearch materialization, which will direct one or more of your Flow collections to your desired Elasticsearch indices.

You must indicate the desired Elasticsearch field type for each field in your Flow collection.
You configure this in the `field_overrides` array for each binding.
To do so, provide a JSON pointer to the field in the collection schema and choose the output field type.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/endpoint`** |  |  | string | Required |
| `/password` |  |  | string |  |
| `/username` |  |  | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/delta_updates`** |  |  | boolean | Required |
| **`/field_overrides`** |  |  | array | Required |
| _`/field_overrides/-`_ |  |  | object |  |
| _`/field_overrides/-/es_type`_ |  | The overriding Elasticsearch data type of the field. | object |  |
| _`/field_overrides/-/es_type/date_spec`_ |  | Spec of the date field, effective if field&#x5F;type is &#x27;date&#x27;. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html). | object |  |
| _`/field_overrides/-/es_type/date_spec/format`_ |  | Format of the date. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html.). | string |  |
| _`/field_overrides/-/es_type/field_type`_ |  | The Elasticsearch field data types. Supported types include: boolean, date, double, geo&#x5F;point, geo&#x5F;shape, keyword, long, null, text. | string |  |
| _`/field_overrides/-/es_type/keyword_spec`_ |  | Spec of the keyword field, effective if field&#x5F;type is &#x27;keyword&#x27;. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html) | object |  |
| _`/field_overrides/-/es_type/keyword_spec/ignore_above`_ |  | Strings longer than the ignore&#x5F;above setting will not be indexed or stored. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html) | integer |  |
| _`/field_overrides/-/es_type/text_spec`_ |  | Spec of the text field, effective if field&#x5F;type is &#x27;text&#x27;. | object |  |
| _`/field_overrides/-/es_type/text_spec/dual_keyword`_ |  | Whether or not to specify the field as text&#x2F;keyword dual field. | boolean |  |
| _`/field_overrides/-/es_type/text_spec/keyword_ignore_above`_ |  | Effective only if DualKeyword is enabled. Strings longer than the ignore&#x5F;above setting will not be indexed or stored. See [Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html) | integer |  |
| _`/field_overrides/-/pointer`_ |  | A &#x27;&#x2F;&#x27;-delimitated json pointer to the location of the overridden field. | string |  |
| **`/index`** |  | Name of the ElasticSearch index to store the materialization results. | string | Required |
| `/number_of_replicas` |  | The number of replicas in ElasticSearch index. If not set, default to be 0. For single-node clusters, make sure this field is 0, because the Elastic search needs to allocate replicas on different nodes. | integer |  |
| `/number_of_shards` |  | The number of shards in ElasticSearch index. Must set to be greater than 0. | integer | `1` |

### Sample
TODO use https://github.com/estuary/demos-ais-vessels/blob/f474b4aa8ff764589e727e447c808fb636effd99/visualizations/vessels.flow.yaml for ref

## Delta updates

TODO Add this section if the connector supports delta updates or both standard and delta. Omit if just standard.
