# RisingWave

This connector materializes Flow collections as Kafka-compatible messages that a RisingWave Kafka consumer can read. [RisingWave](https://www.risingwave.com/) is a cloud-native SQL streaming database that enables real-time data processing and analytics.

## Prerequisites

To use this connector, you'll need:

- At least one Flow collection
- A RisingWave instance

## Variants

This connector is a variant of the default Dekaf connector. For other integration options, see the main [Dekaf](dekaf.md) page.

## Setup

Provide an auth token when setting up the Dekaf connector. This can be a password of your choosing and will be used to authenticate consumers to your Kafka topics.

Once the connector is created, note the full materialization name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`. You will use this as the username.

## Connecting Estuary Flow to RisingWave

1. In your RisingWave instance, use the SQL shell to create a source that connects to your Estuary Flow materialization. Use the following SQL command:

   ```sql
   CREATE SOURCE IF NOT EXISTS estuary
   WITH (
      connector='kafka',
      topic='<your-collection-name>',
      properties.bootstrap.server='dekaf.estuary-data.com:9092',
      scan.startup.mode='latest',
      properties.sasl.mechanism='PLAIN',
      properties.security.protocol='SASL_SSL',
      properties.sasl.username='YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION',
      properties.sasl.password='YOUR-AUTH-TOKEN'
   ) FORMAT PLAIN ENCODE AVRO (
      schema.registry = 'https://dekaf.estuary-data.com',
      schema.registry.username='YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION',
      schema.registry.password='YOUR-AUTH-TOKEN'
   );
   ```

2. Create a materialized view to process the data:

   ```sql
   CREATE MATERIALIZED VIEW IF NOT EXISTS estuary_view AS
   SELECT * FROM estuary;
   ```

   You can customize the materialized view with your desired SQL transformations and aggregations.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Dekaf materialization, which will direct one or more of your Flow collections to your desired topics.

### Properties

#### Endpoint

| Property              | Title              | Description                                                                | Type    | Required/Default |
| --------------------- | ------------------ | -------------------------------------------------------------------------- | ------- | ---------------- |
| `/token`              | Auth Token         | The password that Kafka consumers can use to authenticate to this task.    | string  | Required         |
| `/strict_topic_names` | Strict Topic Names | Whether or not to expose topic names in a strictly Kafka-compliant format. | boolean | `false`          |
| `/deletions`          | Deletion Mode      | Can choose between `kafka` or `cdc` deletion modes.                        | string  | `kafka`          |

#### Bindings

| Property      | Title      | Description                                     | Type   | Required/Default |
| ------------- | ---------- | ----------------------------------------------- | ------ | ---------------- |
| `/topic_name` | Topic Name | Kafka topic name that Dekaf will publish under. | string | Required         |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      dekaf:
        config:
          token: <auth-token>
          strict_topic_names: false
          deletions: kafka
        variant: risingwave
    bindings:
      - resource:
          topic_name: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
