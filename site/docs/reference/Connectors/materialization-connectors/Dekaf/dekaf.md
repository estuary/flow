
import ReactPlayer from "react-player";

# Dekaf

This connector materializes Flow collections as Kafka-compatible messages that Kafka consumers can read.

If you want to send messages to your own Kafka broker, see the [Kafka](../apache-kafka.md) materialization connector instead.

<ReactPlayer controls url="https://www.youtube.com/watch?v=Oil8yNHRrqQ" />

## Prerequisites

To use this connector, you'll need:

* At least one Flow collection
* At least one Kafka consumer

## Variants

Dekaf can be used with a number of systems that act as Kafka consumers. Specific instructions are provided for the following systems:

* [Bytewax](bytewax.md)
* [ClickHouse](clickhouse.md)
* [Imply Polaris](imply-polaris.md)
* [Materialize](materialize.md)
* [SingleStore](singlestore.md)
* [Startree](startree.md)
* [Tinybird](tinybird.md)

For other use cases, continue with the setup details below for general instruction.

## Setup

Provide an auth token when setting up the Dekaf connector. This can be a password of your choosing and will be used to authenticate consumers to your Kafka topics.

Once the connector is created, note the full materialization name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`. You will use this as the username.

You may then connect to a Kafka consumer of your choice using the following details:

* **Broker Address**: `dekaf.estuary-data.com:9092`
* **Schema Registry Address**: `https://dekaf.estuary-data.com`
* **Security Protocol**: `SASL_SSL`
* **SASL Mechanism**: `PLAIN`
* **SASL Username**: The full name of your materialization
* **SASL Password**: The auth token you specified
* **Schema Registry Username**: The full name of your materialization
* **Schema Registry Password**: The auth token you specified

To subscribe to a particular topic, use a binding's topic name. By default, this will be the collection name.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Dekaf materialization, which will direct one or more of your Flow collections to your desired topics.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/token` | Auth Token | The password that Kafka consumers can use to authenticate to this task. | string | Required |
| `/strict_topic_names` | Strict Topic Names | Whether or not to expose topic names in a strictly Kafka-compliant format. | boolean | `false` |
| `/deletions` | Deletion Mode | Can choose between `kafka` or `cdc` deletion modes. | string | `kafka` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/topic_name` | Topic Name | Kafka topic name that Dekaf will publish under. | string | Required |

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
        variant: generic
    bindings:
      - resource:
          topic_name: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
