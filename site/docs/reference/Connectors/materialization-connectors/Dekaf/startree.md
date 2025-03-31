
# StarTree

This connector materializes Flow collections as Kafka-compatible messages that a StarTree Kafka consumer can read. [StarTree](https://startree.ai/) is a real-time analytics platform built on Apache Pinot, designed for performing fast,
low-latency analytics on large-scale data.

## Prerequisites

To use this connector, you'll need:

* At least one Flow collection
* A StarTree account

## Variants

This connector is a variant of the default Dekaf connector. For other integration options, see the main [Dekaf](dekaf.md) page.

## Setup

Provide an auth token when setting up the Dekaf connector. This can be a password of your choosing and will be used to authenticate consumers to your Kafka topics.

Once the connector is created, note the task name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`. You will use this as the username.

## Connecting Estuary Flow to StarTree

1. In the StarTree UI, navigate to the **Data Sources** section and choose **Add New Data Source**.

2. Select **Kafka** as your data source type.

3. Enter the following connection details:

    - **Bootstrap Servers**: `dekaf.estuary-data.com`
    - **Security Protocol**: `SASL_SSL`
    - **SASL Mechanism**: `PLAIN`
    - **SASL Username**: Your materialization task name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`
    - **SASL Password**: Your materialization's auth token

4. **Configure Schema Registry**: To decode Avro messages, enable schema registry settings:

    - **Schema Registry URL**: `https://dekaf.estuary-data.com`
    - **Schema Registry Username**: Same as the SASL username
    - **Schema Registry Password**: Same as the SASL password

5. Click **Create Connection** to proceed.

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
        variant: startree
    bindings:
      - resource:
          topic_name: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
