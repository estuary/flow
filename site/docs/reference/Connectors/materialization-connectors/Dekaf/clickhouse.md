
# ClickHouse

This connector materializes Flow collections as Kafka-compatible messages that a ClickHouse Kafka consumer can read. [ClickHouse](https://clickhouse.com/) is a real-time analytical database and warehouse.

## Prerequisites

To use this connector, you'll need:

* At least one Flow collection
* **[ClickHouse Cloud](https://clickhouse.com/) account** with permissions to configure ClickPipes for data ingestion

## Variants

This connector is a variant of the default Dekaf connector. For other integration options, see the main [Dekaf](dekaf.md) page.

## Setup

Provide an auth token when setting up the Dekaf connector. This can be a password of your choosing and will be used to authenticate consumers to your Kafka topics.

Once the connector is created, note the full materialization name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`. You will use this as the username.

## Connecting Estuary Flow to ClickPipes in ClickHouse Cloud

1. **Set Up ClickPipes**:
    - In ClickHouse Cloud, go to **Integrations** and select **Apache Kafka** as the data source.

2. **Enter Connection Details**:
    - Use the following connection parameters to configure access to Estuary Flow.
        * **Broker Address**: `dekaf.estuary-data.com:9092`
        * **Schema Registry Address**: `https://dekaf.estuary-data.com`
        * **Security Protocol**: `SASL_SSL`
        * **SASL Mechanism**: `PLAIN`
        * **SASL Username**: The full name of your materialization
        * **SASL Password**: The auth token you specified in your materialization
        * **Schema Registry Username**: Same as the SASL username
        * **Schema Registry Password**: Same as the SASL password

3. **Map Data Fields**:
    - Ensure that ClickHouse can parse the incoming data properly. Use ClickHouse’s mapping interface to align fields
      between Estuary Flow collections and ClickHouse tables.

4. **Provision the ClickPipe**:
    - Kick off the integration and allow ClickPipes to set up the pipeline. This should complete within a few seconds.

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
        variant: clickhouse
    bindings:
      - resource:
          topic_name: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
