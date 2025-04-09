
# Imply Polaris

This connector materializes Flow collections as Kafka-compatible messages that an Imply Polaris Kafka consumer can read. [Imply Polaris](https://imply.io/polaris) is a fully managed, cloud-native Database-as-a-Service (DBaaS) built on Apache
Druid, designed for real-time analytics on streaming and batch data.

## Prerequisites

To use this connector, you'll need:

* At least one Flow collection
* An Imply Polaris account

## Variants

This connector is a variant of the default Dekaf connector. For other integration options, see the main [Dekaf](dekaf.md) page.

## Setup

Provide an auth token when setting up the Dekaf connector. This can be a password of your choosing and will be used to authenticate consumers to your Kafka topics.

Once the connector is created, note the full materialization name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`. You will use this as the username.

## Connecting Estuary Flow to Imply Polaris

1. Log in to your Imply Polaris account and navigate to your project.

2. In the left sidebar, click on "Tables" and then "Create Table".

3. Choose "Kafka" as the input source for your new table.

4. In the Kafka configuration section, enter the following details:

    - **Bootstrap Servers**: `dekaf.estuary-data.com:9092`
    - **Topic**: The name of an Estuary Flow collection you added to your materialization (e.g., `/my-collection`)
    - **Security Protocol**: `SASL_SSL`
    - **SASL Mechanism**: `PLAIN`
    - **SASL Username**: Your materialization's full name
    - **SASL Password**: Your materialization's auth token

5. For the "Input Format", select "avro".

6. Configure the Schema Registry settings:
    - **Schema Registry URL**: `https://dekaf.estuary-data.com`
    - **Schema Registry Username**: Same as the SASL username
    - **Schema Registry Password**: Same as the SASL password

7. In the "Schema" section, Imply Polaris should automatically detect the schema from your Avro data. Review and adjust
   the column definitions as needed.

8. Review and finalize your table configuration, then click "Create Table".

9. Your Imply Polaris table should now start ingesting data from Estuary Flow.

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
        variant: imply-polaris
    bindings:
      - resource:
          topic_name: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
