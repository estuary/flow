
# SingleStore

This connector materializes Flow collections as Kafka-compatible messages that a SingleStore Kafka consumer can read. [SingleStore](https://www.singlestore.com/) is a distributed SQL database designed for data-intensive applications,
offering high performance for both transactional and analytical workloads.

## Prerequisites

To use this connector, you'll need:

* At least one Flow collection
* A SingleStore account

## Variants

This connector is a variant of the default Dekaf connector. For other integration options, see the main [Dekaf](dekaf.md) page.

## Setup

Provide an auth token when setting up the Dekaf connector. This can be a password of your choosing and will be used to authenticate consumers to your Kafka topics.

Once the connector is created, note the task name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`. You will use this as the username.

## Connecting Estuary Flow to SingleStore

1. In the SingleStore Cloud Portal, navigate to the SQL Editor section of the Data Studio.

2. Execute the following script to create a table and an ingestion pipeline to hydrate it.

   This example will ingest data from the demo wikipedia collection (`/demo/wikipedia/recentchange-sampled`) in Estuary Flow. This becomes the `recentchange-sampled` topic once added to the SingleStore materialization.

    ```sql
    CREATE TABLE test_table (id NUMERIC, server_name VARCHAR(255), title VARCHAR(255));

    CREATE PIPELINE test AS
            LOAD DATA KAFKA "dekaf.estuary-data.com:9092/recentchange-sampled"
            CONFIG '{
                "security.protocol":"SASL_SSL",
                "sasl.mechanism":"PLAIN",
                "sasl.username":"{YOUR_TASK_NAME}",
                "broker.address.family": "v4",
                "schema.registry.username": "{YOUR_TASK_NAME}",
                "fetch.wait.max.ms": "2000"
            }'
            CREDENTIALS '{
                "sasl.password": "YOUR_AUTH_TOKEN",
                "schema.registry.password": "YOUR_AUTH_TOKEN"
            }'
            INTO table test_table
            FORMAT AVRO SCHEMA REGISTRY 'https://dekaf.estuary-data.com'
            ( id <- id, server_name <- server_name, title <- title );
    ```
3. Your pipeline should now start ingesting data from Estuary Flow into SingleStore.

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
        variant: singlestore
    bindings:
      - resource:
          topic_name: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
