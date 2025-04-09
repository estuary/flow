
# Materialize

This connector materializes Flow collections as Kafka-compatible messages that a Materialize Kafka consumer can read. [Materialize](https://materialize.com/) is an operational data warehouse for real-time analytics that uses standard SQL
for defining transformations and queries.

## Prerequisites

To use this connector, you'll need:

* At least one Flow collection
* A Materialize account

## Variants

This connector is a variant of the default Dekaf connector. For other integration options, see the main [Dekaf](dekaf.md) page.

## Setup

Provide an auth token when setting up the Dekaf connector. This can be a password of your choosing and will be used to authenticate consumers to your Kafka topics.

Once the connector is created, note the full materialization name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`. You will use this as the username.

## Connecting Estuary Flow to Materialize

1. In your Materialize dashboard, use the SQL shell to create a new secret and connection using the Kafka source
   connector. Use the following SQL commands to configure the connection to Estuary Flow:

   ```sql
   CREATE
   SECRET estuary_token AS
     'your_materialization_auth_token_here';

   CREATE
   CONNECTION estuary_connection TO KAFKA (
       BROKER 'dekaf.estuary-data.com',
       SECURITY PROTOCOL = 'SASL_SSL',
       SASL MECHANISMS = 'PLAIN',
       SASL USERNAME = 'YOUR/MATERIALIZATION/NAME',
       SASL PASSWORD = SECRET estuary_token
   );

   CREATE
   CONNECTION csr_estuary_connection TO CONFLUENT SCHEMA REGISTRY (
       URL 'https://dekaf.estuary-data.com',
       USERNAME = 'YOUR/MATERIALIZATION/NAME',
       PASSWORD = SECRET estuary_token
   );
   ```

2. **Create a source in Materialize** to read from the Kafka topic. Use the following SQL command,
   replacing `<name-of-your-flow-collection>` with the name of a collection you added to your Estuary Flow materialization:

   ```sql
   CREATE SOURCE materialize_source
   FROM KAFKA CONNECTION estuary_connection (TOPIC '<name-of-your-flow-collection>')
   FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_estuary_connection
   ENVELOPE UPSERT;
   ```

### Creating Real-Time Views

To begin analyzing the data, create a real-time view using SQL in Materialize. Here is an example query to create a
materialized view that tracks data changes:

```sql
CREATE MATERIALIZED VIEW my_view AS
SELECT *
FROM materialize_source;
```

For more detailed information on creating materialized views and other advanced configurations, refer to
the [Materialize documentation](https://materialize.com/docs/).

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
        variant: materialize
    bindings:
      - resource:
          topic_name: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
