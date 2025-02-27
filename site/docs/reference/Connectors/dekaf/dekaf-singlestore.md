# SingleStore (Cloud)

:::warning
This guide uses a legacy method of connecting with Dekaf and is presented for historical purposes. For new integrations or to migrate your existing Dekaf setup to the new workflow, see the [Dekaf materialization connector](../materialization-connectors/Dekaf/dekaf.md).
:::

This guide demonstrates how to use Estuary Flow to stream data to SingleStore using the Kafka-compatible Dekaf API.

[SingleStore](https://www.singlestore.com/) is a distributed SQL database designed for data-intensive applications,
offering high performance for both transactional and analytical workloads.

## Connecting Estuary Flow to SingleStore

1. [Generate a refresh token](/guides/how_to_generate_refresh_token) for the SingleStore connection from the Estuary
   Admin Dashboard.

2. In the SingleStore Cloud Portal, navigate to the SQL Editor section of the Data Studio.

3. Execute the following script to create a table and an ingestion pipeline to hydrate it.

   This example will ingest data from the demo wikipedia collection in Estuary Flow.

    ```sql
    CREATE TABLE test_table (id NUMERIC, server_name VARCHAR(255), title VARCHAR(255));

    CREATE PIPELINE test AS
            LOAD DATA KAFKA "dekaf.estuary-data.com:9092/demo/wikipedia/recentchange-sampled"
            CONFIG '{
                "security.protocol":"SASL_SSL",
                "sasl.mechanism":"PLAIN",
                "sasl.username":"{}",
                "broker.address.family": "v4",
                "schema.registry.username": "{}",
                "fetch.wait.max.ms": "2000"
            }'
            CREDENTIALS '{
                "sasl.password": "ESTUARY_ACCESS_TOKEN",
                "schema.registry.password": "ESTUARY_ACCESS_TOKEN"
            }'
            INTO table test_table
            FORMAT AVRO SCHEMA REGISTRY 'https://dekaf.estuary-data.com'
            ( id <- id, server_name <- server_name, title <- title );
    ```
4. Your pipeline should now start ingesting data from Estuary Flow into SingleStore.
