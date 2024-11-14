# Imply Polaris

This guide demonstrates how to use Estuary Flow to stream data to Imply Polaris using the Kafka-compatible Dekaf API.

[Imply Polaris](https://imply.io/polaris) is a fully managed, cloud-native Database-as-a-Service (DBaaS) built on Apache
Druid, designed for real-time analytics on streaming and batch data.

## Connecting Estuary Flow to Imply Polaris

1. [Generate a refresh token](/guides/how_to_generate_refresh_token) for the Imply Polaris connection from the Estuary
   Admin Dashboard.

2. Log in to your Imply Polaris account and navigate to your project.

3. In the left sidebar, click on "Tables" and then "Create Table".

4. Choose "Kafka" as the input source for your new table.

5. In the Kafka configuration section, enter the following details:

    - **Bootstrap Servers**: `dekaf.estuary-data.com:9092`
    - **Topic**: Your Estuary Flow collection name (e.g., `/my-organization/my-collection`)
    - **Security Protocol**: `SASL_SSL`
    - **SASL Mechanism**: `PLAIN`
    - **SASL Username**: `{}`
    - **SASL Password**: `Your generated Estuary Access Token`

6. For the "Input Format", select "avro".

7. Configure the Schema Registry settings:
    - **Schema Registry URL**: `https://dekaf.estuary-data.com`
    - **Schema Registry Username**: `{}` (same as SASL Username)
    - **Schema Registry Password**: `The same Estuary Access Token as above`

8. In the "Schema" section, Imply Polaris should automatically detect the schema from your Avro data. Review and adjust
   the column definitions as needed.

9. Review and finalize your table configuration, then click "Create Table".

10. Your Imply Polaris table should now start ingesting data from Estuary Flow.
