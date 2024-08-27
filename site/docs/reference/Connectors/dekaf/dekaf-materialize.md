# Materialize

In this guide, you'll learn how to use Materialize to ingest data from Estuary Flow.

[Materialize](https://materialize.com/) is an operational data warehouse for real-time analytics that uses standard SQL
for defining transformations and queries.

## Prerequisites

- An [Estuary Flow](https://dashboard.estuary.dev/register) account & collection
- A Materialize account

## Connecting Estuary Flow to Materialize

1. **Create a new access token** to use for the Materialize connection. You can generate this token from the Estuary
   Admin Dashboard.

   ![Export Dekaf Access Token](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Group_22_95a85083d4/Group_22_95a85083d4.png)

2. In your Materialize dashboard, use the SQL shell to create a new secret and connection using the Kafka source
   connector. Use the following SQL commands to configure the connection to Estuary Flow:

   ```sql
   CREATE
   SECRET estuary_refresh_token AS
     'your_generated_token_here';
   
   CREATE
   CONNECTION estuary_connection TO KAFKA (
       BROKER 'dekaf.estuary.dev',
       SECURITY PROTOCOL = 'SASL_SSL',
       SASL MECHANISMS = 'PLAIN',
       SASL USERNAME = '{}',
       SASL PASSWORD = SECRET estuary_refresh_token
   );
   
   CREATE
   CONNECTION csr_estuary_connection TO CONFLUENT SCHEMA REGISTRY (
       URL 'https://dekaf.estuary.dev',
       USERNAME = '{}',
       PASSWORD = SECRET estuary_refresh_token
   );
   ```

3. **Create a source in Materialize** to read from the Kafka topic. Use the following SQL command,
   replacing `<name-of-your-flow-collection>` with the name of your collection in Estuary Flow:

   ```sql
   CREATE SOURCE materialize_source
   FROM KAFKA CONNECTION estuary_connection (TOPIC '<name-of-your-flow-collection>')
   FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_estuary_connection
   ENVELOPE UPSERT;
   ```

## Creating Real-Time Views

To begin analyzing the data, create a real-time view using SQL in Materialize. Here is an example query to create a
materialized view that tracks data changes:

```sql
CREATE MATERIALIZED VIEW my_view AS
SELECT *
FROM materialize_source;
```

## Final Steps

After configuring your source and creating the necessary views, the connection with Materialize is complete. New data
from your Estuary Flow collection will now arrive in your Materialize source in real-time, enabling you to perform
real-time analytics on live data streams.

For more detailed information on creating materialized views and other advanced configurations, refer to
the [Materialize documentation](https://materialize.com/docs/).

By following these steps, you can leverage the full potential of Estuary Flow and Materialize for real-time data
processing and analytics.
