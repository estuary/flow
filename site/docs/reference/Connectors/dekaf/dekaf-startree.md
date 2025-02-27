# StarTree

:::warning
This guide uses a legacy method of connecting with Dekaf and is presented for historical purposes. For new integrations or to migrate your existing Dekaf setup to the new workflow, see the [Dekaf materialization connector](../materialization-connectors/Dekaf/dekaf.md).
:::

In this guide, you'll learn how to use Estuary Flow to push data streams to StarTree using the Kafka data source.

[StarTree](https://startree.ai/) is a real-time analytics platform built on Apache Pinot, designed for performing fast,
low-latency analytics on large-scale data.

## Connecting Estuary Flow to StarTree

1. [Generate a refresh token](/guides/how_to_generate_refresh_token) to use for the StarTree connection. You can
   generate this token from the Estuary Admin Dashboard.

2. In the StarTree UI, navigate to the **Data Sources** section and choose **Add New Data Source**.

3. Select **Kafka** as your data source type.

4. Enter the following connection details:

   ![Create StarTree Connection](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//startree_create_connection_548379d134/startree_create_connection_548379d134.png)

    - **Bootstrap Servers**: `dekaf.estuary-data.com`
    - **Security Protocol**: `SASL_SSL`
    - **SASL Mechanism**: `PLAIN`
    - **SASL Username**: `{}`
    - **SASL Password**: `Your generated Estuary Refresh Token`

5. **Configure Schema Registry**: To decode Avro messages, enable schema registry settings:

    - **Schema Registry URL**: `https://dekaf.estuary-data.com`
    - **Schema Registry Username**: `{}` (same as SASL Username)
    - **Schema Registry Password**: `The same Estuary Refresh Token as above`

6. Click **Create Connection** to proceed.
