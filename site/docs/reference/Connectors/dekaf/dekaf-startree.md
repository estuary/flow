# StarTree

In this guide, you'll learn how to use Estuary Flow to push data streams to StarTree using the Kafka data source.

[StarTree](https://startree.ai/) is a real-time analytics platform built on Apache Pinot, designed for performing fast,
low-latency analytics on large-scale data.

## Prerequisites

- An Estuary Flow account & collection
- A StarTree account

## Connecting Estuary Flow to StarTree

1. **Create a new access token** to use for the StarTree connection. You can generate this token from the Estuary Admin
   Dashboard.

   ![Export Dekaf Access Token](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Group_22_95a85083d4/Group_22_95a85083d4.png)

2. In the StarTree UI, navigate to the **Data Sources** section and choose **Add New Data Source**.

3. Select **Kafka** as your data source type.

4. Enter the following connection details:

    - **Bootstrap Servers**: `dekaf.estuary.dev`
    - **Security Protocol**: `SASL_SSL`
    - **SASL Mechanism**: `PLAIN`
    - **SASL Username**: `{}` (Use your Estuary username or any placeholder if not specified)
    - **SASL Password**: `Your generated Estuary Refresh Token`

5. **Configure Schema Registry**: To decode Avro messages, enable schema registry settings:

    - **Schema Registry URL**: `https://dekaf.estuary.dev`
    - **Schema Registry Username**: `{}` (same as SASL Username)
    - **Schema Registry Password**: `The same Estuary Refresh Token as above`

6. Click **Create Connection** to proceed.
