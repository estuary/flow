# Integrating ClickHouse Cloud with Estuary Flow via Dekaf

:::warning
This guide uses a legacy method of connecting with Dekaf and is presented for historical purposes. For new integrations or to migrate your existing Dekaf setup to the new workflow, see the [Dekaf materialization connector](../materialization-connectors/Dekaf/dekaf.md).
:::

## Overview

This guide covers how to integrate ClickHouse Cloud with Estuary Flow using Dekaf, Estuary’s Kafka API compatibility
layer, and ClickPipes for real-time analytics. This integration allows ClickHouse Cloud users to stream data from a vast
array of sources supported by Estuary Flow directly into ClickHouse, using Dekaf for Kafka compatibility.

## Prerequisites

- **[ClickHouse Cloud](https://clickhouse.com/) account** with permissions to configure ClickPipes for data ingestion.
- **[Estuary Flow account](https://dashboard.estuary.dev/register)** with access to Dekaf and necessary connectors (
  e.g., Salesforce, databases).
- **Estuary Flow Refresh Token** to authenticate with Dekaf.

---

## Step 1: Configure Data Source in Estuary Flow

1. **Generate an [Estuary Refresh Token](/guides/how_to_generate_refresh_token)**:
    - To access the Kafka-compatible topics, create a refresh token in the Estuary Flow dashboard. This token will act
      as the password for both the broker and schema registry.

2. **Connect to Dekaf**:
    - Estuary Flow will automatically expose your collections as Kafka-compatible topics through Dekaf. No additional
      configuration is required.
    - Dekaf provides the following connection details:

       ```
       Broker Address: dekaf.estuary-data.com:9092
       Schema Registry Address: https://dekaf.estuary-data.com
       Security Protocol: SASL_SSL
       SASL Mechanism: PLAIN
       SASL Username: {}
       SASL Password: <Estuary Refresh Token>
       Schema Registry Username: {}
       Schema Registry Password: <Estuary Refresh Token>
       ```

---

## Step 2: Configure ClickPipes in ClickHouse Cloud

1. **Set Up ClickPipes**:
    - In ClickHouse Cloud, go to **Integrations** and select **Apache Kafka** as the data source.

2. **Enter Connection Details**:
    - Use the connection parameters from the previous step to configure access to Estuary Flow.

3. **Map Data Fields**:
    - Ensure that ClickHouse can parse the incoming data properly. Use ClickHouse’s mapping interface to align fields
      between Estuary Flow collections and ClickHouse tables.

4. **Provision the ClickPipe**:
    - Kick off the integration and allow ClickPipes to set up the pipeline (should complete within a few seconds).
