# Connecting to Estuary Flow from Kafka using Dekaf

**Dekaf** is Estuary Flow's Kafka API compatibility layer, allowing consumers to read data from Estuary Flow collections
as if they were Kafka topics. Additionally, Dekaf provides a schema registry API for managing schemas. This guide will
walk you through the steps to connect to Estuary Flow using Dekaf and its schema registry.

## Overview

- **Collections** represent datasets within Estuary Flow. All captured documents are written to a collection, and all
  materialized documents are read from a collection.
- **Dekaf** enables you to interact with these collections as though they were Kafka topics, providing seamless
  integration with existing Kafka-based tools and workflows.

## Key Features

- **Kafka Topic Emulation**: Access Estuary Flow collections as if they were Kafka topics.
- **Schema Registry Emulation**: Manage and retrieve schemas assigned to Estuary Flow collections, emulating Confluent's
  Schema Registry.

## Connection Details

To connect to Estuary Flow via Dekaf, use the following connection details in conjunction with a
[Dekaf materialization connector](../reference/Connectors/materialization-connectors/Dekaf/dekaf.md):

- **Broker Address**: `dekaf.estuary-data.com`
- **Schema Registry Address**: `https://dekaf.estuary-data.com`
- **Security Protocol**: `SASL_SSL`
- **SASL Mechanism**: `PLAIN`
- **SASL Username**: The full name of your Dekaf materialization, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`
- **SASL Password**: The auth token from your Dekaf materialization
- **Schema Registry Username**: The full name of your Dekaf materialization
- **Schema Registry Password**: The auth token from your Dekaf materialization

## How to Connect to Dekaf

### 1. Create a Dekaf materialization connector

1. From the [Estuary dashboard](https://dashboard.estuary.dev), navigate to the **Destinations** tab.

2. Click **New Materialization** and choose a Dekaf connector.

   - There are several Dekaf connector variations besides the generic "Dekaf," such as Tinybird. Currently, they don't behave appreciably differently from each other.
   You may use the different variations to keep your data organized, manage what data you share, and see at a glance where your data is going.

3. Provide a **name** and **auth token** to your materialization.

   - The full materialization name, which also includes your organization/prefix, will be used as the **username** when consumers connect to Dekaf.

   - The auth token that you provide will be used as the **password** when consumers connect to Dekaf. Make sure to use a secure token.

4. (Optional) Adjust additional configuration details, such as the **Strict Topic Names** or **Deletion Mode** settings.

5. Choose data collections to materialize. Click **Source from capture** or add individual collections.

   - Each collection you add to the materialization will be available for consumers to read as a **topic**. By default, the topic name is the collection name.

6. Select **Next**, then **Save and Publish**.

### 2. Set Up Your Kafka Client

Configure your Kafka client using the connection details provided.

#### Example Kafka Client Configuration

Below is an example configuration for a Kafka client using Python’s `kafka-python` library:

```python
from kafka import KafkaConsumer

# Configuration details
conf = {
    'bootstrap_servers': 'dekaf.estuary-data.com:9092',
    'security_protocol': 'SASL_SSL',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': 'Your_Org/Your_Prefix/Your_Materialization',
    'sasl_plain_password': 'Your_Auth_Token',
    'group_id': 'your_group_id',
    'auto_offset_reset': 'earliest'
}

# Create Consumer instance
consumer = KafkaConsumer(
    'your_topic_name',
    bootstrap_servers=conf['bootstrap_servers'],
    security_protocol=conf['security_protocol'],
    sasl_mechanism=conf['sasl_mechanism'],
    sasl_plain_username=conf['sasl_plain_username'],
    sasl_plain_password=conf['sasl_plain_password'],
    group_id=conf['group_id'],
    auto_offset_reset=conf['auto_offset_reset'],
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode('utf-8')
)

# Poll for messages
try:
    for msg in consumer:
        print(f"Received message: {msg.value}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

You can also use [kcat](https://github.com/edenhill/kcat) (formerly known as kafkacat) to test reading messages from an
Estuary Flow collection as if it were a Kafka topic.

```shell
kcat -C \
    -X broker.address.family=v4 \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanism=PLAIN \
    -X sasl.username="Your_Org/Your_Prefix/Your_Materialization" \
    -X sasl.password="Your_Auth_Token" \
    -b dekaf.estuary-data.com:9092 \
    -t "Your_Topic_Name" \
    -p 0 \
    -o beginning \
    -s avro \
    -r https://{Your_Org/Your_Prefix/Your_Materialization}:{Your_Auth_Token}@dekaf.estuary-data.com
```
