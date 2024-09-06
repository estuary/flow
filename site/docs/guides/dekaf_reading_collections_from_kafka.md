# Connecting to Estuary Flow from Kafka using Dekaf

:::note Dekaf is currently in beta.

Reporting is not yet supported for Dekaf, but is coming.

We're currently not charging for use of Dekaf, but will eventually charge under our standard data movement pricing

We appreciate your feedback as we continue to refine and enhance this feature.
:::

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

To connect to Estuary Flow via Dekaf, you need the following connection details:

- **Broker Address**: `dekaf.estuary.dev`
- **Schema Registry Address**: `https://dekaf.estuary.dev`
- **Security Protocol**: `SASL_SSL`
- **SASL Mechanism**: `PLAIN`
- **SASL Username**: `{}`
- **SASL Password**: Estuary Refresh Token ([Generate a refresh token](/guides/how_to_generate_refresh_token) in
  the dashboard)
- **Schema Registry Username**: `{}`
- **Schema Registry Password**: The same Estuary Refresh Token as above

## How to Connect to Dekaf

### 1. [Generate an Estuary Flow refresh token](/guides/how_to_generate_refresh_token)

### 2. Set Up Your Kafka Client

Configure your Kafka client using the connection details provided.

#### Example Kafka Client Configuration

Below is an example configuration for a Kafka client using Python’s `kafka-python` library:

```python
from kafka import KafkaConsumer

# Configuration details
conf = {
    'bootstrap_servers': 'dekaf.estuary.dev:9092',
    'security_protocol': 'SASL_SSL',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': '{}',
    'sasl_plain_password': 'Your_Estuary_Refresh_Token',
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
    -X sasl.username="{}" \
    -X sasl.password="Your_Estuary_Refresh_Token" \
    -b dekaf.estuary.dev:9092 \
    -t "full/nameof/estuarycolletion" \
    -p 0 \
    -o beginning \
    -s avro \
    -r https://{}:{Your_Estuary_Refresh_Token}@dekaf.estuary.dev
```
