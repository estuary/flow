---
slug: /guides/dekaf_reading_collections_from_kafka/
---

# Connecting to Kafka Using Dekaf

**Dekaf** is Estuary's Kafka API compatibility layer, allowing consumers to read data from Estuary collections
as if they were Kafka topics. Additionally, Dekaf provides a schema registry API for managing schemas. This guide will
walk you through the steps to connect to Estuary using Dekaf and its schema registry.

## Overview

- **Collections** represent datasets within Estuary. All captured documents are written to a collection, and all
  materialized documents are read from a collection.
- **Dekaf** enables you to interact with these collections as though they were Kafka topics, providing seamless
  integration with existing Kafka-based tools and workflows.

## Key Features

- **Kafka Topic Emulation**: Access Estuary collections as if they were Kafka topics.
- **Schema Registry Emulation**: Manage and retrieve schemas assigned to Estuary collections, emulating Confluent's
  Schema Registry.
- **Backfill Support**: Estuary signals to downstream consumers when offsets need to be reset via Kafka leader epochs.

## Connection Details

To connect to Estuary via Dekaf, use the following connection details in conjunction with a
[Dekaf materialization connector](/reference/Connectors/materialization-connectors/Dekaf):

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
Estuary collection as if it were a Kafka topic.

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

## Testing a Dekaf topic with kcat

When a consumer reports missing or unexpected records, read the topic directly with
[kcat](https://github.com/edenhill/kcat) to isolate whether Dekaf is serving the records
from how your client consumes them (partitions, offsets, deserialization). Start from the
consume example above and adjust as described below.

:::tip
That example pins a single partition with `-p 0`, which reads only that partition. On a
multi-partition topic that makes records on the other partitions look missing — omit `-p`
to read every partition, and add `-e` so kcat exits at end-of-topic instead of waiting for
more messages.
:::

**Confirm the topic is served and list its partitions** using metadata mode (`-L`):

```shell
kcat -L \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanism=PLAIN \
    -X sasl.username="Your_Org/Your_Prefix/Your_Materialization" \
    -X sasl.password="Your_Auth_Token" \
    -b dekaf.estuary-data.com:9092
```

**Count how many records are being served** across all partitions (connection flags
abbreviated as `...`):

```shell
kcat -C -e -q ... -t "Your_Topic_Name" -o beginning -f '%k\n' | wc -l
```

**Locate a specific record** and see which partition and offset it landed on:

```shell
kcat -C -e -q ... -t "Your_Topic_Name" -o beginning -f 'p=%p o=%o key=%k\n' | grep Your_Key
```

Format tokens: `%p` partition, `%o` offset, `%k` key, `%T` timestamp, `%s` payload.

If a record shows up here but not in your application, the problem is client-side: check
your consumer-group offsets and your deserializer. Permissive Avro/JSON decoders can
silently drop records that fail to decode — switch to a strict or fail-fast mode to surface
the error rather than discarding the record.

## Consumer behaviors

Dekaf maps Estuary collections (backed by Gazette journals) onto the Kafka protocol, so a
few behaviors differ from a native Kafka broker. These apply to every consumer (kcat,
librdkafka, Flink, Spark, or a custom client).

### Offsets are journal byte positions

Each journal in a collection is exposed as one Kafka partition, so a collection with N
journals has N partitions. The Kafka offset is the underlying journal byte position, not a
record counter: offsets advance by the size of each document, not by 1. Do not assume
contiguous or record-counting offsets, and do not compute a record count from an offset
range.

### The latest offset can briefly move backward

The high-water-mark (latest offset) that Dekaf advertises for a partition can momentarily
move backward during a routine broker hand-off, then recover within minutes. Recent data
you have already read may still be in the broker's memory before it is flushed to object
storage, and during a hand-off the advertised latest can briefly fall back to the last
flushed position.

This is a transient offset-reporting effect, not data loss: the records remain in the
collection and the latest offset catches back up on its own.

:::caution
Some clients treat any backward move of the latest offset as data loss. Apache Spark's
`failOnDataLoss=true` (its default) will abort the query, and librdkafka-based clients may
log `OFFSET_OUT_OF_RANGE` and reset per `auto.offset.reset`. Before concluding data was
lost, confirm the records are present with `flowctl collections read --collection <name>`
(narrow by time on a large collection), then resume from your committed offset. See the
Spark section below for handling.
:::

### Avro decoding honors logicalTypes

Decoded values follow their Avro `logicalType`. For example, a `string` with
`logicalType: uuid` deserializes to a UUID object in many clients (a `uuid.UUID` in
confluent-kafka Python), not a plain string. Comparing such a value against string IDs can
silently never match and look like missing records — normalize the type (for example,
`str(value)`) before comparing. This is a client-side decoding concern, not an Estuary one.

### Read partitions in parallel by splitting the collection

Because each journal is one Kafka partition, you parallelize reads by splitting the
collection into more journals. A split only distributes data written after it; to spread an
existing backlog across the new journals you also need to re-backfill from the source. A
split is a collection-level change, so every materialization on the collection sees the new
journals (non-breaking). Contact Estuary support before splitting a production collection.

## Reading from Apache Spark Structured Streaming

Spark's `kafka` source reads a Dekaf topic directly. Read [Consumer
behaviors](#consumer-behaviors) first; the items below are Spark-specific configuration on
top of those behaviors.

### Handle `failOnDataLoss`

Spark's default `failOnDataLoss=true` aborts the query whenever a partition's latest offset
moves backward, including the transient case described above, with no real loss. Choose one
of:

- Set `failOnDataLoss=false` and make your sink idempotent (deduplicate on the document
  key). With a checkpoint, Spark keeps your committed offset and continues once the latest
  offset recovers, so the transient case does not skip data.
- Keep `failOnDataLoss=true` and wrap the streaming query in an auto-restart that retries on
  this specific exception.

If you hit the error, the records are almost always still present: verify with
`flowctl collections read`, then restart (Spark resumes from its checkpoint).

### Set the Avro datetime rebase mode explicitly

Spark's `PERMISSIVE` Avro mode silently nulls values it cannot parse, so affected records
look empty or missing. Use `FAILFAST` while debugging to surface the real error. You can
then choose how you'd like to handle these values.

For example, dates before the Gregorian cutover, like `1582-10-15`, cannot be parsed with
permissive null-ing. With `FAILFAST`, the underlying
`INCONSISTENT_BEHAVIOR_CROSS_VERSION.READ_ANCIENT_DATETIME` error is exposed (SPARK-31404),
and you can set `spark.sql.avro.datetimeRebaseModeInRead` to handle old datetime values:

- Use `CORRECTED` to read values as-is
- Use `LEGACY` to rebase across the calendar difference

### Example reader options

```
.option("kafka.bootstrap.servers", "dekaf.estuary-data.com:9092")
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.mechanism", "PLAIN")
.option("kafka.sasl.jaas.config", "<JAAS config: username = Dekaf materialization name, password = auth token>")
.option("startingOffsets", "earliest")
.option("failOnDataLoss", "false")
.option("subscribe", "Your_Topic_Name")
```
