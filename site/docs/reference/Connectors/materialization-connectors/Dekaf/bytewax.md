
# Bytewax

This connector materializes Flow collections as Kafka-compatible messages that a Bytewax Kafka consumer can read. [Bytewax](https://bytewax.io/) is a Python framework for building scalable dataflow applications, designed for
high-throughput, low-latency data processing tasks.

## Prerequisites

To use this connector, you'll need:

* At least one Flow collection
* A Python development setup

## Variants

This connector is a variant of the default Dekaf connector. For other integration options, see the main [Dekaf](dekaf.md) page.

## Setup

Provide an auth token when setting up the Dekaf connector. This can be a password of your choosing and will be used to authenticate consumers to your Kafka topics.

Once the connector is created, note the task name, such as `YOUR-ORG/YOUR-PREFIX/YOUR-MATERIALIZATION`. You will use this as the username.

## Connecting Estuary Flow to Bytewax

1. Install Bytewax and the Kafka Python client:

   ```
   pip install bytewax kafka-python
   ```

2. Create a Python script for your Bytewax dataflow. You can use the following template, inserting your own Kafka topic name(s), your materialization task name, and the auth token you created:

   ```python
   import json
   from datetime import timedelta
   from bytewax.dataflow import Dataflow
   from bytewax.inputs import KafkaInputConfig
   from bytewax.outputs import StdOutputConfig
   from bytewax.window import TumblingWindowConfig, SystemClockConfig

   # Estuary Flow Dekaf configuration
   KAFKA_BOOTSTRAP_SERVERS = "dekaf.estuary-data.com:9092"
   KAFKA_TOPIC = "/your-collection-name"

   # Parse incoming messages
   def parse_message(msg):
       data = json.loads(msg)
       # Process your data here
       return data

   # Define your dataflow
    src = KafkaSource(brokers=KAFKA_BOOTSTRAP_SERVERS, topics=[KAFKA_TOPIC], add_config={
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "YOUR_MATERIALIZATION_NAME",
        "sasl.password": os.getenv("DEKAF_AUTH_TOKEN"),
    })

    flow = Dataflow()
    flow.input("input", src)
    flow.input("input", KafkaInputConfig(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC))
    flow.map(parse_message)
    # Add more processing steps as needed
    flow.output("output", StdOutputConfig())

    if __name__ == "__main__":
        from bytewax.execution import run_main
        run_main(flow)
   ```

3. Run your Bytewax dataflow:

   ```
   python your_dataflow_script.py
   ```

4. Your Bytewax dataflow is now processing data from Estuary Flow in real-time.

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
        variant: bytewax
    bindings:
      - resource:
          topic_name: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
