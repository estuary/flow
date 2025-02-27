# Bytewax

:::warning
This guide uses a legacy method of connecting with Dekaf and is presented for historical purposes. For new integrations or to migrate your existing Dekaf setup to the new workflow, see the [Dekaf materialization connector](../materialization-connectors/Dekaf/dekaf.md).
:::

This guide demonstrates how to use Estuary Flow to stream data to Bytewax using the Kafka-compatible Dekaf API.

[Bytewax](https://bytewax.io/) is a Python framework for building scalable dataflow applications, designed for
high-throughput, low-latency data processing tasks.

## Connecting Estuary Flow to Bytewax

1. [Generate a refresh token](/guides/how_to_generate_refresh_token) for the Bytewax connection from the Estuary Admin
   Dashboard.

2. Install Bytewax and the Kafka Python client:

   ```
   pip install bytewax kafka-python
   ```

3. Create a Python script for your Bytewax dataflow, using the following template:

   ```python
   import json
   from datetime import timedelta
   from bytewax.dataflow import Dataflow
   from bytewax.inputs import KafkaInputConfig
   from bytewax.outputs import StdOutputConfig
   from bytewax.window import TumblingWindowConfig, SystemClockConfig

   # Estuary Flow Dekaf configuration
   KAFKA_BOOTSTRAP_SERVERS = "dekaf.estuary-data.com:9092"
   KAFKA_TOPIC = "/full/nameof/your/collection"

   # Parse incoming messages
   def parse_message(msg):
       data = json.loads(msg)
       # Process your data here
       return data

   # Define your dataflow
    src = KafkaSource(brokers=KAFKA_BOOTSTRAP_SERVERS, topics=[KAFKA_TOPIC], add_config={
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "{}",
        "sasl.password": os.getenv("DEKAF_TOKEN"),
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

4. Replace `"/full/nameof/your/collection"` with your actual collection name from Estuary Flow.

5. Run your Bytewax dataflow:

   ```
   python your_dataflow_script.py
   ```

6. Your Bytewax dataflow is now processing data from Estuary Flow in real-time.
