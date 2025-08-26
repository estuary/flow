
import Mermaid from '@theme/Mermaid';

# Dekaf Integrations

[Dekaf](/guides/dekaf_reading_collections_from_kafka.md) is Estuary Flow's Kafka-API compatibility layer.
It allows services to read data from Estuary Flow's collections as if they were topics in a Kafka cluster. This
functionality enables integrations with the Kafka ecosystem.

## Using Kafka or Dekaf

Estuary provides multiple options for integrating with a Kafka ecosystem. Which option you choose may depend on how much of the Kafka ecosystem you manage and how you are working with your data.

<Mermaid chart={`
	flowchart TD
        d1{Do you manage your own Kafka broker?} --> |No| d2{Do you have a Kafka consumer?}
        d2 --> |No| t1(Other sources/destinations)
        d2 --> |Yes| t2(Dekaf)
		d1 --> |Yes| d3{How do you want to connect to your broker?}
        d3 --> |Pull From| t3(Kafka capture)
        d3 --> |Send To| t4(Kafka materialization)
        click t1 href "https://docs.estuary.dev/reference/Connectors/" "Other connectors"
        click t2 href "https://docs.estuary.dev/reference/Connectors/materialization-connectors/Dekaf/" "Dekaf materialization"
        click t3 href "https://docs.estuary.dev/reference/Connectors/capture-connectors/apache-kafka/" "Kafka capture"
        click t4 href "https://docs.estuary.dev/reference/Connectors/materialization-connectors/apache-kafka/" "Kafka materialization"
`}/>

## Available Dekaf integrations

Dekaf is compatible with many Kafka consumers. For instructions on integrating with specific systems, choose from the following materialization connectors.

- [Bytewax](../materialization-connectors/Dekaf/bytewax.md)
- [ClickHouse](../materialization-connectors/Dekaf/clickhouse.md)
- [Imply Polaris](../materialization-connectors/Dekaf/imply-polaris.md)
- [Materialize](../materialization-connectors/Dekaf/materialize.md)
- [SingleStore](../materialization-connectors/Dekaf/singlestore.md)
- [Startree](../materialization-connectors/Dekaf/startree.md)
- [Tinybird](../materialization-connectors/Dekaf/tinybird.md)

You may use the [generic Dekaf materialization connector](../materialization-connectors/Dekaf/dekaf.md) for other use cases.
