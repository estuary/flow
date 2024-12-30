# Apache Kafka

This connector materializes Flow collections into Apache Kafka topics.

[`ghcr.io/estuary/materialize-kafka:dev`](https://ghcr.io/estuary/materialize-kafka:dev)
provides the latest connector image. You can also follow the link in your browser to see past image
versions.

## Supported message formats

This connectors supports materializing Kafka messages encoded in Avro or JSON
format.

For Avro messages, the connector must be configured to use a [schema
registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

JSON messages may be materialized without a schema registry.

## Prerequisites

- A Kafka cluster with:
  - [bootstrap.servers](https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers)
    configured so that clients may connect via the desired host and port
  - An authentication mechanism of choice set up
  - Connection security enabled with TLS
- If using Avro message format with schema registry:
  - The endpoint to use for connecting to the schema registry
  - Username for authentication
  - Password for authentication

:::tip
If you are using the Confluent Cloud Schema Registry, your schema registry
username and password will be the **key** and **secret** from your schema
registry API key. See the [Confluent Cloud Schema Registry
Documentation](https://docs.confluent.io/cloud/current/get-started/schema-registry.html#create-an-api-key-for-ccloud-sr)
for help setting up a schema registry API key.
:::

### Authentication and connection security

A wide [variety of authentication
methods](https://kafka.apache.org/documentation/#security_overview) are
available for Kafka clusters. Flow supports SASL/SCRAM-SHA-256,
SASL/SCRAM-SHA-512, and SASL/PLAIN. When authentication details are not
provided, the client connection will attempt to use PLAINTEXT (insecure)
protocol.

If you don't already have authentication enabled on your cluster, Estuary
recommends either of the listed
[SASL/SCRAM](https://kafka.apache.org/documentation/#security_sasl_scram)
methods. With SCRAM, you set up a username and password, making it analogous to
the traditional authentication mechanisms you use in other applications.

:::tip
If you are connecting to Kafka hosted on Confluent Cloud, select the **PLAIN**
SASL mechanism.
:::

For connection security, Estuary recommends that you enable TLS encryption for
your SASL mechanism of choice, as well as all other components of your cluster.
Note that because TLS replaced now-deprecated SSL encryption, Kafka still uses
the acronym "SSL" to refer to TLS encryption. See [Confluent's
documentation](https://docs.confluent.io/platform/current/kafka/authentication_ssl.html)
for details.

:::info Beta
TLS encryption is currently the only supported connection security mechanism for
this connector. Other connection security methods may be enabled in the future.
:::

## Configuration

Use the below properties to configure the Apache Kafka materialization, which
will direct one or more of your Flow collections to your desired topics.

### Properties

#### Endpoint

| Property                        | Title                    | Description                                                                                                                                        | Type    | Required/Default        |
|---------------------------------|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|---------|-------------------------|
| **`/bootstrap_servers`**        | Bootstrap servers        | The initial servers in the Kafka cluster to connect to, separated by commas.                                                                       | string  | Required                |
| **`/message_format`**           | Message Format           | Format for materialized messages. Avro format requires a schema registry configuration. Messages in JSON format do not use a schema registry.      | string  | Required                |
| **`/topic_partitions`**         | Topic Partitions         | The number of partitions to create new topics with.                                                                                                | integer | 6                       |
| **`/topic_replication_factor`** | Topic Replication Factor | The replication factor to create new topics with.                                                                                                  | integer | 3                       |
| **`/credentials`**              | Credentials              | Connection details used to authenticate a client connection to Kafka via SASL.                                                                     | object  | Required                |
| `/tls`                          | TLS                      | TLS connection settings.                                                                                                                           | string  | `"system_certificates"` |
| `/credentials/auth_type`        | Authentication type      | The type of authentication to use. Currently supports `UserPassword`.                                                                              | string  |                         |
| `/credentials/mechanism`        | SASL Mechanism           | SASL mechanism describing how to exchange and authenticate client servers.                                                                         | string  |                         |
| `/credentials/username`         | Username                 | Username, if applicable for the authentication mechanism chosen.                                                                                   | string  |                         |
| `/credentials/password`         | Password                 | Password, if applicable for the authentication mechanism chosen.                                                                                   | string  |                         |
| `/schema_registry`              | Schema Registry          | Connection details for interacting with a schema registry.                                                                                         | object  |                         |
| `/schema_registry/endpoint`     | Schema Registry Endpoint | Schema registry API endpoint. For example: https://registry-id.us-east-2.aws.confluent.cloud.                                                      | string  |                         |
| `/schema_registry/username`     | Schema Registry Username | Schema registry username to use for authentication. If you are using Confluent Cloud, this will be the 'Key' from your schema registry API key.    | string  |                         |
| `/schema_registry/password`     | Schema Registry Password | Schema registry password to use for authentication. If you are using Confluent Cloud, this will be the 'Secret' from your schema registry API key. | string  |                         |

#### Bindings

| Property     | Title | Description                                | Type   | Required/Default |
|--------------|-------|--------------------------------------------|--------|------------------|
| **`/topic`** | Topic | Name of the Kafka topic to materialize to. | string | Required         |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
          bootstrap_servers: server1:9092,server2:9092
          tls: system_certificates
          credentials:
            auth_type: UserPassword
            mechanism: SCRAM-SHA-512
            username: bruce.wayne
            password: definitely-not-batman
          schema_registry:
            endpoint: https://schema.registry.com
            username: schemaregistry.username
            password: schemaregistry.password
    bindings:
      - resource:
          topic: ${TOPIC_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta updates

This connector supports [delta
updates](../../../concepts/materialization.md#delta-updates) for materializing
documents.