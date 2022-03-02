---
sidebar_position: 2
---
# Apache Kafka

This connector captures streaming data from Apache Kafka topics.

[`ghcr.io/estuary/source-kafka:dev`](https://github.com/estuary/connectors/pkgs/container/source-kafka) provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites

* A Kafka cluster with:
  * [bootstrap.servers](https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers) configured so that clients may connect via the desired host and port
  * An authentication mechanism of choice set up (highly recommended for production environments)
  * Connection security enabled with TLS (highly recommended for production environments)

### Authentication and connection security

Neither authentication nor connection security are enabled by default in your Kafka cluster, but both are important considerations.
Similarly, Flow's Kafka connectors do not strictly require authentication or connection security mechanisms.
You may choose to omit them for local development and testing; however, both are strongly encouraged for production environments.

A wide [variety of authentication methods](https://kafka.apache.org/documentation/#security_overview) is available in Kafka clusters.
Flow supports SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512, and SASL/PLAIN. Behavior using other authentication methods is not guaranteed.
When authentication details are not provided, the client connection will attempt to use PLAINTEXT (insecure) protocol.

If you don't already have authentication enabled on your cluster, Estuary recommends either of listed [SASL/SCRAM](https://kafka.apache.org/documentation/#security_sasl_scram) methods.
With SCRAM, you set up a username and password, making it analogous to the traditional authentication mechanisms
you use in other applications.

For connection security, Estuary recommends that you enable TLS encryption for your SASL mechanism of choice,
as well as all other components of your cluster.
Note that because TLS replaced now-deprecated SSL encryption, Kafka still uses the acronym "SSL" to refer to TLS encryption.
See [Confluent's documentation](https://docs.confluent.io/platform/current/kafka/authentication_ssl.html) for details.

:::info Beta
TLS encryption is currently the only supported connection security mechanism for this connector.
Other connection security methods may be enabled in the future.
:::

## Configuration

There are various ways to configure and implement connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and code sample below provide configuration details specific to the Apache Kafka source connector.

### Values

#### Endpoint

| Value | Name | Description | Type | Required/Default |
|---|---|---|---|---|
| `bootstrap_servers` | Bootstrap servers | The initial servers in the Kafka cluster to connect to. The Kafka client will be informed of the rest of the cluster nodes by connecting to one of these nodes. | array | Required |
| `tls`| TLS | TLS connection settings. | string | `"system_certificates"` |
| `authentication`| Authentication | Connection details used to authenticate a client connection to Kafka via SASL. | null, object | |
| `authentication/mechanism` | Mechanism | SASL mechanism describing how to exchange and authenticate client servers. | string |  |
| `authentication/password` | Password | Password, if applicable for the authentication mechanism chosen. | string | |
| `authentication/username` | Username | Username, if applicable for the authentication mechanism chosen. | string | |

#### Bindings

| Value | Name | Description | Type | Required/Default |
|-------|------|------|---------| --------|
| `stream` | Stream | Topic name. | string | Required |
| `syncMode` | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample
```yaml
captures:
  ${TENANT}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-kafka:dev
        config:
            bootstrap_servers: [localhost:9093]
            tls: system_certificates
            authentication:
                mechanism: SCRAM-SHA-512
                username: bruce.wayne
                password: definitely-not-batman
    bindings:
      - resource:
           stream: ${TOPIC_NAME}
           syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}
```

Your capture definition will likely be more complex, with additional bindings for each Kafka topic.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures).