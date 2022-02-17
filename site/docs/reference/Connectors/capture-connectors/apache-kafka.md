---
sidebar_position: 2
---
# Apache Kafka

This connector captures streaming data from Apache Kafka topics.

`ghcr.io/estuary/source-kafka:dev` provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites

OLIVIA WIP - need more context

* A Kafka cluster with:
    * A `config.json` file matching the layout of the spec command (???)
    * [bootstrap.servers](https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers) configured so that clients may connect via the desired host and port (this one makes sense)
* One or more Kafka topics within the cluster from which you'd like to capture data. For each:
    * A `catalog.json` file to allow Flow to properly discover data from Kafka

### `catalog.json` setup

(explanation about why this is needed and the layout here)

```json
{
  "streams": [
    {
      "name": "topic-name",
      "json_schema": {
        "type": "object"
      }
    }
  ],
  "estuary.dev/tail": true,
  "estuary.dev/range": {
    "begin": "00000000",
    "end": "ffffffff"
  }
}
```

### Setup

(alternative to the above. Assumption is only catalog.json will need excessive elaboration; otherwise, can doc each component here)
-config.json example
-catalog.json example
-bootstrap server

## Configuration

There are various ways to configure and implement connectors. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about these methods. The values and code sample below provide configuration details specific to the Apache Kafka source connector.

### Values

(TODO: break out authentication info into another section once you understand it)

| Value | Name | Description | Type | Required/Default |
|---|---|---|---|---|
| `authentication`| Authentication | The connection details for authenticating a client connection to Kafka via SASL. When not provided, the client connection will attempt to use PLAINTEXT (insecure) protocol. This must only be used in development or test environments. | null, object | * |
| `authentication/mechanism` | Mechanism | The SASL Mechanism describes how to exchange and authenticate client servers. For secure communication, TLS is required for all supported mechanisms.For more information about the Simple Authentication and Security Layer (SASL), see RFC 4422: https://datatracker.ietf.org/doc/html/rfc4422 For more information about Salted Challenge Response Authentication Mechanism (SCRAM), see RFC 7677. https://datatracker.ietf.org/doc/html/rfc7677 | string |  |
| `authentication/password` |  |  | string |  |
| `authentication/username` |  |  | string | |
| `bootstrap_servers` | Bootstrap servers | The initial servers in the Kafka cluster to connect to. The Kafka client will be informed of the rest of the cluster nodes by connecting to one of these nodes. | array | Required |
| `tls`| TLS | The TLS connection settings. | string | "system_certificates" |

### Sample
```YAML
captures:
  ${TENANT}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-kafka:dev
        config:
            bootstrap_servers: localhost:9093
            tls: system_certificates
            authentication:
                mechanism:
                username:
                password:
    bindings:
      - resource:
           namespace: ${STREAM_NAMESPACE} #maybe delete this Olivia!!!!!!
           stream: ${STREAM_NAME}
           syncMode: incremental
        target: ${TENANT}/${COLLECTION_NAME}

```