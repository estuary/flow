# Apache Kafka

This connector captures streaming data from Apache Kafka topics.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-kafka:dev`](https://github.com/estuary/connectors/pkgs/container/source-kafka) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data types

This connector supports Kafka messages that contain JSON data.
Flow [collections](../../../concepts/collections.md) store data as JSON.
Before deploying this connector, you should modify [schema(s)](../../../concepts/schemas.md)
of the Flow collection(s) you're creating to reflect the structure of your JSON Kafka messages.

At this time, the connector does not support other data types in Kafka messages.

:::info Beta
Support for Avro Kafka messages will be added soon. For more information, [contact the Estuary team](mailto:info@estuary.dev).
:::

## Prerequisites

- A Kafka cluster with:
  - [bootstrap.servers](https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers) configured so that clients may connect via the desired host and port
  - An authentication mechanism of choice set up (highly recommended for production environments)
  - Connection security enabled with TLS (highly recommended for production environments)

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

### AWS Managed Streaming Kafka (MSK)

If using AWS Managed Streaming for Apache Kafka (MSK), you can use IAM authentication with our connector. Read more about IAM authentication with MSK in AWS docs: [IAM access control](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html).

Additionally, you want to make sure that your VPC configuration allows inbound and outbound requests to [Estuary Flow IP addresses](/reference/allow-ip-addresses).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Apache Kafka source connector.

### Properties

#### Endpoint

| Property                             | Title                 | Description                                                                                                                                                                          | Type         | Required/Default        |
| ------------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------ | ----------------------- |
| **`/bootstrap_servers`**             | Bootstrap servers     | The initial servers in the Kafka cluster to connect to, separated by commas. The Kafka client will be informed of the rest of the cluster nodes by connecting to one of these nodes. | string       | Required                |
| **`/tls`**                           | TLS                   | TLS connection settings.                                                                                                                                                             | string       | `"system_certificates"` |
| `/credentials`                       | Credentials           | Connection details used to authenticate a client connection to Kafka via SASL.                                                                                                       | null, object |                         |
| `/credentials/auth_type`             | Authentication type   | One of `UserPassword` for SASL or `AWS` for IAM authentication                                                                                                                       | string       |                         |
| `/credentials/mechanism`             | SASL Mechanism        | SASL mechanism describing how to exchange and authenticate client servers.                                                                                                           | string       |                         |
| `/credentials/password`              | Password              | Password, if applicable for the authentication mechanism chosen.                                                                                                                     | string       |                         |
| `/credentials/username`              | Username              | Username, if applicable for the authentication mechanism chosen.                                                                                                                     | string       |                         |
| `/credentials/aws_access_key_id`     | AWS Access Key ID     | Supply if using auth_type: AWS                                                                                                                                                       | string       |                         |
| `/credentials/aws_secret_access_key` | AWS Secret Access Key | Supply if using auth_type: AWS                                                                                                                                                       | string       |                         |
| `/credentials/region`                | AWS Region            | Supply if using auth_type: AWS                                                                                                                                                       | string       |                         |

#### Bindings

| Property        | Title     | Description                                    | Type   | Required/Default |
| --------------- | --------- | ---------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Kafka topic name.                              | string | Required         |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental` | string | Required         |

### Sample

User and password authentication (SASL):

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-kafka:dev
        config:
          bootstrap_servers: localhost:9093
          tls: system_certificates
          credentials:
            auth_type: UserPassword
            mechanism: SCRAM-SHA-512
            username: bruce.wayne
            password: definitely-not-batman
    bindings:
      - resource:
          stream: ${TOPIC_NAME}
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}
```

AWS IAM authentication:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-kafka:dev
        config:
          bootstrap_servers: localhost:9093
          tls: system_certificates
          credentials:
            auth_type: AWS
            aws_access_key_id: AK...
            aws_secret_access_key: secret
            region: us-east-1
    bindings:
      - resource:
          stream: ${TOPIC_NAME}
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}
```

Your capture definition will likely be more complex, with additional bindings for each Kafka topic.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures).
