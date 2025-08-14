# Apache Kafka

This connector captures streaming data from Apache Kafka topics.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-kafka:dev`](https://github.com/estuary/connectors/pkgs/container/source-kafka) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported message formats

This connectors supports Kafka messages encoded in Avro or JSON format.

For Avro messages, the connector must be configured to use a [schema
registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

JSON messages may be read without a schema registry. If the JSON messages were
encoded with a JSON schema, configuring a schema registry is recommended to
enable discovery of collection keys if the message key has an associated schema.

## Prerequisites

- A Kafka cluster with:
  - [bootstrap.servers](https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers) configured so that clients may connect via the desired host and port
  - An authentication mechanism of choice set up (highly recommended for production environments)
  - Connection security enabled with TLS (highly recommended for production environments)
- If using schema registry:
  - The endpoint to use for connecting to the schema registry
  - Username for authentication
  - Password for authentication
- Flat schemas, i.e. no use of schema references (`import`, `$ref`), as these are not currently supported

:::tip
If you are using the Confluent Cloud Schema Registry, your schema registry
username and password will be the **key** and **secret** from your schema
registry API key. See the [Confluent Cloud Schema Registry
Documentation](https://docs.confluent.io/cloud/current/get-started/schema-registry.html#create-an-api-key-for-ccloud-sr)
for help setting up a schema registry API key.
:::

### Discovered collection schemas

If no schema registry is configured, all available topics will be discovered and
use a collection key composed of the captured message's `partition` and
`offset`. If schema registry is configured, Flow collections for Kafka topics
will be discovered using the _latest_ version of the registered key schema for
the topic.

For a collection key to be discovered from a registered topic key schema, the
topic key schema must be compatible with a [Flow collection
key](../../../concepts/collections.md#keys), with the following additional considerations:
- Key fields must not contain `null` as a type
- Key fields can be a single type only
- Keys may contain nested fields, such as types with nested Avro records

If a topic has a registered key schema but it does not fit these requirements,
the default collection key of `parition` and `offset` will be used instead.

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

:::tip
If you are connecting to Kafka hosted on Confluent Cloud, select the **PLAIN**
SASL mechanism.
:::

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

| Property                                   | Title                                | Description                                                                                                                                                                          | Type         | Required/Default        |
|--------------------------------------------|--------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|-------------------------|
| **`/bootstrap_servers`**                   | Bootstrap servers                    | The initial servers in the Kafka cluster to connect to, separated by commas. The Kafka client will be informed of the rest of the cluster nodes by connecting to one of these nodes. | string       | Required                |
| **`/tls`**                                 | TLS                                  | TLS connection settings.                                                                                                                                                             | string       | `"system_certificates"` |
| `/credentials`                             | Credentials                          | Connection details used to authenticate a client connection to Kafka via SASL.                                                                                                       | null, object |                         |
| `/credentials/auth_type`                   | Authentication type                  | One of `UserPassword` for SASL or `AWS` for IAM authentication                                                                                                                       | string       |                         |
| `/credentials/mechanism`                   | SASL Mechanism                       | SASL mechanism describing how to exchange and authenticate client servers.                                                                                                           | string       |                         |
| `/credentials/password`                    | Password                             | Password, if applicable for the authentication mechanism chosen.                                                                                                                     | string       |                         |
| `/credentials/username`                    | Username                             | Username, if applicable for the authentication mechanism chosen.                                                                                                                     | string       |                         |
| `/credentials/aws_access_key_id`           | AWS Access Key ID                    | Supply if using auth_type: AWS                                                                                                                                                       | string       |                         |
| `/credentials/aws_secret_access_key`       | AWS Secret Access Key                | Supply if using auth_type: AWS                                                                                                                                                       | string       |                         |
| `/credentials/region`                      | AWS Region                           | Supply if using auth_type: AWS                                                                                                                                                       | string       |                         |
| **`/schema_registry`**                     | Schema Registry                      | Connection details for interacting with a schema registry.                                                                                                                           | object       | Required                |
| **`schema_registry/schema_registry_type`** | Schema Registry Type                 | Either `confluent_schema_registry` or `no_schema_registry`.                                                                                                                          | object       | Required                |
| `/schema_registry/endpoint`                | Schema Registry Endpoint             | Schema registry API endpoint. For example: https://registry-id.us-east-2.aws.confluent.cloud.                                                                                        | string       |                         |
| `/schema_registry/username`                | Schema Registry Username             | Schema registry username to use for authentication. If you are using Confluent Cloud, this will be the 'Key' from your schema registry API key.                                     | string       |                         |
| `/schema_registry/password`                | Schema Registry Password             | Schema registry password to use for authentication. If you are using Confluent Cloud, this will be the 'Secret' from your schema registry API key.                                  | string       |                         |
| `/schema_registry/enable_json_only`        | Capture Messages in JSON Format Only | If no schema registry is configured the capture will attempt to parse all data as JSON, and discovered collections will use a key of the message partition & offset.                 | boolean      |                         |


#### Bindings

| Property        | Title     | Description                                    | Type   | Required/Default |
| --------------- | --------- | ---------------------------------------------- | ------ | ---------------- |
| **`/topic`**   | Stream    | Kafka topic name.                              | string | Required         |

### Sample

User and password authentication (SASL):

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-kafka:dev
        config:
          bootstrap_servers: server1:9092,server2:9092
          tls: system_certificates
          credentials:
            auth_type: UserPassword
            mechanism: SCRAM-SHA-512
            username: bruce.wayne
            password: definitely-not-batman
          schema_registry:
            schema_registry_type: confluent_schema_registry
            endpoint: https://schema.registry.com
            username: schemaregistry.username
            password: schemaregistry.password
    bindings:
      - resource:
          topic: ${TOPIC_NAME}
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
          bootstrap_servers: server1:9092,server2:9092
          tls: system_certificates
          credentials:
            auth_type: AWS
            aws_access_key_id: AK...
            aws_secret_access_key: secret
            region: us-east-1
          schema_registry:
            schema_registry_type: confluent_schema_registry
            endpoint: https://schema.registry.com
            username: schemaregistry.username
            password: schemaregistry.password
    bindings:
      - resource:
          topic: ${TOPIC_NAME}
        target: ${PREFIX}/${COLLECTION_NAME}
```

Your capture definition will likely be more complex, with additional bindings for each Kafka topic.

[Learn more about capture definitions.](../../../concepts/captures.md).
