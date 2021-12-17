---
description: >-
  Connectors are plugin components that bridge the gap between Flow and your
  data systems.
---

# Connectors

Flow’s vision is to provide a common runtime against which any open connector may be run. Today Flow supports the [Airbyte specification](https://docs.airbyte.io/understanding-airbyte/airbyte-specification) as well as Flow’s low-latency gRPC protocols for captures and materializations.\
\
Estuary is implementing a number of connectors, with a particular focus on integrating high-performance technical systems. Connectors for SaaS APIs are already well-covered through Flow’s support of Airbyte connectors and the [singer.io](https://www.singer.io) ecosystem. Connectors implemented by Estuary are dual-licensed under Apache 2.0 or MIT.

### Why an open connector architecture?

Historically, data platforms have directly implemented integrations to external systems with which they interact. Today, there are simply so many systems and APIs that companies use, that it’s not feasible for a company to provide all possible integrations. Users are forced to wait indefinitely while the platform works through their prioritized integration list.

An open connector architecture removes Estuary — or any company — as a bottleneck in the development of integrations. Estuary contributes open-source connectors to the ecosystem, and in turn is able to leverage connectors implemented by others. Users are empowered to write their own connectors for esoteric systems not already covered by the ecosystem.

Furthermore, implementing a Docker-based community specification brings other important qualities to Estuary connectors:

* Cross-platform interoperability between Flow, Airbyte, and any other platform that supports the protocol&#x20;
* Connectors can be written in any language and run on any machine&#x20;
* Built-in solutions for version management (through image tags) and distribution.&#x20;
* Container Image Registries allow you to integrate connectors from different sources at will, without the centralized control of a single company

### Configuration

Connectors interface with external systems, and universally require additional configuration, such as a database hostname or account credentials. A Flow catalog must provide the required configuration to a connector, and that configuration is verified and validated during the catalog build process.

#### Protecting configured credentials

Flow integrates with Mozilla’s [sops](https://github.com/mozilla/sops) tool to encrypt and protect credentials within a GitOps-managed catalog and within the Flow data plane. Sops, short for “Secrets Operations,” is a tool that encrypts the values of a document, such as a connector configuration, against a key management system such as Google Cloud Platform KMS, Azure Key Vault, or Hashicorp Vault. Flow stores credentials in their encrypted form, decrypting them only when invoking a connector on the user’s behalf.

### Available connectors

The following Estuary connectors are available. Each connector has a unique configuration you must follow in your [catalog specification](concepts/catalog-entities/catalog-spec.md); these will be linked below the connector name.&#x20;

{% hint style="warning" %}
Beta note: configurations coming to the docs soon. [Contact the team](mailto:info@estuary.dev) for more information.&#x20;
{% endhint %}

Links to the package page on GitHub are also provided, including the most recent Docker image.

Estuary is actively developing new connectors, so check back regularly for the latest additions. We’re prioritizing the development of high-scale technological systems, as well as client needs.

#### Capture connectors

* Amazon Kinesis&#x20;
  * Configuration&#x20;
  * [Package ](https://github.com/estuary/connectors/pkgs/container/source-kinesis)
* Amazon S3&#x20;
  * Configuration&#x20;
  * [Package ](https://github.com/estuary/connectors/pkgs/container/source-s3)
* Apache Kafka&#x20;
  * Configuration&#x20;
  * [Package ](https://github.com/estuary/connectors/pkgs/container/source-kafka)
* Google Cloud Storage&#x20;
  * Configuration&#x20;
  * [Package ](https://github.com/estuary/connectors/pkgs/container/source-gcs)
* PostgreSQL&#x20;
  * Configuration&#x20;
  * [Package](https://github.com/estuary/connectors/pkgs/container/source-postgres)

#### Materialization connectors

* Apache Parquet&#x20;
  * Configuration&#x20;
  * [Package ](https://github.com/estuary/connectors/pkgs/container/materialize-s3-parquet)
* Elasticsearch&#x20;
  * Configuration&#x20;
  * [Package](https://github.com/estuary/connectors/pkgs/container/materialize-elasticsearch)&#x20;
* Google BigQuery&#x20;
  * Configuration&#x20;
  * [Package ](https://github.com/estuary/connectors/pkgs/container/materialize-bigquery)
* PostgreSQL&#x20;
  * Configuration&#x20;
  * [Package ](https://github.com/estuary/connectors/pkgs/container/materialize-postgres)
* Rockset&#x20;
  * Configuration&#x20;
  * [Package ](https://github.com/estuary/connectors/pkgs/container/materialize-rockset)
* Snowflake&#x20;
  * Configuration&#x20;
  * [Package ](https://github.com/estuary/connectors/pkgs/container/materialize-snowflake)
* Webhook&#x20;
  * Configuration&#x20;
  * [Package](https://github.com/estuary/connectors/pkgs/container/materialize-webhook)

#### Third-party connectors

[Many additional connectors are available from Airbyte](https://airbyte.io/connectors). They function similarly but are limited to batch workflows, which Flow will run at a regular and configurable cadence.\
