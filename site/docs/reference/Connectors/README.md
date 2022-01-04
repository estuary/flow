# Connectors

Flow’s vision is to provide a common runtime against which any open connector may be run. Today Flow supports the [Airbyte specification](https://docs.airbyte.io/understanding-airbyte/airbyte-specification) as well as Flow’s low-latency gRPC protocols for captures and materializations.

Estuary is implementing a number of connectors, with a particular focus on integrating high-performance technical systems.
Connectors for SaaS APIs are already well-covered through Flow’s support of Airbyte connectors and the [singer.io](https://www.singer.io) ecosystem. Connectors implemented by Estuary are dual-licensed under Apache 2.0 or MIT.

## Why an open connector architecture?

Historically, data platforms have directly implemented integrations to external systems with which they interact. Today, there are simply so many systems and APIs that companies use, that it’s not feasible for a company to provide all possible integrations. Users are forced to wait indefinitely while the platform works through their prioritized integration list.

An open connector architecture removes Estuary — or any company — as a bottleneck in the development of integrations. Estuary contributes open-source connectors to the ecosystem, and in turn is able to leverage connectors implemented by others. Users are empowered to write their own connectors for esoteric systems not already covered by the ecosystem.

Furthermore, implementing a Docker-based community specification brings other important qualities to Estuary connectors:

* Cross-platform interoperability between Flow, Airbyte, and any other platform that supports the protocol
* Connectors can be written in any language and run on any machine
* Built-in solutions for version management (through image tags) and distribution.
* Container Image Registries allow you to integrate connectors from different sources at will, without the centralized control of a single company

## Configuration

Connectors interface with external systems, and universally require additional configuration, such as a database hostname or account credentials. A Flow catalog must provide the required configuration to a connector, and that configuration is verified and validated during the catalog build process.

### Protecting configured credentials

Flow integrates with Mozilla’s [sops](https://github.com/mozilla/sops) tool to encrypt and protect credentials within a GitOps-managed catalog and within the Flow data plane. Sops, short for “Secrets Operations,” is a tool that encrypts the values of a document, such as a connector configuration, against a key management system such as Google Cloud Platform KMS, Azure Key Vault, or Hashicorp Vault. Flow stores credentials in their encrypted form, decrypting them only when invoking a connector on the user’s behalf.

## Using connectors
Connectors are stored as packaged Docker images on GitHub. To implement a connector in your dataflow, you must connect the Flow runtime to that image. This tells Flow to configure your capture or materialization to the specifications of the desired endpoint system. You do this either directly, using a GitOps workflow to work with the catalog spec YAML files, or with the help of an application.

Once you've pointed Flow to the connector, you must supply the required configuration values for that connector.

The different processes you can use to implement connectors are each described below in general terms. Configuration details for each connector are described on their individual pages.

### GitOps workflow
With this method, you work in a local or virtualized development environment to edit the catalog spec files directly. Make sure you [set up your development environment](../../getting-started/installation.md) if you haven't already.

#### Capture connectors
Currently, the`flowctl discover` command is the provided method to begin setting up a capture, and saves significant time
compared to manually writing the catalog spec. `discover` generates a catalog spec file including the capture specification as well as the
**collections** you'll need to perpetuate each bound resource within the Flow runtime. This makes the `discover` workflow a quick way to start setting up a new data flow.

1. In your terminal, run:
```console
flowctl discover --image=${connector-image-link}
```
This generates a config from the latest version of the connector, provided as a Docker image.
:::tip
A list of connector image links can be found [here](./capture-connectors/README.md)
:::

2. Open the newly generated config file ending in `-config.yaml`. This is your space to specify the required values for the connector. Fill in required values and modify other values, if you'd like.
3. Run the command again:
```console
flowctl discover --image=${connector-image-link}
```
4. Open the resulting catalog spec file, which has a name ending in `.flow.yaml`.
It will include a capture definition with one or more bindings, and the collection(s) created to support each binding.

If you notice any undesired resources from the source were included in the catalog spec, you can remove its binding and corresponding collection to omit it from the dataflow.

#### Materialization connectors
Materialization connectors must be added manually to your catalog spec. Typically, you will have already generated a catalog spec with a capture and collections using `discover`. Now, you're simply adding to it to complete the dataflow.

1. Find your [materialization connector](./materialization-connectors/README.md) and use the provided code sample as a template.
2. Fill in the required values and other values, if desired.
3. Add as many additional bindings as you need. As with captures, each collection in your catalog must have an individual binding to be connected to the endpoint system.

### Credential Manager

This method is for Beta clients using Flow as a managed service.

The Estuary Credential Manager acts and feels like a simple user interface. In practice, it's a secure way to collect the configurations details for your use case, so that Estuary engineers can create and start your dataflow.

To use it, simply select your desired connector from the drop-down menu and fill out the required fields.

### Flow UI

:::info Beta
Flow UI is still undergoing development and will be available, with detailed documentation, in mid 2022.
:::

The Flow user interface is an alternative to the GitOps workflow, but both provide the same results and can be used interchangeably to work with the same Flow catalog.

In the UI, you select the connector you want to use and populate the fields that appear.

## Available connectors

A current list and configuration details for Estuary's connectors can be found on the following pages:
* [Capture connectors](./capture-connectors/)
* [Materialization connectors](./materialization-connectors/)

[Many additional connectors are available from Airbyte](https://airbyte.io/connectors). They function similarly but are limited to batch workflows, which Flow will run at a regular and configurable cadence.
