# Connectors

Flow’s vision is to provide a common runtime against which any open connector may be run.
Today Flow supports the
[Airbyte specification](https://docs.airbyte.io/understanding-airbyte/airbyte-specification)
as well as Flow’s low-latency gRPC protocols for captures and materializations.

Estuary is implementing a number of connectors,
with a particular focus on integrating high-performance technical systems.
Connectors for SaaS APIs are already well-covered through Flow’s support of
Airbyte connectors and the [singer.io](https://www.singer.io) ecosystem.
Connectors implemented by Estuary are dual-licensed under Apache 2.0 or MIT.

## Why an open connector architecture?

Historically, data platforms have directly implemented integrations to external systems with which they interact.
Today, there are simply so many systems and APIs that companies use,
that it’s not feasible for a company to provide all possible integrations.
Users are forced to wait indefinitely while the platform works through their prioritized integration list.

An open connector architecture removes Estuary — or any company — as a bottleneck in the development of integrations.
Estuary contributes open-source connectors to the ecosystem, and in turn is able to leverage connectors implemented by others.
Users are empowered to write their own connectors for esoteric systems not already covered by the ecosystem.

Furthermore, implementing a Docker-based community specification brings other important qualities to Estuary connectors:

* Cross-platform interoperability between Flow, Airbyte, and any other platform that supports the protocol
* Connectors can be written in any language and run on any machine
* Built-in solutions for version management (through image tags) and distribution.
* Container Image Registries allow you to integrate connectors from different sources at will, without the centralized control of a single company

## Using Connectors

Connectors are packaged as [Open Container](https://opencontainers.org/) (Docker) images,
and can be discovered, tagged, and pulled using
[Docker Hub](https://hub.docker.com/),
[GitHub Container Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry),
or any other public image registry provider.

To interface with a connector, the Flow runtime needs to know:

1. The specific image to use, through an image name such as `ghcr.io/estuary/source-postgres:dev`.
  Notice that the image name also conveys the specific image registry and version tag to use.

2. Endpoint configuration such as a database address and account, with meaning that is specific to the connector.

3. Resource configuration such as a specific database table to capture, which is also specific to the connector.


To integrate a connector within your dataflow,
you define all three components within your catalog specification:

```yaml
materializations:
  acmeCo/postgres-views:
    endpoint:
      connector:
        # 1: Provide the image which implements your endpoint connector.
        image: ghcr.io/estuary/materialize-postgres:dev
        # 2: Provide endpoint configuration which the connector requires.
        config:
          host: localhost
          password: password
          database: postgres
          user: postgres
          port: 5432
    bindings:
      - source: acmeCo/products/anvils
        # 3: Provide resource configuration for the binding between the Flow
        #    collection and the endpoint resource. This connector interfaces
        #    with a SQL database and its resources are database tables. Here,
        #    we provide a table to create and materialize which is bound to the
        #    `acmeCo/products/anvils` source collection.
        resource:
          table: anvil_products

      # Multiple resources can be configured through a single connector.
      # Bind additional collections to tables as part of this connector instance:
      - source: acmeCo/products/TNT
        resource:
          table: tnt_products

      - source: acmeCo/customers
        resource:
          table: customers
```

In some cases, you may be comfortable writing out the required configuration of your connector.
Often you don't know what configuration a connector requires ahead of time.
Or you may simply prefer a more guided workflow.

For this reason connectors offer APIs which specify the configuration they may require,
or the resources they may have available.
Flow uses these APIs to offer guided workflows for easy configuration and usage of connectors.

The different processes you can use to implement connectors are each described below in general terms.
Configuration details for each connector are described on their individual pages.

:::info
Estuary is implementing better UI-driven workflows to easily
configure and use connectors, expected by end of Q1 2022.
The support offered today is rudimentary.
:::


### `flowctl discover`

The [`flowctl`](flowctl.md) command-line tool offers a `discover` sub-command
which offers a rudimentary guided workflow for creating a connector instance.

:::info Limitation
Currently, `flowctl discover` is limited to creating catalog captures using a connector.
Materializations must be written manually.
:::

`discover` generates a catalog source file which includes the capture specification
as well as recommended **collections** which are bound to each captured resource of the endpoint.
This makes the `discover` workflow a quick way to start setting up a new data flow.

1. In your terminal, run:
```console
flowctl discover --image=ghcr.io/estuary/<connector-name>:dev
```

This generates a config from the latest version of the connector, provided as a Docker image.

:::tip
A list of connector images can be found [here](../reference/Connectors/capture-connectors/README.md)
:::

2. Open the newly generated config file called `discover-source-<connector-name>-config.yaml`.
  This is your space to specify the required values for the connector.
  Fill in required values and modify other values, if you'd like.

3. Run the command again:
```console
flowctl discover --image=ghcr.io/estuary/<connector-name>:dev
```

4. Open the resulting catalog spec file, `discover-source-<connector-name>.flow.yaml`.
   It will include a capture definition with one or more bindings, and the collection(s) created to support each binding.

If you notice any undesired resources from the endpoint were included in the catalog spec,
you can remove its binding and corresponding collection to remove it from your catalog.

### Editing with `flowctl check`

You can directly write your capture or materialization in a catalog source file,
and use `flowctl check` to provide a fast feedback loop to determine what configuration
may be missing or incorrect.

This is the current supported path for creating materializations.
Typically, you will have already have a catalog spec with a capture and collections using `discover`.
Now, you're simply adding a materialization to complete the dataflow.

1. Find your [materialization connector](../reference/Connectors/materialization-connectors/README.md)
   and use the provided code sample as a template.
2. Fill in the required values and other values, if desired.
3. Add as many additional bindings as you need.
   As with captures, each collection in your catalog must have an individual binding
   to be connected to the endpoint system.
4. Run `flowctl check` to verify that the connector can reach the endpoint system,
   and that all configuration is correct.

:::tip
Flow integrates with VSCode and other editors to offer auto-complete within catalog source files,
which makes it easier to write and structure your files.
:::

### Credential Manager

This method is for Beta clients using Flow as a managed service.

The Estuary Credential Manager acts and feels like a simple user interface. In practice, it's a secure way to collect the configurations details for your use case, so that Estuary engineers can create and start your dataflow.

To use it, simply select your desired connector from the drop-down menu and fill out the required fields.

### Flow UI

:::info Beta
Flow UI is still undergoing development and will be available, with detailed documentation, in mid 2022.
:::

The Flow user interface is an alternative to the GitOps workflow,
but both provide the same results and can be used interchangeably to work with the same Flow catalog.

In the UI, you select the connector you want to use and populate the fields that appear.

## Configuration

Connectors interface with external systems and universally require endpoint configuration,
such as a database hostname or account credentials,
which must be provided to the connector for it to function.
When directly working with catalog source files,
you have the option of inlining configuration into your connector
or storing it in separate files:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="Inline" default>

```yaml title="my.flow.yaml"
materializations:
  acmeCo/postgres-views:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-postgres:dev
        config:
          host: localhost
          password: password
          database: postgres
          user: postgres
          port: 5432
      bindings: []
```

</TabItem>
<TabItem value="Referenced File">

```yaml title="my.flow.yaml"
materializations:
  acmeCo/postgres-views:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-postgres:dev
        config: my.config.yaml
      bindings: []
```

```yaml title="my.config.yaml"
host: localhost
password: password
database: postgres
user: postgres
port: 5432
```

</TabItem>
</Tabs>

Storing configuration in separate files serves to important purposes:
 * Re-use of configuration across multiple captures or materializations.
 * Making it possible to protect your sensitive credentials.

### Protecting Secrets

Most endpoint systems require credentials of some kind,
such as a username or password.

Directly storing secrets in files which are versioned in Git is poor practice.
Similarly, sensitive credentials should be protected while not in use within Flow's runtime as well.
The only time a credential needs to be directly accessed is when it's
required by Flow's runtime for the purposes of instantiating the connector.

Flow integrates with Mozilla’s [sops](https://github.com/mozilla/sops) tool,
which can encrypt and protect credentials within a GitOps-managed catalog.
Flow's runtime similarly stores a `sops`-protected configuration in its encrypted form,
and decrypts it only when invoking a connector on the user’s behalf.

Sops, short for “Secrets Operations,” is a tool that encrypts the values of a JSON or YAML document
against a key management system (KMS) such as Google Cloud Platform KMS, Azure Key Vault, or Hashicorp Vault.
Encryption or decryption of a credential with `sops` is an active process:
it requires that the user (or the Flow runtime identity) have a current authorization to the required KMS,
and creates a request trace which can be logged and audited.
It's also possible to revoke access to the KMS,
which immediately and permanently removes access to the protected credential.

#### Example: Protect a Configuration

Suppose your given a connector configuration:

```yaml title="config.yaml"
host: my.hostname
password: "this is sensitive!"
user: my-user
```

You can protect it using a Google KMS key that you own:

```bash
# Login to Google Cloud and initialize application default credentials used by `sops`.
$ gcloud auth application-default login
# Use `sops` to re-write the configuration document in place, protecting its values.
$ sops --encrypt --in-place --gcp-kms projects/your-project-id/locations/us-central1/keyRings/your-ring/cryptoKeys/your-key-name config.yaml
```

`sops` re-writes the file, wrapping each value in an encrypted envelope and adding a `sops` metadata section:

```yaml title="config.yaml"
host: ENC[AES256_GCM,data:K/clly65pThTg2U=,iv:1bNmY8wjtjHFBcXLR1KFcsNMGVXRl5LGTdREUZIgcEU=,tag:5GKcguVPihXXDIM7HHuNnA==,type:str]
password: ENC[AES256_GCM,data:IDDY+fl0/gAcsH+6tjRdww+G,iv:Ye8st7zJ9wsMRMs6BoAyWlaJeNc9qeNjkkjo6BPp/tE=,tag:EPS9Unkdg4eAFICGujlTfQ==,type:str]
user: ENC[AES256_GCM,data:w+F7MMwQhw==,iv:amHhNCJWAJnJaGujZgjhzVzUZAeSchEpUpBau7RVeCg=,tag:62HguhnnSDqJdKdwYnj7mQ==,type:str]
sops:
    # Some items omitted for brevity:
    gcp_kms:
        - resource_id: projects/your-project-id/locations/us-central1/keyRings/your-ring/cryptoKeys/your-key-name
          created_at: "2022-01-05T15:49:45Z"
          enc: CiQAW8BC2GDYWrJTp3ikVGkTI2XaZc6F4p/d/PCBlczCz8BZiUISSQCnySJKIptagFkIl01uiBQp056c
    lastmodified: "2022-01-05T15:49:45Z"
    version: 3.7.1
```

You then use this `config.yaml` within your Flow catalog.
The Flow runtime knows that this document is protected by `sops`
will continue to store it in its protected form,
and will attempt a decryption only when invoking a connector on your behalf.

If you need to make further changes to your configuration,
edit it using `sops config.yaml`.
It's not required to provide the KMS key to use again,
as `sops` finds it within its metadata section.

:::important
When deploying catalogs onto the managed Flow runtime,
you must grant access to decrypt your GCP KMS key to the Flow runtime service agent,
which is:

```
flow-258@helpful-kingdom-273219.iam.gserviceaccount.com
```
:::

:::warning
Due to a known issue within `sops` and Flow,
you must currently order your configuration files in their natural key-sorted order.
See https://github.com/estuary/flow/issues/303
:::

#### Example: Protect Portions of a Configuration

Endpoint configurations are typically a mix of sensitive and non-sensitive values.
It can be cumbersome when `sops` protects an entire configuration document as you
lose visibility into non-sensitive values, which you might prefer to store as
cleartext for eas of use.

You can use the encrypted-suffix feature of `sops` to selectively protect credentials:

```yaml title="config.yaml"
host: my.hostname
password_sops: "this is sensitive!"
user: my-user
```

Notice that `password` in this configuration has an added `_sops` suffix.
Next, encrypt only values which have that suffix:

```bash
$ sops --encrypt --in-place --encrypted-suffix "_sops" --gcp-kms projects/your-project-id/locations/us-central1/keyRings/your-ring/cryptoKeys/your-key-name config.yaml
```

`sops` re-writes the file, wrapping only values having a "_sops" suffix and adding its `sops` metadata section:

```yaml title="config.yaml"
host: my.hostname
password_sops: ENC[AES256_GCM,data:dlfidMrHfDxN//nWQTPCsjoG,iv:DHQ5dXhyOOSKI6ZIzcUM67R6DD/2MSE4LENRgOt6GPY=,tag:FNs2pTlzYlagvz7vP/YcIQ==,type:str]
user: my-user
sops:
    # Some items omitted for brevity:
    encrypted_suffix: _sops
    gcp_kms:
        - resource_id: projects/your-project-id/locations/us-central1/keyRings/your-ring/cryptoKeys/your-key-name
          created_at: "2022-01-05T16:06:36Z"
          enc: CiQAW8BC2Au779CGdMFUjWPhNleCTAj9rL949sBvPQ6eyAC3EdESSQCnySJKD3eWX8XrtrgHqx327
    lastmodified: "2022-01-05T16:06:37Z"
    version: 3.7.1
```

You then use this `config.yaml` within your Flow catalog.
Flow looks for and understands the `encrypted_suffix`,
and will remove this suffix from configuration keys before passing them to the connector.

### Connecting to endpoints on secure networks

In some cases, your source or destination endpoint may be within a secure network, and you may not be able
to allow direct access to its port due to your organization's security policy.

[SHH tunneling](https://www.ssh.com/academy/ssh/tunneling/example#local-forwarding), or port forwarding,
provides a means for Flow to access the port indirectly through an SSH server.

To set up and configure the SSH server, see the [guide](../../guides/connect-network/).

:::info Beta
Currently, Flow supports SSH tunneling on a per-connector basis; consult the appropriate connector's documentation
to verify. Estuary plans to expand this to universally cover all connectors in the future. Additionally,
we'll add support for other means of secure connection.
:::

After verifying that the connector is supported, you can add a `proxy` stanza to the capture or materialization
definition to enable SSH tunneling.

```yaml title="postgres-ssh-tunnel.flow.yaml"
captures:
  acmeCo/postgres-ssh:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:dev
        config:
          # When using a proxy like SSH tunneling, set to localhost
          host: localhost
          # 0 allows Flow to dynamically choose an open port on your local machine
          # to connect to the proxy (recommended).
          # If necessary, you're free to specify a different port.
          port: 0
          database: flow
          user: flow_capture
          password: secret
          proxy:
            # Support for other proxy types will be enabled in the future.
            proxy_type: ssh_forwarding
              ssh_forwarding:
                # Location of the remote SSH server that supports tunneling.
                ssh_endpoint: 198.21.98.1
                # Base64-encoded private key to connect to the SSH server.
                ssh_private_key_base64: wjkEpr7whDZQ8UqIYI4RcNRuithu7chNZg
                # Username to connect to the SSH server.
                ssh_user: ssh_user
                # Host or IP address of the final endpoint to which you’ll
                # connect via tunneling from the SSH server
                remote_host: 127.0.0.1
                # Port of the final endpoint to which you’ll connect via
                # tunneling from the SSH server.
                remote_port: 5432
                # Port on the local machine from which you'll connect to the SSH server.
                # This must match port, above.
                local_port: 0
      bindings: []
```
## Available Connectors

[Learn about available connectors in the reference section](../../reference/Connectors/)