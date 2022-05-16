---
sidebar_position: 3
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connectors

**Connectors** are plugin components that bridge the gap between Flow’s runtime and
the various endpoints from which you capture or materialize data.
They're packaged as Docker images, each encapsulating the details of working with
a particular kind of endpoint.

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
* The abilities to write connectors in any language and run them on any machine
* Built-in solutions for version management (through image tags) and distribution
* The ability to integrate connectors from different sources at will, without the centralized control of a single company, thanks to container image registries

## Using connectors

Connectors are packaged as [Open Container](https://opencontainers.org/) (Docker) images,
and can be discovered, tagged, and pulled using
[Docker Hub](https://hub.docker.com/),
[GitHub Container registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry),
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
        # 1: Provide the image that implements your endpoint connector.
        image: ghcr.io/estuary/materialize-postgres:dev
        # 2: Provide endpoint configuration that the connector requires.
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
Often, you don't know what configuration a connector requires ahead of time,
or you may simply prefer a more guided workflow.

For this reason, connectors offer APIs that specify the configuration they may require,
or the resources they may have available.
Flow uses these APIs to offer guided workflows for easy configuration and usage of connectors.

The different processes you can use to implement connectors are each described below in general terms.
Configuration details for each connector are described on their individual pages.

:::info
Estuary is implementing better UI-driven workflows to easily
configure and use connectors, expected by Q2 2022.
The support offered today is rudimentary.
:::


### `flowctl discover`

The [`flowctl`](./flowctl.md) command-line tool offers a rudimentary guided workflow
for creating a connector instance through the `discover` sub-command

`discover` generates a catalog source file. It includes the capture specification
as well as recommended **collections**, which are bound to each captured resource of the endpoint.
This makes the `discover` workflow a quick way to start setting up a new data flow.

:::info Limitation
`flowctl discover` can fully scaffold catalog captures.
You can also use `flowctl discover`
to create stub configuration files for materialization connectors,
but the remainder of the materialization must be written manually.
:::

#### Step 1: Generate a configuration file stub

In your terminal, run:
```console
$ flowctl discover --image ghcr.io/estuary/${connector_name}:dev --prefix acmeCo/anvils

Creating a connector configuration stub at /workspaces/flow/acmeCo/anvils/source-postgres.config.yaml.
Edit and update this file, and then run this command again.
```

This command takes a connector Docker `--image`
and creates a configuration file stub,
which by default is written to the `--prefix` subdirectory of your current directory
— in this case `./acmeCo/anvils/source-postgres.config.yaml`.

:::tip
A list of connector images can be found [here](../reference/Connectors/capture-connectors/README.md).
:::

#### Step 2: Update your stubbed configuration file

Open and edit the generated config file.
It is pre-populated with configuration required by the connector,
their default values, and descriptive comments:

```yaml
database: postgres
# Logical database name to capture from.
# [string] (required)

host: ""
# Host name of the database to connect to.
# [string] (required)

password: ""
# User password configured within the database.
# [string] (required)

port: 5432
# Host port of the database to connect to.
# [integer] (required)
```

#### Step 3: Discover resources from your endpoint

Run the same command
to use your configuration file
to complete the discovery workflow.
Flow creates (or overwrites) a catalog source file within your directory,
which includes a capture definition with one or more bindings,
definitions of collections to support each binding,
and associated collection schemas:

```console
$ flowctl discover --image ghcr.io/estuary/${connector_name}:dev --prefix acmeCo/anvils

Created a Flow catalog at /workspaces/flow/acmeCo/anvils/source-postgres.flow.yaml
with discovered collections and capture bindings.
```

The generated `${connector_name}.flow.yaml` is the source file for your capture.
It will include a capture definition with one or more bindings,
and the collection(s) created to contain documents from each bound endpoint resource.
The capture and all collections are named using your chosen `--prefix`:

<Tabs>
<TabItem value="File Listing">

```console
$ find acmeCo/
acmeCo/
acmeCo/anvils
acmeCo/anvils/source-postgres.flow.yaml
acmeCo/anvils/source-postgres.config.yaml
acmeCo/anvils/my_table.schema.yaml
acmeCo/anvils/my_other_table.schema.yaml
```

</TabItem>
<TabItem value="source-postgres.flow.yaml">

```yaml
collections:
  acmeCo/anvils/my_table:
    schema: my_table.schema.yaml
    key: [/the/primary, /key]
  acmeCo/anvils/my_other_table:
    schema: my_other_table.schema.yaml
    key: [/other/key]
captures:
  acmeCo/anvils/source-postgres:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:dev
        config: source-postgres.config.yaml
    bindings:
      - resource:
          namespace: public
          stream: my_table
          syncMode: incremental
        target: acmeCo/anvils/my_table
      - resource:
          namespace: public
          stream: my_other_table
          syncMode: incremental
        target: acmeCo/anvils/my_other_table
```

</TabItem>
</Tabs>

You can repeat this step any number of times,
to re-generate and update your catalog sources
so that they reflect the endpoint's current resources.

#### Step 4: Inspect and trim your catalog

If you notice an undesired resources from the endpoint was included in the catalog spec,
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
Flow integrates with VS Code and other editors to offer auto-complete within catalog source files,
which makes it easier to write and structure your files.
:::

### Config Manager

This method is for Beta clients using Flow as a managed service.

The Estuary Config Manager acts and feels like a simple user interface. In practice,
it's a secure way to collect the configurations details for your use case, so that
Estuary engineers can create and start your dataflow.

To use it, simply select your desired connector from the drop-down menu and fill out the required fields.

### Flow UI

:::info Beta
Flow UI is still undergoing development and will be available, with detailed
documentation, in Q2 2022.
:::

The Flow user interface is an alternative to the GitOps workflow,
but both provide the same results and can be used interchangeably to work with the same Flow catalog.

In the UI, you select the connector you want to use and populate the fields that appear.

## Configuration

Connectors interface with external systems and universally require endpoint configuration,
such as a database hostname or account credentials,
which must be provided to the connector for it to function.
When directly working with catalog source files,
you have the option of inlining the configuration into your connector
or storing it in separate files:

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
<TabItem value="Referenced file">

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

Storing configuration in separate files serves two important purposes:
 * Re-use of configuration across multiple captures or materializations
 * The ability to protect sensitive credentials

### Protecting secrets

Most endpoint systems require credentials of some kind,
such as a username or password.

Directly storing secrets in files that are versioned in Git is poor practice.
Similarly, sensitive credentials should be protected while not in use within Flow's runtime as well.
The only time a credential needs to be directly accessed is when it's
required by Flow's runtime for the purposes of instantiating the connector.

Flow integrates with Mozilla’s [sops](https://github.com/mozilla/sops) tool,
which can encrypt and protect credentials within a GitOps-managed catalog.
Flow's runtime similarly stores a `sops`-protected configuration in its encrypted form,
and decrypts it only when invoking a connector on the user’s behalf.

sops, short for “Secrets Operations,” is a tool that encrypts the values of a JSON or YAML document
against a key management system (KMS) such as Google Cloud Platform KMS, Azure Key Vault, or Hashicorp Vault.
Encryption or decryption of a credential with `sops` is an active process:
it requires that the user (or the Flow runtime identity) have a current authorization to the required KMS,
and creates a request trace which can be logged and audited.
It's also possible to revoke access to the KMS,
which immediately and permanently removes access to the protected credential.

#### Example: Protect a configuration

Suppose you're given a connector configuration:

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

#### Example: Protect portions of a configuration

Endpoint configurations are typically a mix of sensitive and non-sensitive values.
It can be cumbersome when `sops` protects an entire configuration document as you
lose visibility into non-sensitive values, which you might prefer to store as
cleartext for ease of use.

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
provides a means for Flow to access the port indirectly through an SSH server. SSH tunneling is universally supported by Estuary's connectors.

To set up and configure the SSH server, see the [guide](../../guides/connect-network/).
Then, add the appropriate properties when you define the capture or materialization in the Flow web app,
or add the `networkTunnel` stanza directly to the YAML, as shown below.

#### Sample

```yaml title="materialize-postgres-ssh-tunnel.flow.yaml"
materializations:
  acmeCo/postgres-materialize-ssh:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-postgres:dev
        config:
          # When using a proxy like SSH tunneling, set to localhost
          host: localhost
          # Specify an open port on your local machine to connect to the proxy.
          port: 15432
          database: flow
          user: flow_user
          password: secret
          networkTunnel:
            sshForwarding:
              # Port on the local machine from which you'll connect to the SSH server.
              # If a port is specified elsewhere in the connector configuration, it must match.
              localPort: 15432
              # Host or IP address of the final endpoint to which you’ll
              # connect via tunneling from the SSH server
              forwardHost: 127.0.0.1
              # Port of the final endpoint to which you’ll connect via
              # tunneling from the SSH server.
              forwardPort: 5432
              # Location of the remote SSH server that supports tunneling.
              # Formatted as ssh://hostname[:port].
              sshEndpoint: ssh://198.21.98.1
              # Username to connect to the SSH server.
              user: sshUser
              # Private key to connect to the SSH server, formatted as multiline plaintext.
              # Use the YAML literal block style with the indentation indicator.
              # See https://yaml-multiline.info/ for details.
              privateKey: |2
                -----BEGIN RSA PRIVATE KEY-----
                MIICXAIBAAKBgQCJO7G6R+kv2MMS8Suw21sk2twHg8Vog0fjimEWJEwyAfFM/Toi
                EJ6r5RTaSvN++/+MPWUll7sUdOOBZr6ErLKLHEt7uXxusAzOjMxFKZpEARMcjwHY
                v/tN1A2OYU0qay1DOwknEE0i+/Bvf8lMS7VDjHmwRaBtRed/+iAQHf128QIDAQAB
                AoGAGoOUBP+byAjDN8esv1DCPU6jsDf/Tf//RbEYrOR6bDb/3fYW4zn+zgtGih5t
                CR268+dwwWCdXohu5DNrn8qV/Awk7hWp18mlcNyO0skT84zvippe+juQMK4hDQNi
                ywp8mDvKQwpOuzw6wNEitcGDuACx5U/1JEGGmuIRGx2ST5kCQQDsstfWDcYqbdhr
                5KemOPpu80OtBYzlgpN0iVP/6XW1e5FCRp2ofQKZYXVwu5txKIakjYRruUiiZTza
                QeXRPbp3AkEAlGx6wMe1l9UtAAlkgCFYbuxM+eRD4Gg5qLYFpKNsoINXTnlfDry5
                +1NkuyiQDjzOSPiLZ4Abpf+a+myjOuNL1wJBAOwkdM6aCVT1J9BkW5mrCLY+PgtV
                GT80KTY/d6091fBMKhxL5SheJ4SsRYVFtguL2eA7S5xJSpyxkadRzR0Wj3sCQAvA
                bxO2fE1SRqbbF4cBnOPjd9DNXwZ0miQejWHUwrQO0inXeExNaxhYKQCcnJNUAy1J
                6JfAT/AbxeSQF3iBKK8CQAt5r/LLEM1/8ekGOvBh8MAQpWBW771QzHUN84SiUd/q
                xR9mfItngPwYJ9d/pTO7u9ZUPHEoat8Ave4waB08DsI=
                -----END RSA PRIVATE KEY-----
        bindings: []
```

## Available connectors

[Learn about available connectors in the reference section](../reference/Connectors/README.md)
