---
sidebar_position: 5
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connectors

**Connectors** bridge the gap between Flow and
the various endpoints from which you capture or to which you materialize data.

Supported connectors are all available to you within the Flow web application.
From a technical perspective, they're packaged as [Docker images](https://github.com/orgs/estuary/packages?repo_name=connectors),
each encapsulating the details of working with a particular external system.

All connectors available in Flow are open-source, and many of them were built by Estuary.
Estuary connectors are dual-licensed under Apache 2.0 or MIT.
Flow also supports open-source connectors built by third parties, which Estuary independently tests and may alter slightly
for optimal performance within the Flow ecosystem.

Estuary’s vision is to provide a common runtime against which any open connector may be run.
To that end, Flow currently supports the
[Airbyte specification](https://docs.airbyte.io/understanding-airbyte/airbyte-specification)
as well as Flow’s low-latency gRPC protocols for captures and materializations.

## Using connectors

Most — if not all — of your Data Flows will use at least one connector.
You configure connectors within capture or materialization specifications.
When you publish one of these entities, you're also deploying all the connectors it uses.

You can interact with connectors using either the Flow web application or the flowctl CLI.

### Flow web application

The Flow web application is designed to assist you with connector configuration and deployment.
It's a completely no-code experience, but it's compatible with Flow's command line tools, discussed below.

When you add a capture or materialization in the Flow web app, choose the desired data system from the **Connector** drop-down menu.

The required fields for the connector appear below the drop-down. When you fill in the fields and click **Discover Endpoint**,
Flow automatically "discovers" the data streams or tables — known as **resources** — associated with the endpoint system.
From there, you can refine the configuration, save, and publish the resulting Flow specification.

### GitOps and flowctl

Connectors are packaged as [Open Container](https://opencontainers.org/) (Docker) images,
and can be tagged, and pulled using
[Docker Hub](https://hub.docker.com/),
[GitHub Container registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry),
or any other public image registry provider.

To interface with a connector, the Flow runtime needs to know:

1. The specific image to use, through an image name such as `ghcr.io/estuary/source-postgres:dev`.
   Notice that the image name also conveys the specific image registry and version tag to use.

2. Endpoint configuration such as a database address and account, with meaning that is specific to the connector.

3. Resource configuration such as a specific database table to capture, which is also specific to the connector.

To integrate a connector into your dataflow,
you must define all three components within your Flow specification.

The web application is intended to help you generate the Flow specification.
From there, you can use [flowctl](./flowctl.md) to refine it in your local environment.
It's also possible to manually write your Flow specification files, but this isn't the recommended workflow.

```yaml
materializations:
  acmeCo/postgres-views:
    endpoint:
      connector:
        # 1: Provide the image that implements your endpoint connector.
        # The `dev` tag uses the most recent version (the web app chooses this tag automatically)
        image: ghcr.io/estuary/materialize-postgres:dev
        # 2: Provide endpoint configuration that the connector requires.
        config:
          address: localhost:5432
          password: password
          database: postgres
          user: postgres
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

#### Configuration

Because connectors interface with external systems, each requires a slightly different **endpoint configuration**.
Here you specify information such as a database hostname or account credentials —
whatever that specific connector needs to function.

If you're working directly with Flow specification files,
you have the option of including the configuration inline
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
          address: localhost:5432
          password: password
          database: postgres
          user: postgres
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
address: localhost:5432
password: password
database: postgres
user: postgres
```

</TabItem>
</Tabs>

Storing configuration in separate files serves two important purposes:

- Re-use of configuration across multiple captures or materializations
- The ability to protect sensitive credentials

### Protecting secrets

Most endpoint systems require credentials of some kind,
such as a username or password.

Sensitive credentials should be protected while not in use.
The only time a credential needs to be directly accessed is when Flow initiates the connector.

Flow integrates with Mozilla’s [sops](https://github.com/mozilla/sops) tool,
which can encrypt and protect credentials.
It stores a `sops`-protected configuration in its encrypted form,
and decrypts it only when invoking a connector on the your behalf.

sops, short for “Secrets Operations,” is a tool that encrypts the values of a JSON or YAML document
against a key management system (KMS) such as Google Cloud Platform KMS, Azure Key Vault, or Hashicorp Vault.
Encryption or decryption of a credential with `sops` is an active process:
it requires that the user (or the Flow runtime identity) have a current authorization to the required KMS,
and creates a request trace which can be logged and audited.
It's also possible to revoke access to the KMS,
which immediately and permanently removes access to the protected credential.

When you use the Flow web application, Flow automatically
adds `sops` protection to sensitive fields on your behalf.
You can also implement `sops` manually if you are writing a Flow specification locally.
The examples below provide a useful reference.

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

You then use this `config.yaml` within your Flow specification.
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

`sops` re-writes the file, wrapping only values having a "\_sops" suffix and adding its `sops` metadata section:

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

You then use this `config.yaml` within your Flow specification.
Flow looks for and understands the `encrypted_suffix`,
and will remove this suffix from configuration keys before passing them to the connector.

### Connecting to endpoints on secure networks

In some cases, your source or destination endpoint may be within a secure network, and you may not be able
to allow direct access to its port due to your organization's security policy.

:::tip
If permitted by your organization, a quicker solution is to [allowlist the Estuary IP addresses](/reference/allow-ip-addresses)

For help completing this task on different cloud hosting platforms,
see the documentation for the [connector](../reference/Connectors/README.md) you're using.
:::

[SHH tunneling](https://www.ssh.com/academy/ssh/tunneling/example#local-forwarding), or port forwarding,
provides a means for Flow to access the port indirectly through an SSH server.
SSH tunneling is available in Estuary connectors for endpoints that use a network address for connection.

To set up and configure the SSH server, see the [guide](../../guides/connect-network/).
Then, add the appropriate properties when you define the capture or materialization in the Flow web app,
or add the `networkTunnel` stanza directly to the YAML, as shown below.

#### Sample

```yaml title="source-postgres-ssh-tunnel.flow.yaml"
captures:
  acmeCo/postgres-capture-ssh:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:dev
        config:
          address: 127.0.0.1:5432
          database: flow
          user: flow_user
          password: secret
          networkTunnel:
            sshForwarding:
              # Location of the remote SSH server that supports tunneling.
              # Formatted as ssh://user@hostname[:port].
              sshEndpoint: ssh://sshUser@198.21.98.1:22
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

## Why an open connector architecture?

Historically, data platforms have directly implemented integrations to external systems with which they interact.
Today, there are simply so many systems and APIs that companies use,
that it’s not feasible for a company to provide all possible integrations.
Users are forced to wait indefinitely while the platform works through their prioritized integration list.

An open connector architecture removes Estuary — or any company — as a bottleneck in the development of integrations.
Estuary contributes open-source connectors to the ecosystem, and in turn is able to leverage connectors implemented by others.
Users are empowered to write their own connectors for esoteric systems not already covered by the ecosystem.

Furthermore, implementing a Docker-based community specification brings other important qualities to Estuary connectors:

- Cross-platform interoperability between Flow, Airbyte, and any other platform that supports the protocol
- The abilities to write connectors in any language and run them on any machine
- Built-in solutions for version management (through image tags) and distribution
- The ability to integrate connectors from different sources at will, without the centralized control of a single company, thanks to container image registries

:::info
In order to be reflected in the Flow web app and used on the managed Flow platform,
connectors must be reviewed and added by the Estuary team. Have a connector you'd like to add?
[Contact us](mailto:info@estuary.dev).
:::

## Available connectors

[Learn about available connectors in the reference section](../reference/Connectors/README.md)
