---
sidebar_position: 6
---
import Mermaid from '@theme/Mermaid';

# flowctl

There are two ways to work with Flow: through the web app, and using the flowctl command-line interface.
flowctl gives you more direct control over the files and directories that comprise your Data Flows.
You can work with any catalog to which you have [access](/reference/authentication), regardless of whether it was created from the command line or in the web app.

You can also authorize Flow users and roles and generate TypeScript modules to write custom transformations for your [derivations](derivations.md) — workflows that aren't yet available in the web app.

flowctl is the only Flow binary that you need to work with,
so distribution and upgrades are all simple.

## Installation and setup

flowctl binaries for MacOS and Linux are available. For Windows, [install Windows Subsystem for Linux (WSL)](https://docs.estuary.dev/concepts/flowctl/) to run Linux on Windows, or use a remote development environment.

1. Copy and paste the appropriate script below into your terminal. This will download flowctl, make it executable, and add it to your `PATH`.

   * For Linux:
   ```console
   sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-x86_64-linux' && sudo chmod +x /usr/local/bin/flowctl
   ```

   * For Mac:
   ```console
   sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-multiarch-macos' && sudo chmod +x /usr/local/bin/flowctl
   ```

   Alternatively, Mac users can install with Homebrew:
   ```console
   brew tap estuary/flowctl
   brew install flowctl
   ```

   You can also find the source files on GitHub [here](https://go.estuary.dev/flowctl).


2. To connect to your Flow account and start a session, [use an authentication token](/reference/authentication/#authenticating-flow-using-the-cli) from the web app.

## User guides

[View guides for common flowctl workflows](../guides/flowctl/README.md).

## flowctl subcommands

flowctl includes several top-level subcommands representing different functional areas. Each of these include multiple nested subcommands.
Important top-level flowctl subcommands are described below.

* `auth` allows you to authenticate your development session in your local development environment.
It's also how you provision Flow roles and users. Learn more about [authentication](/reference/authentication).

* `catalog` allows you to work with your organization's current active catalog entities. You can investigate the current Data Flows,
pull specifications for local editing, test and publish specifications that you wrote or edited locally,
and delete entities from the catalog.

* `collections` allows you to work with your Flow collections. You can read the data from the collection and output it to stdout, or list the [journals](../concepts/advanced/journals.md) or journal fragments that comprise the collection. [Learn more about reading collections with flowctl](../concepts/collections.md#using-the-flowctl-cli).

* `draft` provides an alternative method for many of the actions you'd normally perform with `catalog`, but common workflows have more steps.

* `generate` creates stub files and folder structures based on a provided `flow.yaml` file. This is helpful when [creating a derivation locally](../guides/flowctl/create-derivation.md#create-a-derivation-locally).

* `logs` allows you to review or follow logs for a particular task. This can be useful to help debug captures, derivations, and materializations.

You can access full documentation of all flowctl subcommands from the command line by passing the `--help` or `-h` flag, for example:

* `flowctl --help` lists top-level flowctl subcommands.

* `flowctl catalog --help` lists subcommands of `catalog`.

## Editing Data Flows with flowctl

flowctl allows you to work locally on the specification files that define your Data Flows.
You'll often need to move these specifications back and forth between your local environment and the **catalog**
of published entities.

The basic steps of this workflow are listed below, along with a diagram of the subcommands you'd use to accomplish them.
Keep in mind that there's no single, correct way to work with flowctl,
but we recommend this method to get started.

* List all the active specifications in the catalog, which you can then pull into your local environment.
You can filter the output by [prefix](../concepts/catalogs.md#namespace) or entity type.
For example, `flowctl catalog list --prefix acmeCo/sales/ --collections` only lists collections under the
`acmeCo/sales/` prefix.

* Pull a group of active specifications directly, resulting in local source files. You can refine results by prefix or entity type as described above (1).

  Note that if there are already files in your working directory, flowctl must reconcile them with the newly pulled specification.
  [Learn more about your options](#reconciling-specifications-in-local-drafts).

* Make edits locally.

* Test local specifications (2).

* Publish local specifications to the catalog (3).

<Mermaid chart={`
	graph LR;
    d[Local environment];
    c[Active catalog];
    d-- 2: flowctl catalog test -->d;
    d-- 3: flowctl catalog publish -->c;
    c-- 1: flowctl catalog pull-specs -->d;
`}/>

[View the step-by-step guide.](../guides/flowctl/edit-specification-locally.md)

### Reconciling specifications in local drafts

When you pull specifications to your working directory directly using `flowctl catalog pull-specs`,
there may be conflicts between the existing files in that directory and the specifications you pull.

By default, `flowctl catalog pull-specs` will abort if it detects an existing file with the same name as a specification
it is attempting to pull. You can change this behavior with the `--overwrite` flag.

Adding the `--overwrite` flag will pull the new versions of conflicting files in place of the old versions.

## Development directories

Flow specifications and other files are written to your working directory when you run `flowctl draft develop` or `flowctl catalog pull-specs`.

They typically include:

* `flow.yaml`:
  The main specification file that imports all other Flow specification files created in a single operation.
  As part of local development, you may add new specifications that you create as imports.

* `flow_generated/`:
  Directory of generated files, including TypeScript classes and interfaces.
  See [TypeScript code generation](#typescript-code-generation).

* `<prefix-name>/`:
  Directory of specifications that you pulled. Its name corresponds to your catalog prefix. Its contents will vary, but it may contain various YAML files and subdirectories.

* `package.json` and `package-lock.json`:
  Files used by `npm` to manage dependencies and your Data Flow's associated JavaScript project.
  You may customize `package.json`,
  but its `dependencies` stanza will be overwritten by the
  [npmDependencies](./import.md#importing-derivation-resources)
  of your Flow specification source files, if any exist.

When you run commands like `flowctl catalog publish` or `flowctl draft author`, you can use the `--source-dir` flag
to push specifications from a directory other than your current working directory,
for example, `flowctl draft author --source-dir ../AcmeCoNew/marketing`.

### TypeScript code generation

TypeScript files are used in the Flow catalog both as part of the automatic build process,
and to define lambdas functions for [derivations](./derivations.md), which requires your input.

As part of the Data Flow build process, Flow translates your
[schemas](schemas.md)
into equivalent TypeScript types on your behalf.
These definitions live within `flow_generated/` in your Data Flow's build directory,
and are frequently over-written by invocations of `flowctl`.
Files in this subdirectory are human-readable and stable.
You may want to commit them as part of a GitOps-managed project, but this isn't required.

Whenever you define a derivation that uses a [lambda](./derivations.md#lambdas),
you must define the lambda in an accompanying TypeScript module, and reference that module
in the derivation's definition. To facilitate this,
you can generate a stub of the module using `flowctl generate`
and simply write the function bodies.
[Learn more about this workflow.](./derivations.md#modules)

If a TypeScript module exists, `flowctl` will never overwrite it,
even if you update or expand your specifications such that the required interfaces have changed.

## Protecting secrets

Most endpoint systems require credentials of some kind,
such as a username or password.

Sensitive credentials should be protected while not in use.
The only time a credential needs to be directly accessed is when Flow initiates the connector.

Flow integrates with Mozilla’s [sops](https://github.com/mozilla/sops) tool,
which can encrypt and protect credentials.
It stores a `sops`-protected configuration in its encrypted form,
and decrypts it only when invoking a connector on your behalf.

sops, short for “Secrets Operations,” is a tool that encrypts the values of a JSON or YAML document
against a key management system (KMS) such as Google Cloud Platform KMS, Azure Key Vault, or Hashicorp Vault.
Encryption or decryption of a credential with `sops` is an active process:
it requires that the user (or the Flow runtime identity) have a current authorization to the required KMS,
and creates a request trace which can be logged and audited.
It's also possible to revoke access to the KMS,
which immediately and permanently removes access to the protected credential.

When you use the Flow web application or `flowctl`, Flow automatically
adds `sops` protection to sensitive fields on your behalf.

Most workflows can make use of this built-in encryption mechanism.
There is also the option to configure your own encryption with `sops` to maintain strict control over your encryption process.

### Using `flowctl`'s auto-encryption

Starting with version 0.5.18, `flowctl` will automatically encrypt plain-text connector endpoint configurations when you run one of the following commands:

* `draft author`
* `catalog test`
* `catalog publish`

These commands use the same encryption mechanism as the dashboard and ensure that your secrets are encrypted whenever you send your specifications to Estuary.

This does not, by default, encrypt secrets in your local environment.
If you want to encrypt local files, you can overwrite your local plain-text configuration with Estuary's encrypted version. To do so, run:

```bash
flowctl draft author --source your/flow.yaml
flowctl draft develop --overwrite
```

This will send a draft specification to Estuary without publishing it yet.
Estuary will encrypt the configuration as part of the `draft author` command, and you can pull this version back to overwrite your local file.

### Controlling encryption with `sops`

You can also implement `sops` manually if you are writing a Flow specification locally.

This can be useful if you need to maintain strict control over how credentials are encrypted.
In this case, you own the KMS key and grant Estuary access for decryption.
`flowctl` will not modify endpoint configurations that have already been encrypted.

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

## Troubleshooting

If you're developing locally with `flowctl`, watch out for these errors:

* `Failed to locate sops`: sops may not be installed correctly. See these [installation instructions](https://github.com/getsops/sops/releases) and ensure sops is on your PATH. For details on working with sops, see [Protecting secrets](#protecting-secrets) above.

* `Decrypting sops document failed`: ensure you have correctly applied a KMS key using sops to your configuration file. See above for [examples](#example-protect-a-configuration). Note that you will not be able to decrypt credentials entered via the Flow web app.

Since updates are released regularly, make sure you're using the latest version of `flowctl`. You can see the latest versions and changelogs on the [Flow releases](https://github.com/estuary/flow/releases) page.

To check your current version, run: `flowctl --version`

If you installed `flowctl` with Homebrew, you can upgrade with: `brew update && brew upgrade flowctl`
