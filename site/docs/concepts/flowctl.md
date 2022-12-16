---
sidebar_position: 7
---
import Mermaid from '@theme/Mermaid';

# flowctl

There are two ways to work with Flow: through the web app, and using the flowctl command-line interface.
flowctl gives you more direct control over the files and directories that comprise your Data Flows.
You can work with any catalog to which you have [access](../reference/authentication.md), regardless of whether it was created from the command line or in the web app.

You can also authorize Flow users and roles and generate Typescript modules to write custom transformations for your [derivations](derivations.md) — workflows that aren't yet available in the web app.

flowctl is the only Flow binary that you need to work with,
so distribution and upgrades are all simple.

## Installation and setup

flowctl binaries for MacOS and Linux are available.

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

2. To connect to your Flow account and start a session, [use an authentication token](../reference/authentication.md#authenticating-flow-using-the-cli) from the web app.

## flowctl subcommands

flowctl includes several top-level subcommands representing different functional areas. Each of these include multiple nested subcommands.
Important top-level flowctl subcommands are described below.

* `auth` allows you to authenticate your development session in your local development environment.
It's also how you provision Flow roles and users. Learn more about [authentication](../reference/authentication.md).

* `catalog` allows you to work with your organization's current active catalog entities. You can investigate the current Data Flows,
 or add their specification files to a **draft**, where you can develop them further.f

* `collections` allows you to work with your Flow collections. You can read the data from the collection and output it to stdout, or list the [journals](../concepts/advanced/journals.md) or journal fragments that comprise the collection. [Learn more about reading collections with flowctl](../concepts/collections.md#using-the-flowctl-cli).

* `draft` allows you to work with drafts. You can create, test, develop locally, and then **publish**, or deploy, them to the catalog.

You can access full documentation of all flowctl subcommands from the command line by passing the `--help` or `-h` flag, for example:

* `flowctl --help` lists top-level flowctl subcommands.

* `flowctl catalog --help` lists subcommands of `catalog`.

## Working with drafts

`flowctl draft` allows you to work with Flow specification files in the draft state and deploy changes to the catalog.
`draft` is an essential flowctl subcommand that you'll use often.

With `draft`, you:

* Create new drafts or convert active Data Flows into drafts.
* Pull a draft created in the web app or on the command line into your current working directory.
* Develop the draft locally.
* Author your local changes to the draft. This is equivalent to syncing changes.
* Test and publish the draft to publish to the catalog.

<Mermaid chart={`
	graph LR;
    a((Start));
    s[Selected, synced draft];
    d[Local draft];
    c[Active catalog];
    s-- flowctl draft develop -->d;
    d-- flowctl draft author -->s;
    s-- flowctl draft publish -->c;
    a-- flowctl draft select -->s;
    d-- Work locally -->d;
`}/>

## Development directories

Most of the work you perform with flowctl takes place remotely on Estuary infrastructure.
You'll only see files locally when you are actively developing a draft.

These files are created within your current working directory when you run `flowctl draft develop`.

They typically include:

* `flow.yaml`:
  The main specification file that imports all other Flow specification files in the current draft. As part of local development, you may add new specifications that you create as imports.
* `flow_generated/`:
  Directory of generated files, including TypeScript classes and interfaces.
  See [TypeScript code generation](#typescript-code-generation).
* `estuary/`:
  Directory of the draft's current specifications. Its contents will vary, but it may contain various YAML files and subdirectories.
* `package.json` and `package-lock.json`:
  Files used by `npm` to manage dependencies and your Data Flow's associated JavaScript project.
  You may customize `package.json`,
  but its `dependencies` stanza will be overwritten by the
  [npmDependencies](derivations.md#npm-dependencies)
  of your Flow specification source files, if any exist.

### TypeScript code generation

TypeScript files are used in the Flow catalog both as part of the automatic build process,
and to define lambdas functions for [derivations](./derivations.md), which requires your input.

As part of the Data Flow build process, Flow translates your
[schemas](schemas.md)
into equivalent TypeScript types on your behalf.
These definitions live within `flow_generated/` in your Data Flow's build directory ,
and are frequently over-written by invocations of `flowctl`.
Files in this subdirectory are human-readable and stable.
You may want to commit them as part of a GitOps-managed project, but this isn't required.

Whenever you define a derivation that uses a [lambda](./derivations.md#lambdas),
you must define the lambda in an accompanying TypeScript module, and reference that module
in the derivation's definition. To facilitate this,
you can generate a stub of the module using `flowctl typescript generate`
and simply write the function bodies.
[Learn more about this workflow.](./derivations.md#creating-typescript-modules)

If a TypeScript module exists, `flowctl` will never overwrite it,
even if you update or expand your specifications such that the required interfaces have changed.
