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
 or add their specification files to a **draft**, where you can develop them further.

* `collections` allows you to work with your Flow collections. You can read the data from the collection and output it to stdout, or list the [journals](../concepts/advanced/journals.md) or journal fragments that comprise the collection. [Learn more about reading collections with flowctl](../concepts/collections.md#using-the-flowctl-cli).

* `draft` allows you to work with drafts. You can create, test, develop locally, and then **publish**, or deploy, them to the catalog.

You can access full documentation of all flowctl subcommands from the command line by passing the `--help` or `-h` flag, for example:

* `flowctl --help` lists top-level flowctl subcommands.

* `flowctl catalog --help` lists subcommands of `catalog`.

## Working with catalog drafts

Most of the commands you'll use to create, modify, and deploy Data Flows locally are found under the `flowctl draft`
and `flowctl catalog` subcommands.

The term **draft** refers to the source files that define different Flow entities in their unpublished state;
that is, the files in your local development environment.
The term **catalog** refers to the repository of published Flow entities.
Much of your work with flowctl involves moving specifications back and forth between a draft and the catalog,
so the two subcommands have a lot of overlap.

There's no single, correct way to complete these common workflows; flowctl offers multiple paths to accomplish the same goals.
Experiment to find the path that works best for you.

With `flowctl draft`, you can:

* Create new drafts or select existing drafts (1).

* Pull your selected draft into the current working directory (2).

* Develop the draft locally (3).

* Author your local changes to the draft. This is equivalent to syncing changes (4).

* Test and publish the draft to publish to the catalog (5).

<Mermaid chart={`
	graph LR;
    a((Start));
    s[Selected, synced draft];
    d[Local draft];
    c[Active catalog];
    s-- 2: flowctl draft develop -->d;
    d-- 4: flowctl draft author -->s;
    s-- 5: flowctl draft publish -->c;
    a-- 1: flowctl draft select or flowctl draft create -->s;
    d-- 3: Work locally -->d;
`}/>

With `flowctl catalog`, you can:

* List all the active specifications in the catalog, which you can then select to add to your draft.
You can filter the output by [prefix](../concepts/catalogs.md#namespace) or entity type.
For example, `flowctl catalog list --prefix acmeCo/sales/ --collections` only lists collections under the
`acmeCo/sales/` prefix.

* Add an active specification to your selected, synced draft (1).

* Add a group of active specifications directly to your local draft. You can refine results by prefix or entity type as described above (2).

  Note that if there are already files in your working directory, flowctl must reconcile them with the newly pulled specification.
  [Learn more about your options](#reconciling-specifications-in-local-drafts).

* Publish a draft directly from a local draft to the catalog.
This combines the functionality of `flowctl draft author` and `flowctl draft publish` (3).

* Test local drafts (4).

<Mermaid chart={`
	graph LR;
    a((Start));
    s[Selected, synced draft];
    d[Local draft];
    c[Active catalog];
    s-->d;
    c-- 1: flowctl catalog draft -->s
    d-- 4: flowctl catalog test -->d;
    d-- 3: flowctl catalog publish -->c;
    a-->s;
    c-- 2: flowctl catalog pull-specs -->d;
`}/>

### Reconciling specifications in local drafts

When you bypass your synced draft and pull specifications to your working directory directly using `flowctl catalog pull-specs`,
there may be conflicts between the existing files in that directory and the specifications you pull.

By default, `flowctl catalog pull-specs` will abort if it detects an existing file with the same name as a specification
it is attempting to pull. You can change this behavior with the `--existing` flag:

* `--existing=overwrite` pulls the new versions of conflicting files in place of the old versions.

* `--existing=keep` keeps the old versions of conflicting files.

* `--existing=merge-specs` performs a simple merge of new and old versions of conflicting files.
For example, if an existing `flow.yaml` file references collections a and b,
and the new version of `flow.yaml` references collections a and c,
the merged version will reference collections a, b, and c.

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

When you run commands like `flowctl catalog publish` or `flowctl draft author`, you can use the `--source-dir` flag
to push specifications from a directory other than your current working directory,
for example, `flowctl draft author --source-dir ../AcmeCoNew/marketing`.

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
