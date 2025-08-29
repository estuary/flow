---
sidebar_position: 1
---
import Mermaid from '@theme/Mermaid';

# Edit a Flow specification locally

The [Flow web application](../../concepts/web-app.md) is designed to make the most common Flow tasks quick and easy.
With the app, you're able to create, monitor, and manage captures, materializations, and more.
For [creating basic Data Flows](../create-dataflow.md), the web app is by far the most efficient option,
and [basic editing capabilities](/guides/edit-data-flows) are provided.

However, advanced editing tasks are only possible using flowctl. These include:

* Manually editing collection schemas, for example, to add [projections](../../concepts/advanced/projections.md)
or change the [reduction strategy](../../reference/reduction-strategies/README.md).
* Editing, testing, and publishing multiple entities at once.
* Creating and editing derivations.

:::tip
A simplified development experience for derivations is available. You can use the web app to create a cloud-based development environment pre-populated with the components you need. Learn how [here](./create-derivation.md).
:::

This guide covers the basic procedure of pulling one or more live Flow entities to your local development environment,
editing their specifications, and re-publishing them.

<Mermaid chart={`
	graph LR;
    d[Local files];
    c[Catalog];
    d-- 2: Test -->d;
    d-- 3: Publish specifications -->c;
    c-- 1: Pull specifications -->d;
`}/>

## Prerequisites

To complete this workflow, you need:

* An [Estuary account](../../getting-started/getting-started.md)

* [flowctl installed locally](../get-started-with-flowctl.md)

* One or more published Flow entities. (To edit unpublished drafts, [use this guide](./edit-draft-from-webapp.md).)

## Pull specifications locally

Every *entity* (including active *tasks*, like captures and materializations, and static *collections*)
has a globally unique name in the Flow catalog.

For example, a given Data Flow may comprise:

* A capture, `myOrg/marketing/leads`, which writes to...
* Two collections, `myOrg/marketing/emailList` and `myOrg/marketing/socialMedia`, which are materialized as part of...
* A materialization, `myOrg/marketing/contacts`.

Using these names, you'll identify and pull the relevant specifications for editing.

1. Authorize flowctl.

   1. Go to the [CLI-API tab of the web app](https://dashboard.estuary.dev/admin/api) and copy your access token.

   2. Run `flowctl auth token --token <paste-token-here>`

2. Determine which entities you need to pull from the catalog. You can:

   * Check the web app's **Sources**, **Collections**, and **Destinations** pages.
  All published entities to which you have access are listed and can be searched.

   * Run `flowctl catalog list`. This command returns a complete list of entities to which you have access.
  You can refine by specifying a `--prefix` and filter by entity type:  `--captures`, `--collections`, `--materializations`, or `--tests`.

    From the above example, `flowctl catalog list --prefix myOrg/marketing --captures --materializations` would return
    `myOrg/marketing/leads` and `myOrg/marketing/contacts`.

3. Pull the specifications you need by running `flowctl catalog pull-specs`:

   * Pull one or more specifications by name, for example: `flowctl catalog pull-specs --name myOrg/marketing/emailList`

   * Pull a group of specifications by prefix or type filter, for example: `flowctl catalog pull-specs --prefix myOrg/marketing --collections`

   The source files are written to your current working directory.

4. Browse the source files.

  flowctl pulls specifications into subdirectories organized by entity name,
  and specifications sharing a catalog prefix are written to the same YAML file.

  Regardless of what you pull, there is always a top-level file called `flow.yaml` that *imports* all other nested YAML files.
  These, in turn, contain the entities' specifications.

## Edit source files and re-publish specifications

Next, you'll complete your edits, test that they were performed correctly, and re-publish everything.

1. Open the YAML files that contain the specification you want to edit.

2. Make changes. For guidance on how to construct Flow specifications, see the documentation for the task type:

   * [Captures](../../concepts/captures.md#specification)
   * [Collections](../../concepts/collections.md#specification)
   * [Materializations](../../concepts/materialization.md#specification)
   * [Derivations](../../concepts/derivations.md#specification)
   * [Tests](../../concepts/tests.md)

3. When you're done, you can test your changes:
`flowctl catalog test --source flow.yaml`

   You'll almost always use the top-level `flow.yaml` file as the source here because it imports all other Flow specifications
   in your working directory.

   Once the test has passed, you can publish your specifications.

4. Re-publish all the specifications you pulled: `flowctl catalog publish --source flow.yaml`

   Again you'll almost always want to use the top-level `flow.yaml` file. If you want to publish only certain specifications,
   you can provide a path to a different file.

5. Return to the web app or use `flowctl catalog list` to check the status of the entities you just published.
Their publication time will be updated to reflect the work you just did.

If you're not satisfied with the results of your edits, repeat the process iteratively until you are.