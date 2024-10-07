---
sidebar_position: 2
---
import Mermaid from '@theme/Mermaid';

# Edit a draft created in the web app

When you [create](../create-dataflow.md) or [edit](../edit-data-flows.md) tasks in the web app, your work is periodically saved as a **draft**.
Specifically, each time you click the **Next** button to reveal the **Save and Publish** button, a draft is saved.

If you navigate away from your workflow in the web app before publishing, or if you simply prefer to finish up with flowctl,
you can pull the draft into a local environment, finish making changes, and publish the task.

<Mermaid chart={`
	graph LR;
    a[Catalog];
    d[Local files];
    c[Draft];
    d-- 2: Author to draft -->c;
    c-- 1: Pull draft -->d;
    c-- 3: Publish draft -->a;
`}/>

## Prerequisites

To complete this workflow, you need:

* An [Estuary account](../../getting-started/getting-started)

* [flowctl installed locally](../../getting-started/getting-started#get-started-with-the-flow-cli)

## Identify the draft and pull it locally

Drafts aren't currently visible in the Flow web app, but you can get a list with flowctl.

1. Authorize flowctl.

   1. [Generate an Estuary Flow refresh token](/guides/how_to_generate_refresh_token).

   2. Run `flowctl auth token --token <paste-token-here>`

2. Run `flowctl draft list`

  flowctl outputs a table of all the drafts to which you have access, from oldest to newest.

3. Use the name and timestamp to find the draft you're looking for.

  Each draft has an **ID**, and most have a name in the **Details** column. Note the **# of Specs** column.
  For drafts created in the web app, materialization drafts will always contain one specification.
  A number higher than 1 indicates a capture with its associated collections.

4. Copy the draft ID.

5. Select the draft: `flowctl draft select --id <paste-id-here>`.

6. Pull the draft source files to your working directory: `flowctl draft develop`.

7. Browse the source files.

  The source files and their directory structure will look slightly different depending on the draft.
  Regardless, there will always be a top-level file called `flow.yaml` that *imports* all other YAML files,
  which you'll find in a subdirectory named for your catalog prefix.
  These, in turn, contain the specifications you'll want to edit.

## Edit the draft and publish

Next, you'll make changes to the specification(s), test, and publish the draft.

1. Open the YAML files that contain the specification you want to edit.

2. Make changes. For guidance on how to construct Flow specifications, see the documentation for the entity type:

   * [Captures](../../concepts/captures.md#specification)
   * [Collections](../../concepts/collections.md#specification)
   * [Materializations](../../concepts/materialization.md#specification)

3. When you're done, sync the local work to the global draft: `flowctl draft author --source flow.yaml`.

  Specifying the top-level `flow.yaml` file as the source ensures that all entities in the draft are imported.

4. Publish the draft: `flowctl draft publish`

5. Once this operation completes successfully, check to verify if the entity or entities are live. You can:

   * Go to the appropriate tab in the Flow web app.

   * Run `flowctl catalog list`, filtering by `--name`, `--prefix`, or entity type, for example `--capture`.

If you're not satisfied with the published entities, you can continue to edit them.
See the other guides for help:

* [Edit in the web app](../edit-data-flows.md).
* [Edit with flowctl](./edit-specification-locally.md).
