---
sidebar_position: 3
---
# Edit Data Flows in the web app

You can edit existing captures, collection schemas, and materializations in the Flow web app.
For a tour of the web app, [start here](../concepts/web-app.md).

There are separate workflows for editing [captures](#edit-a-capture) and [materializations](#edit-a-materialization).
You can edit a collection schema using Flow's **Schema Inference** tool by editing either a capture or materialization associated
with that collection.

:::caution
Although you edit components of your Data Flows separately, they are all connected.
Flow's [schema evolution](../concepts/advanced/evolutions.md) and [AutoDiscover](../concepts/captures.md#automatically-update-captures) features help prevent common errors, but it's still important to be mindful of your edits' potential effects.
:::

## Edit a capture

1. Go to the [Sources page](https://dashboard.estuary.dev/captures) of the web app.

2. Locate the capture you'd like to edit. Click the **Options** button in its table row, then click **Edit specification**.

   The Edit Capture page opens.

3. Edit the connection to the destination system, if desired. You can either update fields in the Endpoint Config section or manually update the JSON in the Advanced Specification Editor.

:::caution
You may have to re-authenticate with the source system. Be sure to have current credentials on hand before editing the endpoint configuration.
:::

4. Use the **Output Collections** browser to add or remove collections from the capture, if desired.

   To refresh your connection with the source and see an updated list of possible collections, click the **Refresh** button,
   but be aware that it will overwrite all existing collection selections.

5. Use the **Schema Inference** tool, if desired.

   This option is available for source systems with permissive schemas, such as NoSQL databases and cloud storage.
   Flow can help you tighten up the schema to be used for downstream tasks in your Data Flow.

   1. In the Output Collections browser, choose a collection and click its **Collection** tab.

   2. Click **Schema Inference**

      The Schema Inference window appears. Flow scans the data in your collection and infers a new schema, called the [`readSchema`](../concepts/schemas.md#write-and-read-schemas), to use for
      downstream tasks like materializations and derivations.

   3. Review the new schema and click **Apply Inferred Schema**.

6. When you're done making changes, click **Next.**

8. Click **Save and Publish**.

Editing a capture only affects how it will work going forward.
Data that was captured before editing will reflect the original configuration.

## Edit a materialization

To edit a materialization:

1. Go to the [Destinations page](https://dashboard.estuary.dev/materializations) of the web app.

2. Locate the materialization you'd like to edit. Click the **Options** button in its table row, then click **Edit specification**.

   The Edit Materialization page opens.

3. Edit the connection to the destination system, if desired. You can either update fields in the Endpoint Config section or manually update the JSON in the Advanced Specification Editor.

:::caution
You may have to re-authenticate with the destination system. Be sure to have current credentials on hand before editing the endpoint configuration.
:::

4. Use the **Source Collections** browser to add or remove collections from the materialization, if desired.

6. Optionally apply a stricter schema to each collection to use for the materialization.

   This option is available for collections captured from source systems with permissive schemas, such as NoSQL databases and cloud storage.
   Flow can help you tighten up the schema to be used for downstream tasks in your Data Flow.

   1. In the Source Collections browser, choose a collection and click its **Collection** tab.

   2. Click **Schema Inference**

      The Schema Inference window appears. Flow scans the data in your collection and infers a new schema, called the [`readSchema`](../concepts/schemas.md#write-and-read-schemas), to use for the materialization.

   3. Review the new schema and click **Apply Inferred Schema**.

5. When you're done making changes, click **Next.**

6. Click **Save and Publish**.

Editing a materialization only affects how it will work going forward.
Data that was materialized before editing will reflect the original configuration.

## Advanced editing

For more fine-grain control over editing, you can use flowctl and work directly on specification files in your local environment.
[View the tutorial.](./flowctl/edit-specification-locally.md)