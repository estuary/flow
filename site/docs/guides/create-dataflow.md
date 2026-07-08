---
sidebar_position: 1
---
# Create a Basic Data Flow

This guide walks you through the process of creating an end-to-end Data Flow.

## Prerequisites

This guide is intended for new users and briefly introduces Estuary's key concepts.
Though it's not required, you may find it helpful to read
the [high level concepts](../concepts/README.md#essential-concepts) documentation for more detail before you begin.

## Introduction

In Estuary, you create Data Flows to connect data **source** and **destination** systems.

The simplest Data Flow comprises three types of entities:

* A data **capture**, which ingests data from an external source
* One or more **collections**, which store that data in a cloud-backed data lake
* A **materialization**, to push the data to an external destination

The capture and materialization each rely on a **connector**.
A connector is a plug-in component that interfaces between Estuary and whatever data system you need to connect to.
Here, we'll walk through how to leverage various connectors, configure them, and deploy your Data Flow.

## Create a capture

You'll first create a **capture** to connect to your data source system.
This process will create one or more **collections** in Estuary, which you can then materialize to another system.

1. Go to the Estuary dashboard at [dashboard.estuary.dev](https://dashboard.estuary.dev/) and sign in.

2. Click the **Sources** tab and choose **New Capture**.

3. Choose the appropriate **Connector** for your desired data source.

   A form appears with the properties required for that connector.
   A documentation page with details about that connector appears in the side panel.
   You can also browse the [connectors reference](../reference/Connectors/capture-connectors/README.md) in your browser.

4. Type a name for your capture.

   Your capture name must begin with a [prefix](../concepts/catalogs.md#namespace) to which you [have access](/reference/authentication).

    In the **Name** field, click the drop-down arrow and select an available prefix.
    Append a unique capture name after the `/` to create the full name, for example `acmeCo/myFirstCapture`.

5. Fill out the required properties and click **Next**.

   Estuary uses the provided information to initiate a connection to the source system.
   It identifies one or more data **resources** — these may be tables, data streams, or something else, depending on the connector. These are each mapped to a **collection**.

   The **Target Collections** section shows this list of available collections.
   You can decide which ones you want to capture.

6. Look over the list of available collections. All are selected by default.
You can remove collections you don't want to capture, change collection names, and for some connectors, modify other properties.

:::tip
Narrow down a large list of available collections by typing in the **Filter Bindings** box.
:::

7. Select a collection and click the **Collection** tab to view its [schema](/concepts/#schemas) and collection key.

   You can **rename** or [**redact**](/features/redaction) fields in the schema table.
   If you don't want to keep all the discovered fields, you'll have the option to select or remove fields when you materialize the collections.

8. If you made any changes to output collections, click **Next** again.

8. Once you're satisfied with the configuration, click **Save and Publish**. You'll see a notification when the capture publishes successfully.

9. Click **Materialize** to continue.

## Create a materialization

Now that you've captured data into one or more collections, you can materialize it to a destination.

1. Find the tile for your desired data destination and click **Materialization**.

   The page populates with the properties required for that connector.
   More details on each connector are provided in the [connectors reference](../reference/Connectors/materialization-connectors/README.md).

2. If your destination supports the concept of a schema, you will be prompted to choose a [default naming convention](/concepts/materialization/#target-resource-naming-conventions) for your materialization.

   This lets you define rules about how table and schema names should appear in your destination.

3. Choose a unique name for your materialization like you did when naming your capture; for example, `acmeCo/myFirstMaterialization`.

4. Fill out the required properties in the **Endpoint Configuration**.

5. Click **Next**.

   Estuary initiates a connection with the destination system.

   The Endpoint Config has collapsed and the **Source Collections** browser is now prominent.
   It shows each collection you captured previously.
   All of them will be mapped to a **resource** in the destination.
   Again, these may be tables, data streams, or something else.
   When you publish the Data Flow, Estuary will create these new resources in the destination.

   Now's your chance to make changes to the collections before you materialize them.

6. Optionally remove some collections or add additional collections.

   * Type in the **Search Collections** box to find a collection.

   * To remove a collection, click the **x** in its table row. You can also click the **Remove All** button.

7. Optionally modify fields to materialize from the binding's **Resource Configuration**.

   [Field selection](/guides/customize-materialization-fields/#field-selection-for-materializations) lets you require or exclude fields,
   select a default field depth to materialize, and choose a group-by key independent of the collection key.

8. Click **Save and publish**. You'll see a notification when the full Data Flow publishes successfully.

## What's next?

Now that you've deployed your first Data Flow, you can explore more possibilities.

* Read the [high level concepts](../concepts/README.md) to better understand how Estuary works and what's possible.

* Create more complex Data Flows by mixing and matching collections in your captures and materializations. For example:

   * Materialize the same collection to multiple destinations.

   * If a capture produces multiple collections, materialize each one to a different destination.

   * Materialize collections that came from different sources to the same destination.

   * Advanced users can modify collection [schemas](../concepts/schemas.md), apply data [reductions](../concepts/schemas.md#reductions),
   or transform data with a [derivation](../concepts/derivations.md)
