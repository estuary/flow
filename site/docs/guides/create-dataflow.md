---
sidebar_position: 1
---
# Create a basic Data Flow

This guide walks you through the process of creating an end-to-end Data Flow in the
Flow web application.

## Prerequisites

This guide is intended for new Flow users and briefly introduces Flow's key concepts.
Though it's not required, you may find it helpful to read
the [high level concepts](../concepts/README.md#essential-concepts) documentation for more detail before you begin.

## Introduction

In Estuary Flow, you create Data Flows to connect data **source** and **destination** systems.

The simplest Data Flow comprises three types of entities:

* A data **capture**, which ingests data from an external source
* One or more **collections**, which store that data in a cloud-backed data lake
* A **materialization**, to push the data to an external destination

Almost always, the capture and materialization each rely on a **connector**.
A connector is a plug-in component that interfaces between Flow and whatever data system you need to connect to.
Here, we'll walk through how to leverage various connectors, configure them, and deploy your Data Flow.

## Create a capture

You'll first create a **capture** to connect to your data source system.
This process will create one or more **collections** in Flow, which you can then materialize to another system.

1. Go to the Flow web application at [dashboard.estuary.dev](https://dashboard.estuary.dev/) and sign in using the
credentials provided by your Estuary account manager.

2. Click the **Captures** tab and choose **New capture**.

3. Choose the appropriate **Connector** for your desired data source.

  A form appears with the properties required for that connector.
  More details are on each connector are provided in the [connectors reference](../reference/Connectors/capture-connectors/README.md).

4. Type a name for your capture.

   Your capture name must begin with a [prefix](../concepts/catalogs.md#namespace) to which you [have access](../reference/authentication.md).

    Click inside the **Name** field to generate a drop-down menu of available prefixes, and select your prefix.
    Append a unique capture name after the `/` to create the full name, for example `acmeCo/myFirstCapture`.

5. Fill out the required properties and click **Next**.

  Flow uses the provided information to initiate a connection to the source system.
  It identifies one or more data **resources** — these may be tables, data streams, or something else, depending on the connector. These are each mapped to a **collection**.

  The **Collection Selector** appears, showing this list of available collections.
  You can decide which ones you want to capture.

6. Look over the list of available collections. All are selected by default.
You can remove collections you don't want to capture, change collection names, and for some connectors, modify other properties.

:::tip
Use a filter to narrow down a large list of available collections.
Hover your cursor within the Collection Selector table header, beneath the **Remove All** button, to reveal an expandable menu icon (three dots).
Click the menu icon, and then choose **Filter**.

**Note that the **Remove All** button will always remove all collections — even those that are hidden by a filter. Use this button with caution.**
:::

  If you're unsure which collections you want to keep or remove, you can look at their schemas.

7. Scroll down to the **Specification Editor**

  Here, you can view the generated capture definition and the schema for each collection.

  Flow displays these specifications as JSON in a read-only editor.
  For many source systems, you'll notice that the collection schemas are quite permissive.
  You'll have the option to apply more restrictive schemas later, when you materialize the collections.

  (Those who prefer a [command-line interface](../concepts/flowctl.md) can manage and edit the schema in their preferred development environment.
  Click the **CLI** button near the **Collection Specification** viewer to get started.).

8. If you made any changes in the Collection Editor, click **Next** again.

8. Once you're satisfied with the configuration, click **Save and publish**. You'll see a notification when the capture publishes successfully.

9. Click **Materialize collections** to continue.

## Create a materialization

Now that you've captured data into one or more collections, you can materialize it to a destination.

1. Select the **Connector** tile for your desired data destination.

  The page populates with the properties required for that connector.
  More details are on each connector are provided in the [connectors reference](../reference/Connectors/materialization-connectors/README.md).

2. Choose a unique name for your materialization like you did when naming your capture; for example, `acmeCo/myFirstMaterialization`.

3. Fill out the required properties in the **Endpoint Configuration**.

4. Click **Next**.

  Flow initiates a connection with the destination system.

  The Endpoint Config has collapsed and the **Collection Selector** is now prominent.
  It shows each collection you captured previously.
  All of them will be mapped to a **resource** in the destination.
  Again, these may be tables, data streams, or something else.
  When you publish the Data Flow, Flow will create these new resources in the destination.

  Now's your chance to make changes to the collections before you materialize them.

5. Optionally remove some collections or add additional collections.

   * To easily find collections, you can use a filter.
   Hover your cursor within to the Collection Selector table header, next to the **Remove All** button, to reveal an expandable menu icon (three dots).
   Click the menu icon, and then choose **Filter**.

   * To remove a collection, click the **x** in its table row. You can also click the **Remove All** button, but keep in mind that this button always removes _all_
   collections from the materialization, regardless of whether they're hidden by a filter.

6. Optionally apply a stricter schema to each collection to use for the materialization.

   Depending on the data source, you may have captured data with a fairly permissive schema.
   You can tighten up the schema so it'll materialize to your destination in the correct shape.
   (This shouldn't be necessary for database and SaaS data sources.)

   1. In the Collection Selector, choose a collection and click its **Specification** tab.

   2. Click **Schema Inference**

      The Schema Inference window appears. Flow scans the data in your collection and infers a new schema, called the `readSchema`, to use for the materialization.

   3. Review the new schema and click **Apply Inferred Schema**.

7. Click **Save and publish**. You'll see a notification when the full Data Flow publishes successfully.

## What's next?

Now that you've deployed your first Data Flow, you can explore more possibilities.

* Read the [high level concepts](../concepts/README.md) to better understand how Flow works and what's possible.

* Create more complex Data Flows by mixing and matching collections in your captures and materializations. For example:

   * Materialize the same collection to multiple destinations.

   * If a capture produces multiple collections, materialize each one to a different destination.

   * Materialize collections that came from different sources to the same destination.

* Advanced users can modify collection [schemas](../concepts/schemas.md), apply data [reductions](../concepts/schemas.md#reductions),
or transform data with a [derivation](../concepts/derivations.md)
(derivations are currently available using the [CLI](../concepts/flowctl.md),
but support in the web application is coming soon.)
