---
sidebar_position: 1
---
# Create a basic Data Flow

This guide walks you through the process of creating an end-to-end Data Flow in the
Flow web application.

:::info Beta
The Flow web application is currently available to users in the Estuary beta program. Sign up for a free [discovery call](https://go.estuary.dev/sign-up)
or email support@estuary.dev for your free account.
:::

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

5. Fill out the required properties and click **Discover Endpoint**.

  Flow uses the provided information to initiate a connection to the source system.
  It identifies one or more data **resources** — these may be tables, data streams, or something else, depending on the connector. Each resource is mapped to a collection through a **binding**.

  If there's an error, you'll be prompted to fix and test your configuration.

6. Look over the generated capture definition and the schema of the resulting Flow **collection(s)**.

  Flow generates these specifications as YAML files.
  You can modify it by filling in new values in the form and clicking **Discover Endpoint**,
  or by editing the YAML files directly in the web application.
  (Those who prefer a [command-line interface](../concepts/flowctl.md) can manage and edit YAML in their preferred development environment).

  It's not always necessary to review and edit the YAML — Flow will prevent the publication of invalid specifications.

7. Once you're satisfied with the configuration, click **Save and publish**. You'll see a notification when the capture publishes successfully.

8. Click **Materialize collections** to continue.

## Create a materialization

Now that you've captured data into one or more collections, you can materialize it to a destination.


1. Select the **Connector** tile for your desired data destination.

  The page populates with the properties required for that connector.
  More details are on each connector are provided in the [connectors reference](../reference/Connectors/materialization-connectors/README.md).

  Details of the collection you just created are already filled in.

2.  Choose a unique name for your materialization like you did when naming your capture; for example, `acmeCo/myFirstMaterialization`.

3. Fill out the required properties and click **Discover Endpoint**.

  Flow initiates a connection with the destination system, and creates a binding to map each collection from your capture to a **resource** in the destination.
  Again, these may be tables, data streams, or something else.
  When you publish the Data Flow, Flow will create these new resources in the destination.

4. Look over the generated materialization definition and edit it, if you'd like.

5. Click **Save and publish**. You'll see a notification when the full Data Flow publishes successfully.

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
