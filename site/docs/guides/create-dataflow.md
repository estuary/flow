---
sidebar_position: 1
---
# Create a simple data flow

This guide walks you through the process of creating an end-to-end data flow in the
Flow web application.

:::info Beta
The Flow web application is currently available to users in the Estuary [beta program](https://go.estuary.dev/sign-up).
:::

## Prerequisites
This guide assumes a basic understanding of Flow and its key concepts.
Before you begin, it's recommended that you read
the [high level concepts](../concepts/README.md) documentation.

## Introduction

The simplest Flow **catalog** comprises three types of entities that define your data flow: a data **capture** from an external source, one or more **collections**, which store that data in a cloud-backed data lake, and a **materialization**, to push them to an external destination.

In the majority of cases, the capture and materialization each rely on a plug-in **connector**. Here, we'll walk through how to leverage various connectors, configure them, and deploy your catalog to create an active data flow.

## Create a capture

You'll first create a **capture** to connect to your data source system.
This process will create one or more **collections** in Flow, which you can then materialize to another system.

1. Go to the Flow web application at [dashboard.estuary.dev](https://dashboard.estuary.dev/) and sign in using the
credentials provided by your Estuary account manager.

2. Click the **Captures** tab and choose **New capture**.

3. On the **Create Captures** page, choose a name for your capture.
Your capture name must begin with your organization's globally unique tenant [prefix](../concepts/README.md#namespace).
Click inside the **Name** field to generate a drop-down menu of available prefixes, and select your prefix.
Append a unique capture name after the `/` to create the full name, for example `acmeCo/myFirstCapture`.

4. Use the **Connector** drop down to choose your desired data source.

  The rest of the page populates with the properties required for that connector.
  More details are on each connector are provided in the [connectors reference](../reference/Connectors/capture-connectors/README.md).

5. Fill out the required properties and click **Test Config**.

  Flow uses the provided information to initiate a connection to the source system.
  It identifies one or more data **resources** — these may be tables, data streams, or something else, depending on the connector. If there's an error, you'll be prompted to fix your configuration and test again.

6. Look over the generated capture definition and the schema of the resulting Flow **collection**.
If you'd like, you can edit the YAML files directly.

7. Once you're satisfied with the configuration, click **Save and publish**. You'll see a notification when the capture publishes successfully.

8. Click **Materialize collections** to continue.

## Create a materialization

Now that you've captured data into one or more collections, you can materialize it to a destination.

The **New Materializations** page is pre-populated with the capture and collection you just created.

1.  Choose a unique name for your materialization like you did when naming your capture; for example, `acmeCo/myFirstMaterialization`.

2. Use the **Connector** drop down to choose your desired data destination.

  The rest of the page populates with the properties required for that connector.
  More details are on each connector are provided in the [connectors reference](../reference/Connectors/materialization-connectors/README.md).

3. Fill out the required properties and click **Regenerate catalog**.

  Flow initiates a connection with the destination system, and creates a binding to map each collection in your catalog to a **resource** in the destination.
  Again, these may be tables, data streams, or something else.
  When you publish the complete catalog, Flow will create these new resources in the destination.

4. Look over the generated materialization definition and edit it, if you'd like.

5. Click **Save and publish**. You'll see a notification when the full data flow publishes successfully.
