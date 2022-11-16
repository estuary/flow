# Google Firestore to Snowflake

This guide walks you through the process of creating an
end-to-end real-time Data Flow from Google Firestore to Snowflake using Estuary Flow.

## Prerequisites

You'll need:

* (Recommended) understanding of the [basic Flow concepts](../../concepts/README.md#essential-concepts).

* Access to the [**Flow web application**](http://dashboard.estuary.dev) through an Estuary account.

:::info Beta
Flow is in private beta. Sign up for a free [discovery call](https://go.estuary.dev/sign-up)
or email support@estuary.dev for your free account.
:::

* A **Firestore database** that contains the data you'd like to move to Snowflake. You [create this as part of a Google Firebase project](https://cloud.google.com/firestore/docs/create-database-web-mobile-client-library).

* A Google service account with:

    * Read access to your Firestore database, via [roles/datastore.viewer](https://cloud.google.com/datastore/docs/access/iam).
    You can assign this role when you [create the service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating), or [add it to an existing service account](https://cloud.google.com/iam/docs/granting-changing-revoking-access#single-role).

    * A generated [JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) for the account.

* A Snowflake account with:

  * A target **database**, **schema**, and virtual **warehouse**; and a **user** with a **role** assigned that grants the appropriate access levels to these resources.
  [You can use a script to quickly create all of these items.](../../reference/Connectors/materialization-connectors/Snowflake.md#setup) Have these details on hand for setup with Flow.

  * The account identifier and host URL noted. [The URL is formatted using the account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used). For example, you might have the account identifier `orgname-accountname.snowflakecomputing.com`.

## Introduction

In Estuary Flow, you create **Data Flows** to transfer data from **source** systems to **destination** systems in real time.
In this use case, your source is an Google Firestore NoSQL database and your destination is a Snowflake data warehouse.

After following this guide, you'll have a Data Flow that comprises:

* A **capture**, which ingests data from Firestore
* Several **collection**, cloud-backed copies of [Firestore collections](https://cloud.google.com/firestore/docs/data-model) in the Flow system
* A **materialization**, which pushes the collections to Snowflake

The capture and materialization rely on plug-in components called **connectors**.
We'll walk through how to configure the [Firestore](../../reference/Connectors/capture-connectors/google-firestore.md) and [Snowflake](../../reference/Connectors/materialization-connectors/Snowflake.md) connectors to integrate these systems with Flow.

## Capture from Firestore

You'll first create a capture to connect to your Firestore database, which will yield one Flow collection for each [Firestore collection](https://cloud.google.com/firestore/docs/data-model) in your database.

1. Go to the Flow web application at [dashboard.estuary.dev](https://dashboard.estuary.dev/) and sign in using the
credentials provided by your Estuary account manager.

2. Click the **Captures** tab and choose **New Capture**.

3. Click the **Google Firestore** tile.

  A form appears with the properties required for a Firestore capture.

4. Type a name for your capture.

    Your capture name must begin with a [prefix](../../concepts/catalogs.md#namespace) to which you [have access](../../reference/authentication.md).

    Click inside the **Name** field to generate a drop-down menu of available prefixes, and select your prefix.
    Append a unique capture name after the `/` to create the full name, for example, `acmeCo/myFirestoreCapture`.

5. Fill out the required properties for Firestore.

   * **Database**: Flow can autodetect the database name, but you may optionally specify it here. This is helpful if the service account used has access to multiple Firebase projects. Your database name usually follows the format `projects/$PROJECTID/databases/(default)`.

   * **Credentials**: The JSON service account key created per the [prerequisites](#prerequisites).

6. Click **Discover Endpoint**.

  Flow uses the provided configuration to initiate a connection with Firestore.

  It maps each available Firestore collection to a possible Flow collection. It also generates a capture specification and schemas for each collection.

  You can use the **Collection Selector** to remove or modify collections, or edit the JSON in the **Specification Editor** directly.

  :::tip
  If you make any changes in the collection editor, click **Discover endpoint** again.
  :::

  :::tip
  If you'd rather work on the specification files in their native YAML format, you can use the [flowctl](../../concepts/flowctl.md) CLI. flowctl provides a developer-focused path to build full Data Flows in your preferred development environment.
  :::

7. Once you're satisfied with the collections to be captured, click **Save and publish**.

  You'll see a notification when the capture publishes successfully.

  The data currently in your Firestore database has been captured, and future updates to it will be captured continuously.

8. Click **Materialize Collections** to continue.

## Materialize to Snowflake

Next, you'll add a Snowflake materialization to connect the captured data to its destination: your data warehouse.

1. On the **Create Materialization** page, search for and select the **Snowflake** tile.

  A form appears with the properties required for a Snowflake materialization.

2.  Choose a unique name for your materialization like you did when naming your capture; for example, `acmeCo/mySnowflakeMaterialization`.

3. Fill out the required properties for Snowflake (you should have most of these handy from the [prerequisites](#prerequisites)).

   * **Host URL**
   * **Account**
   * **User**
   * **Password**
   * **Database**
   * **Schema**
   * **Warehouse**: optional
   * **Role**: optional

4. Scroll down to view the **Collection Selector** and fill in the **Table** field for each collection.

   The collections you just created have already been selected, but you must provide names for the tables to which they'll be materialized in Snowflake.

5. For each table, choose whether to [enable delta updates](../../reference/Connectors/materialization-connectors/Snowflake.md#delta-updates).

6. Click **Discover endpoint**.

  Flow uses the provided configuration to initiate a connection to Snowflake. It generates a materialization specification.

  You'll be notified if there's an error. In that case, fix the configuration form or Snowflake setup as needed and click **Discover Endpoint** to try again.

7. Click **Save and Publish**. You'll see a notification when the full Data Flow publishes successfully.

## What's next?

Your Data Flow has been deployed, and will run continuously until it's stopped. Updates in your Firestore database will be reflected in your Snowflake table as they occur.

You can advance your Data Flow by adding a **derivation**. Derivations are real-time data transformations.
See the [guide to create a derivation](../create-derivation.md).