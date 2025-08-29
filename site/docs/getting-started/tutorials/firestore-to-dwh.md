---
slug: /guides/system-specific-dataflows/firestore-to-dwh/
---

# Google Cloud Firestore to Snowflake

This guide walks you through the process of creating an
end-to-end real-time Data Flow from Google Cloud Firestore to Snowflake using Estuary Flow.

## Prerequisites

You'll need:

* (Recommended) understanding of the [basic Flow concepts](/concepts/#essential-concepts).

* Access to the [**Flow web application**](http://dashboard.estuary.dev) through an Estuary account.
If you don't have one, visit the web app to register for free.

* A **Firestore database** that contains the data you'd like to move to Snowflake. You [create this as part of a Google Firebase project](https://cloud.google.com/firestore/docs/create-database-web-mobile-client-library).

* A Google service account with:

    * Read access to your Firestore database, via [roles/datastore.viewer](https://cloud.google.com/datastore/docs/access/iam).
    You can assign this role when you [create the service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating), or [add it to an existing service account](https://cloud.google.com/iam/docs/granting-changing-revoking-access#single-role).

    * A generated [JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) for the account.

* A Snowflake account with:

  * A target **database**, **schema**, and virtual **warehouse**; and a **user** with a **role** assigned that grants the appropriate access levels to these resources.
  [You can use a script to quickly create all of these items.](/reference/Connectors/materialization-connectors/Snowflake/#setup) Have these details on hand for setup with Flow.

  * The account identifier and host URL noted. [The URL is formatted using the account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used). For example, you might have the account identifier `orgname-accountname.snowflakecomputing.com`.

## Introduction

In Estuary Flow, you create **Data Flows** to transfer data from **source** systems to **destination** systems in real time.
In this use case, your source is a Google Cloud Firestore NoSQL database and your destination is a Snowflake data warehouse.

After following this guide, you'll have a Data Flow that comprises:

* A **capture**, which ingests data from Firestore
* Several **collections**, cloud-backed copies of [Firestore collections](https://cloud.google.com/firestore/docs/data-model) in the Flow system
* A **materialization**, which pushes the collections to Snowflake

The capture and materialization rely on plug-in components called **connectors**.
We'll walk through how to configure the [Firestore](/reference/Connectors/capture-connectors/google-firestore) and [Snowflake](/reference/Connectors/materialization-connectors/Snowflake) connectors to integrate these systems with Flow.

## Capture from Firestore

You'll first create a capture to connect to your Firestore database, which will yield one Flow collection for each [Firestore collection](https://cloud.google.com/firestore/docs/data-model) in your database.

1. Go to the Flow web application at [dashboard.estuary.dev](https://dashboard.estuary.dev/) and sign in using the
credentials provided by your Estuary account manager.

2. Click the **Sources** tab and choose **New Capture**.

3. Find the **Google Firestore** tile and click **Capture**.

   A form appears with the properties required for a Firestore capture.

4. Type a name for your capture.

    Your capture name must begin with a [prefix](/concepts/catalogs/#namespace) to which you [have access](/reference/authentication).

    In the **Name** field, use the drop-down to select your prefix.
    Append a unique capture name after the `/` to create the full name, for example, `acmeCo/myFirestoreCapture`.

5. Fill out the required properties for Firestore.

   * **Database**: Flow can autodetect the database name, but you may optionally specify it here. This is helpful if the service account used has access to multiple Firebase projects. Your database name usually follows the format `projects/$PROJECTID/databases/(default)`.

   * **Credentials**: The JSON service account key created per the [prerequisites](#prerequisites).

6. Click **Next**.

   Flow uses the provided configuration to initiate a connection with Firestore.

   It maps each available Firestore collection to a possible Flow collection. It also generates minimal schemas for each collection.

   You can use the **Source Collections** browser to remove or modify collections. You'll have the chance to tighten up each collection's JSON schema later, when you materialize to Snowflake.

  :::tip
  If you make any changes to collections, click **Next** again.
  :::

7. Once you're satisfied with the collections to be captured, click **Save and Publish**.

   You'll see a notification when the capture publishes successfully.

   The data currently in your Firestore database has been captured, and future updates to it will be captured continuously.

8. Click **Materialize Collections** to continue.

## Materialize to Snowflake

Next, you'll add a Snowflake materialization to connect the captured data to its destination: your data warehouse.

1. Locate the **Snowflake** tile and click **Materialization**.

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

4. Click **Next**.

   Flow uses the provided configuration to initiate a connection to Snowflake.

   You'll be notified if there's an error. In that case, fix the configuration form or Snowflake setup as needed and click **Next** to try again.

   Once the connection is successful, the Endpoint Config collapses and the **Source Collections** browser becomes prominent.
   It shows the collections you captured previously.
   Each of them will be mapped to a Snowflake table.

5. In the **Source Collections** browser, optionally change the name in the **Table** field for each collection.

   These will be the names of the output tables in Snowflake.

6. For each table, choose whether to [enable delta updates](/reference/Connectors/materialization-connectors/Snowflake/#delta-updates).

7. For each collection, apply a stricter schema to be used for the materialization.

   Firestore has a flat data structure.
   To materialize data effectively from Firestore to Snowflake, you should apply a schema that can translate to a table structure.
   Flow's **Schema Inference** tool can help.

   1. In the Source Collections browser, choose a collection and click its **Collection** tab.

   2. Click **Schema Inference**

      The Schema Inference window appears. Flow scans the data in your collection and infers a new schema, called the `readSchema`, to use for the materialization.

   3. Review the new schema and click **Apply Inferred Schema**.

8. Click **Next** to apply the changes you made to collections.

9. Click **Save and Publish**. You'll see a notification when the full Data Flow publishes successfully.

## What's next?

Your Data Flow has been deployed, and will run continuously until it's stopped. Updates in your Firestore database will be reflected in your Snowflake table as they occur.

You can advance your Data Flow by adding a **derivation**. Derivations are real-time data transformations.
See the [guide to create a derivation](/guides/flowctl/create-derivation).
