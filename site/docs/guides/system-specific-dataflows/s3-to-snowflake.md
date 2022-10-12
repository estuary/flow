
# Amazon S3 to Snowflake

This guide walks you through the process of creating an
end-to-end real-time Data Flow from Amazon S3 to Snowflake using Estuary Flow.

## Prerequisites

You'll need:

* (Recommended) understanding of the [basic Flow concepts](../../concepts/README.md#essential-concepts).

* Access to the [**Flow web application**](http://dashboard.estuary.dev) through an Estuary account.

:::info Beta
Flow is in private beta. Sign up for a free [discovery call](https://go.estuary.dev/sign-up)
or email support@estuary.dev for your free account.
:::

* An **S3 bucket** that contains the data you'd like to move to Snowflake.

  * For public buckets, verify that the [access policy](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-overview.html#access-control-resources-manage-permissions-basics) allows anonymous reads.

  * For buckets accessed by a user account, you'll need the AWS **access key** and **secret access key** for the user.
  See the [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding these credentials.

* A Snowflake account with:

  * A target **database**, **schema**, and virtual **warehouse**; and a **user** with a **role** assigned that grants the appropriate access levels to these resources.
  [You can use a script to quickly create all of these items.](../../reference/Connectors/materialization-connectors/Snowflake.md#setup) Have these details on hand for setup with Flow.

  * The account identifier and host URL noted. The URL is formatted using the [Snowflake account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used). For example, you might have the account identifier `orgname-accountname.snowflakecomputing.com`.

## Introduction

In Estuary Flow, you create **Data Flows** to transfer data from **source** systems to **destination** systems in real time.
In this use case, your source is an Amazon S3 bucket and your destination is a Snowflake data warehouse.

After following this guide, you'll have a Data Flow that comprises:

* A **capture**, which ingests data from S3
* A **collection**, a cloud-backed copy of that data in the Flow system
* A **materialization**, which pushes the data to Snowflake

The capture and materialization rely on plug-in components called **connectors**.
We'll walk through how to configure the [S3](../../reference/Connectors/capture-connectors/amazon-s3.md) and [Snowflake](../../reference/Connectors/materialization-connectors/Snowflake.md) connectors to integrate these systems with Flow.

## Capture from S3

You'll first create a capture to connect to your S3 bucket, which will yield one or more Flow collections.

1. Go to the Flow web application at [dashboard.estuary.dev](https://dashboard.estuary.dev/) and sign in using the
credentials provided by your Estuary account manager.

2. Click the **Captures** tab and choose **New Capture**.

3. Click the **Amazon S3** tile.

  A form appears with the properties required for an S3 capture.

4. Type a name for your capture.

    Your capture name must begin with a [prefix](../../concepts/catalogs.md#namespace) to which you [have access](../../reference/authentication.md).

    Click inside the **Name** field to generate a drop-down menu of available prefixes, and select your prefix.
    Append a unique capture name after the `/` to create the full name, for example `acmeCo/myS3Capture`.

5. Fill out the required properties for S3.

   * [**AWS Access Key ID** and **AWS Secret Access Key**](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/): Required for private buckets.

   * **AWS Region** and **Bucket**: These are listed in your [S3 console](https://s3.console.aws.amazon.com/s3/buckets).

   * **Prefix**: You might organize your S3 bucket using [prefixes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html), which emulate a directory structure. To capture *only* from a specific prefix, add it here.

   * **Match Keys**: Filters to apply to the objects in the S3 bucket. If provided, only data whose absolute path matches the filter will be captured. For example, `*\.json` will only capture JSON file.

   See the S3 connector documentation for information on [advanced fields](../../reference/Connectors/capture-connectors/amazon-s3.md#endpoint) and [parser settings](../../reference/Connectors/capture-connectors/amazon-s3.md#advanced-parsing-cloud-storage-data). (You're unlikely to need these for most use cases.)

6. Click **Discover Endpoint**.

  Flow uses the provided configuration to initiate a connection to S3. It generates a capture specification and details of the collection that it will create, once published.

  You'll be notified if there's an error. In that case, fix the configuration form or your S3 bucket setup as needed and click **Discover Endpoint** to try again.

  :::tip
  If you'd rather work on the specification files in their native YAML format, you can use the [flowctl](../../concepts/flowctl.md) CLI. flowctl provides a developer-focused path to build full Data Flows in your preferred development environment.

  flowctl also offers access to advanced features — with S3, for instance, you can [map multiple prefixes to different collections within a single capture](../../reference/Connectors/capture-connectors/amazon-s3.md#configuration).
  :::

7. Click **Save and publish**.

  You'll see a notification when the capture publishes successfully.

  The data currently in your S3 bucket has been captured, and future updates to it will be captured continuously.

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

4. Scroll down to view the **Collection Selector** and fill in the **Table** field.

   The collection you just created is already selected, but you must provide a name for the table to which it'll be materialized in Snowflake.

5. Choose whether to [enable delta updates](../../reference/Connectors/materialization-connectors/Snowflake.md#delta-updates).

6. Click **Discover endpoint**.

  Flow uses the provided configuration to initiate a connection to Snowflake. It generates a materialization specification.

  You'll be notified if there's an error. In that case, fix the configuration form or Snowflake setup as needed and click **Discover Endpoint** to try again.

7. Click **Save and Publish**. You'll see a notification when the full Data Flow publishes successfully.

## What's next?

Your Data Flow has been deployed, and will run continuously until it's stopped. Updates in your S3 bucket will be reflected in your Snowflake table as they occur.

You can advance your Data Flow by adding a **derivation**. Derivations are real-time data transformations.
See the [guide to create a derivation](../create-derivation.md).