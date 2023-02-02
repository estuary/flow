---
sidebar_position: 2
---

# Create a real-time materialized view in PostgreSQL

PostgreSQL supports *materialized views*: database objects that contain the result of a query, usually a focused subset of a large dataset.

In this tutorial, you'll use Flow and your Postgres instance to create something that's not possible in Postgres alone:
a materialized view that updates continuously based on a real-time data feed.

## Prerequisites

* An Estuary Flow trial account (or a full account). If you don't have one, visit the [Flow web app](https://dashboard.estuary.dev) to register for your free trial.

* The [flowctl CLI installed](../installation.md#get-started-with-the-flow-cli) (for the optional section).

* A Postgres database for testing set up to [allow connections from Flow](../../reference/Connectors/materialization-connectors/PostgreSQL.md#setup).
Amazon RDS, Amazon Aurora, Google Cloud SQL, Azure Database for PostgreSQL, and self-hosted databases are supported.

## Introduction

Materialized views in Postgres give you a powerful way narrow down a huge dataset into a compact one that you can easily monitor.
But if your data is updating in real-time, traditional materialized views introduce latency. They're batch workflows — the query is run at a set interval.

To get around this, you'll need to perform a real-time transformation elsewhere.
Flow [derivations](../../concepts/README.md#derivations) are a great way to do this.

For this example, you'll use Estuary's public data collection of recent changes to Wikipedia,
captured from the [Wikimedia Foundation's event stream](https://www.mediawiki.org/wiki/API:Recent_changes_stream).

The raw dataset is quite large.
It captures every change to the platform — about 30 per second —  and includes various properties.
Written to a Postgres table, it quickly grows to an size that's very expensive to query.

First, you'll scope the raw data down to a small fact table with a derivation.

You'll then materialize both the raw and transformed datasets to your Postgres instance and compare performance.

## Add a derivation to transform data.

Derivations are currently available in the flowctl CLI. If you'd prefer to only work in the web app today,
you can skip to the next section. Estuary provides a pre-computed copy of the derivation that you can use to compare performance.

1. On the [CLI-API tab](https://dashboard.estuary.dev/admin/api) in the Flow web app, copy the access token.

2. Authorize Flow in your local development environment:

   ```console
   flowctl auth token --token <your-token-here>
   ```

3. Next, pull the raw wikipedia collection.

   ```console
   flowctl catalog pull-specs --name estuary/public/wikipedia/recentchange
   ```

   Source files are written to your current working directory.

4. Open Estuary > Public > Wikipedia and examine the contents of `flow.yaml` and `recentchange.schema.yaml`.

   The collection is keyed on its metadata, so every new change event is seen as unique. Its schema has many fields.
   This would yield a large, unwieldy table in Postgres.

   Learn more about [Flow collections](../../concepts/collections.md) and [schemas](../../concepts/schemas.md).

   Next, you'll add the derivation. Technically, a derivation is a new collection that contains a transformation within it.
   First, you'll define the collection. Then, you'll flesh out the transformation.

5. Create a new file called `fact-table.flow.yaml` and add a new collection called `<your-prefix>/wikipedia/user-fact-table`.

:::info tip
Your prefix is likely your organization name. You can find it in the [web app's admin tab](https://dashboard.estuary.dev/admin/accessGrants).
You must have write or admin access to create a collection in the prefix.
:::

  Copy the sample below:

   ```yaml file=./samples/continuous-materialized-view/fact-table.flow.yaml
   ```

  The new collection's schema contains [reduction annotations](../../concepts/schemas.md#reduce-annotations).
  These merge the data based on the user ID and the date they were last updated.

6. Generate a TypeScript file for the derivation's transformation function.

   ```console
   flowctl typescript generate --source flow.yaml
   ```

7. Open `user-fact-table.ts`. It contains a stubbed-out transformation.
You'll populate it with a function that counts the number of changes associated with each user on a given date
and converts the timestamp in the source data to a familiar date format.

8. Copy and paste from the below sample (beginning at line 4):

  ```typescript file=./samples/continuous-materialized-view/user-fact-table.ts
  ```

9. Publish the derivation:

  ```console
  flowctl catalog publish --source path/to/your/fact-table.flow.yaml
  ```

Your transformation will continue in real time based on the raw dataset, which is also updating in real time.

## Create the continuous materialized view

Now, you'll materialize your new fact table to Postgres. You'll also materialize the source dataset to compare performance.

1. Go to the [Materializations page](https://dashboard.estuary.dev/materializations) in the Flow web app.

2. Click **New Materialization**.

3. For Connector, choose **PostgreSQL**. Add a unique name for the materialization, for example, `yourprefix/yourname-materialized-views-demo`.

4. Fill out the **Basic Config** with:

   1. A username and password for the Postgres instance.

   2. Your database host and port.

   3. The database name (if in doubt, use the default, `postgres`).

   See the [connector documentation](https://docs.estuary.dev/reference/Connectors/materialization-connectors/PostgreSQL/) if you need help finding these properties.

5. In the **Collection Selector**, search for and add the collection `estuary/public/wikipedia/recentchange` and name the corresponding Postgres Table `wikipedia_raw`.

6. Also search for and add the collection you just derived, (for example, `yourprefix/wikipedia/user-fact-table`).
If you skipped the derivation, use the provided version, `estuary/public/wikipedia/user-fact-table`.
Name the corresponding Postgres table `wikipedia_data_by_user`.

7. Click **Next** to test the connection.

8. Click **Save and Publish**.

## Explore the results

In your Postgres client of choice, note the size of each table and how they quickly change.
Try running some basic queries against both and compare performance.
See the [blog post](https://www.estuary.dev/how-to-create-a-real-time-materialized-view-in-postgresql/) for ideas.

**Once you're satisfied, and to prevent continual resource use, disable or delete your materialization from the
[Materializations page](https://dashboard.estuary.dev/materializations).**

## Resources

[Detailed guide to create derivations.](../../guides/flowctl/create-derivation.md)
