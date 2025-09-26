---
sidebar_position: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Create a Real-Time Materialized View in PostgreSQL

PostgreSQL supports *materialized views*: database objects that contain the result of a query, usually a focused subset of a large dataset.

In this tutorial, you'll use Flow and your Postgres instance to create something that's not possible in Postgres alone:
a materialized view that updates continuously based on a real-time data feed.

## Prerequisites

* An Estuary Flow account. If you don't have one, visit the [Flow web app](https://dashboard.estuary.dev) to register for free.

* `flowctl` [installed and authenticated](/guides/get-started-with-flowctl)

* A Postgres database set up to [allow connections from Flow](/reference/Connectors/materialization-connectors/PostgreSQL/#setup).
Amazon RDS, Amazon Aurora, Google Cloud SQL, Azure Database for PostgreSQL, and self-hosted databases are supported.

## Introduction

Materialized views in Postgres give you a powerful way to narrow down a huge dataset into a compact one that you can easily monitor.
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

## Loading the Wikipedia Demo

1. Navigate to the [Live Demo](https://dashboard.estuary.dev/welcome?activeTab=demo) page and click on `See the capture`.

2. After accepting the pop up, Estuary will populate your Sources, Collections and Destinations with the Wikipedia Demo tasks.

## Check out the source data

1. Got the the [collections page](https://dashboard.estuary.dev/collections) of the Flow web app.

2. Search for `demo/wikipedia/recentchange` and click on its name.

3. On the **Collection Details** page, click the **Spec** tab.

   The collection schema has many fields. Because Wikipedia sees a lot of edits,
   this would yield a large, unwieldy table in Postgres.

   :::info Tip
   To save on performance, you can also perform this tutorial using the smaller `demo/wikipedia/recentchange-sampled` collection. Apart from the collection name, all other steps are the same.
   :::

   *Learn more about [Flow collections](../../concepts/collections.md) and [schemas](../../concepts/schemas.md).*

   Now you'll create the derivation. A derivation is a new collection that's defined by a transformation.
   First, you'll define the collection's schema. Then, you'll write the transformation to shape the data to that schema.

## Add a derivation to transform data

You'll write your derivation with the help of `flowctl`, Estuary's CLI.

1. Open a terminal and pull the demo collection spec locally.

   ```bash
   flowctl catalog pull-specs --name demo/wikipedia/recentchange
   ```

2. This command will write a new file structure in your current directory.
Each slash-delimited prefix of your derivation name has become a folder.
Open the nested folders to find the deepest-nested `flow.yaml` file, in the `demo/wikipedia/` directory.

3. Open this `flow.yaml` file in a code editor. It should contain a collection spec similar to:

   ```yaml
   ---
   collections:
     demo/wikipedia/recentchange:
       writeSchema: recentchange.write.schema.yaml
       readSchema: recentchange.read.schema.yaml
       key:
         - /_meta/file
         - /_meta/offset
       projections:
         flow_document: ""
       expectPubId: 1234ff4b568003a5
   ```

4. The derivation will be a new collection in this file. Add the derivation specification, using your own prefix for the collection name:

   ```yaml
   yourprefix/wikipedia/user-fact-table:
     schema:
       properties:
         edits_this_day:
           reduce:
             strategy: sum
           type: integer
         date:
           format: date
           type: string
         user:
           type: string
       reduce:
         strategy: merge
       required:
         - user
         - date
         - edits_this_day
       type: object
     key:
       - /user
       - /date
     derive:
       using:
         sqlite: {}
       transforms:
         - name: dailychangesbyuser
           source: demo/wikipedia/recentchange
           shuffle: { key: [ /user ] }
           lambda: user-fact-table.lambda.recentchange.sql
   ```

   Let's walk through some key aspects of this specification.

   The new schema contains [reduction annotations](/concepts/schemas/#reduce-annotations).
   These sum the changes made by a given user on a given date.
   The collection is now keyed on each unique combination of user ID and date.
   It has just three fields:
   the user, date, and the number of changes made by that user on that date.

   We also specify that we're _deriving_ the new collection using a SQL transformation.
   You may notice that we reference files that don't exist yet. We'll fix this next.

   In the `transforms` stanza, we specify that we want to [shuffle](/concepts/derivations/#shuffles) on the `user` key.
   Since we're working with a large dataset, this ensures that each user is processed by the same task **shard**.
   This way, you'll prevent Flow from creating multiple counts for a given user and date combination.

5. With the derivation specification in place, we need to generate the associated SQL transformation files.
This will hold the function that will shape the source data to fit the new schema.

   You can create a stub file to get started with `flowctl`.

   ```bash
   flowctl generate --source flow.yaml
   ```

6. Open the new file called `user-fact-table.lambda.recentchange.sql`.

7. Replace its contents with

    ```sql
    select $user, 1 as edits_this_day, date($meta$dt) as date where $user is not null;
    ```

    This creates the `edits_this_day` field we referenced earlier, and starts the counter at 1.
    It converts the timestamp into a simplified date format.
    Finally, it filters out `null` users (which occasionally occur in the Wikipedia data stream and would violate your schema).

8. All pieces of the derivation are in place. Double check your files against these samples:

<Tabs>
<TabItem value="flow.yaml" default>

```yaml file=./samples/continuous-materialized-view/flow.yaml
```

</TabItem>
<TabItem value="user-fact-table.lambda.recentchange.sql" default>

```sql file=./samples/continuous-materialized-view/user-fact-table.lambda.recentchange.sql
```

</TabItem>
</Tabs>

9. Run the derivation locally and preview its output:

   ```console
   flowctl preview --source flow.yaml
   ```

   In your terminal, you'll see JSON documents that look like:

   ```json
   {"date":"2023-07-18","edits_this_day":3,"user":"WMrapids"}
   ```

   This looks right: it includes the correctly formatted date, the number of edits, and the username.
   You're ready to publish.

10. Stop the local derivation with **Ctrl-C**.

11. Publish the derivation:

  ```console
  flowctl catalog publish --source flow.yaml
  ```

The message `Publish successful` means you're all set.
Your transformation will continue in real time based on the raw dataset, which is also updating in real time.

## Create the continuous materialized view

Now, you'll materialize your new fact table to Postgres. You'll also materialize the source dataset to compare performance.

1. Go to the [Destinations page](https://dashboard.estuary.dev/materializations) in the Flow web app.

2. Click **New Materialization**.

3. Find the **PostgreSQL** and click **Materialization**.

3. Add a unique name for the materialization, for example, `yourprefix/yourname-materialized-views-demo`.

4. Fill out the **Basic Config** with:

   1. A username and password for the Postgres instance.

   2. Your database host and port.

   3. The database name (if in doubt, use the default, `postgres`).

   See the [connector documentation](https://docs.estuary.dev/reference/Connectors/materialization-connectors/PostgreSQL/) if you need help finding these properties.

5. In the **Source Collections** browser, search for and add the collection `demo/wikipedia/recentchange` and name the corresponding Postgres Table `wikipedia_raw`.

6. Also search for and add the collection you just derived, (for example, `yourprefix/wikipedia/user-fact-table`).
Name the corresponding Postgres table `wikipedia_data_by_user`.

7. Click **Next** to test the connection.

8. Click **Save and Publish**.

## Explore the results

In your Postgres client of choice, note the size of each table and how they quickly change.
Try running some basic queries against both and compare performance.
See the [blog post](https://estuary.dev/how-to-create-a-real-time-materialized-view-in-postgresql/#step-3-compare-performance/) for ideas.

**Once you're satisfied, and to prevent continual resource use, disable or delete your materialization from the
[Destinations page](https://dashboard.estuary.dev/materializations).**

## Resources

[About derivations](../../concepts/derivations.md)