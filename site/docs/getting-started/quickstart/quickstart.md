# Quickstart

<head>
    <meta property="og:image" content="https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//architecture_6bbaf2c5a6/architecture_6bbaf2c5a6.png" />
</head>

In this tutorial, we'll set up a streaming CDC pipeline from PostgreSQL to Snowflake using Estuary Flow.

Before you get started, make sure you do two things.

1. Sign up for Estuary Flow [here](https://dashboard.estuary.dev/register). It’s simple, fast and free.

2. Make sure you also join
   the [Estuary Slack Community](https://estuary-dev.slack.com/ssb/redirect#/shared-invite/email). Don’t struggle. Just
   ask a question.

When you register for Flow, your account will use Flow's secure cloud storage bucket to store your data.
Data in Flow's cloud storage bucket is deleted 30 days after collection.

For production use cases, you
should [configure your own cloud storage bucket to use with Flow](#configuring-your-cloud-storage-bucket-for-use-with-flow).

## Step 1. Set up a Capture<a id="step-2-set-up-a-capture"></a>

Head over to your Flow dashboard (if you haven’t registered yet, you can do
so [here](https://dashboard.estuary.dev/register).) and create a new **Capture.** A capture is how Flow ingests data
from an external source.

Go to the sources page by clicking on the **Sources** on the left hand side of your screen, then click on **+ New
Capture**

![Add new Capture](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//new_capture_4583a8a120/new_capture_4583a8a120.png)

Configure the connection to the database and press **Next.**

![Configure Capture](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//capture_configuration_89e2133f83/capture_configuration_89e2133f83.png)

On the following page, we can configure how our incoming data should be represented in Flow as collections. As a quick
refresher, let’s recap how Flow represents data on a high level.

**Documents**

The documents of your flows are stored in collections: real-time data lakes of JSON documents in cloud storage.
Documents being backed by an object storage mean that once you start capturing data, you won’t have to worry about it
not being available to replay – object stores such as S3 can be configured to cheaply store data forever.
See [docs page](https://docs.estuary.dev/concepts/collections/#documents) for more information.

**Schemas**

Flow documents and collections always have an associated schema that defines the structure, representation, and
constraints of your documents. In most cases, Flow generates a functioning schema on your behalf during the discovery
phase of capture, which has already automatically happened - that’s why you’re able to take a peek into the structure of
the incoming data!

To see how Flow parsed the incoming records, click on the Collection tab and verify the inferred schema looks correct.

![Configure Collections](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//collections_configuration_34e53025c7/collections_configuration_34e53025c7.png)

## Step 3. Set up a Materialization<a id="step-3-set-up-a-materialization"></a>

Similarly to the source side, we’ll need to set up some initial configuration in Snowflake to allow Flow to materialize
collections into a table.

Head over to the **Destinations** page, where you
can [create a new Materialization](https://dashboard.estuary.dev/materializations/create).

![Add new Materialization](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//new_materialization_31df04d81f/new_materialization_31df04d81f.png)

Choose Snowflake and start filling out the connection details based on the values inside the script you executed in the
previous step. If you haven’t changed anything, this is how the connector configuration should look like:

![Configure Materialization endpoint](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//materialization_endpoint_configuration_0d540a12b5/materialization_endpoint_configuration_0d540a12b5.png)

You can grab your Snowflake host URL and account identifier by navigating to these two little buttons on the Snowflake
UI.

![Grab your Snowflake account id](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//snowflake_account_id_af1cc78df8/snowflake_account_id_af1cc78df8.png)

After the connection details are in place, the next step is to link the capture we just created to Flow is able to see
collections we are loading data into from Postgres.

You can achieve this by clicking on the “Source from Capture” button, and selecting the name of the capture from the
table.

![Link Capture](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//link_source_to_capture_b0d37a738f/link_source_to_capture_b0d37a738f.png)

After pressing continue, you are met with a few configuration options, but for now, feel free to press **Next,** then *
*Save and Publish** in the top right corner, the defaults will work perfectly fine for this tutorial.

A successful deployment will look something like this:

![Successful Deployment screen](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//publish_successful_4e18642288/publish_successful_4e18642288.png)

And that’s it, you’ve successfully published a real-time CDC pipeline. Let’s check out Snowflake to see how
the data looks.

![Results in Snowflake](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//snowflake_verification_2eb047efec/snowflake_verification_2eb047efec.png)

Looks like the data is arriving as expected, and the schema of the table is properly configured by the connector based
on the types of the original table in Postgres.

To get a feel for how the data flow works; head over to the collection details page on the Flow web UI to see your
changes immediately. On the Snowflake end, they will be materialized after the next update.

## Next Steps<a id="next-steps"></a>

That’s it! You should have everything you need to know to create your own data pipeline for loading data into Snowflake!

Now try it out on your own PostgreSQL database or other sources.

If you want to learn more, make sure you read through the [Estuary documentation](https://docs.estuary.dev/).

You’ll find instructions on how to use other connectors [here](https://docs.estuary.dev/). There are more
tutorials [here](https://docs.estuary.dev/guides/).

Also, don’t forget to join
the [Estuary Slack Community](https://estuary-dev.slack.com/ssb/redirect#/shared-invite/email)!
