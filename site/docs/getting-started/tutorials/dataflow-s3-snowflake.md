---
sidebar_position: 3
---
# Create your first dataflow with Amazon S3 and Snowflake

In this tutorial, you'll create your first complete **Data Flow** with Estuary Flow using publicly available data.

The dataset you'll use is composed of zipped CSV files in an Amazon S3 cloud storage bucket. You'll transport this data to a table in your own Snowflake data warehouse.

## Prerequisites

You'll need:

* An Estuary Flow account. If you don't have one, visit the [Flow web app](https://dashboard.estuary.dev) to register for free.

* A [Snowflake free trial account](https://signup.snowflake.com/) (or a full account).
  Snowflake trials are valid for 30 days.

## Introduction

#### The data

New York City hosts the United States' largest bike share program, Citi Bike. [Citi Bike shares ride data](https://ride.citibikenyc.com/system-data) in CSV format with the public, including the starting and ending times and locations for every ride.
They upload new data monthly to [their Amazon S3 bucket](https://s3.amazonaws.com/tripdata/index.html) as a zipped CSV file.

In this scenario, let's imagine you're interested in urban bike safety, or perhaps you plan to open a bike store and entice Citi Bike renters to buy their own bikes.
You'd like to access the Citi Bike data in your Snowflake data warehouse.
From there, you plan to use your data analytics platform of choice to explore the data, and perhaps integrate it with business intelligence apps.

You can use Estuary Flow to build a real-time Data Flow that will capture all the new data from Citi Bike as soon as it appears, convert it to Snowflake's format, and land the data in your warehouse.

#### Estuary Flow

In Estuary Flow, you create Data Flows to connect data **source** and **destination** systems.

The simplest Data Flow comprises three types of entities:

* A data **capture**, which ingests data from the source. In this case, you'll capture from Amazon S3.

* One or more **collections**, which Flow uses to store that data inside a cloud-backed data lake

* A **materialization**, to push the data to an external destination. In this case, you'll materialize to a Snowflake data warehouse.

import Mermaid from '@theme/Mermaid';

<Mermaid chart={`
	graph LR;
		Capture-->Collection;
        Collection-->Materialization;
`}/>


For the capture and materialization to work, they need to integrate with outside systems: in this case, S3 and Snowflake, but many other systems can be used.
To accomplish this, Flow uses **connectors**.
Connectors are plug-in components that interface between Flow and an outside system.
Today, you'll use Flow's S3 capture connector and Snowflake materialization connector.

You'll start by creating your capture.

## Capture Citi Bike data from S3

1. Go to the Flow web app at [dashboard.estuary.dev](http://dashboard.estuary.dev) and sign in.

2. Click the **Sources** tab and choose **New Capture**

   All of the available capture connectors — representing the possible data sources — appear as tiles.

3. Find the **Amazon S3** tile and click **Capture**.

   A form appears with the properties required for an S3 capture. Every connector requires different properties to configure.

   First, you'll name your capture.

4. Click inside the **Name** box.

   Names of entities in Flow must be unique. They're organized by prefixes, similar to paths in a file system.

   You'll see one or more prefixes pertaining to your organization.
   These prefixes represent the **namespaces** of Flow to which you have access.

5. Click your prefix from the dropdown and append a unique name after it. For example, `myOrg/yourname/citibiketutorial`.

6. Next, fill out the required properties for S3.

   * **AWS Access Key ID** and **AWS Secret Access Key**: The bucket is public, so you can leave these fields blank.

   * **AWS Region**: `us-east-1`

   * **Bucket**: `tripdata`

   * **Prefix**: The storage bucket isn't organized by prefixes, so leave this blank.

   * **Match Keys**: `2022`

   The Citi Bike storage bucket has been around for a while. Some of the older datasets have incorrect file extensions or contain data in different formats. By selecting a subset of files from the year 2022, you'll make things easier to manage for the purposes of this tutorial.
   (In a real-world use case, you'd likely reconcile the different schemas of the various data formats using a **derivation**.
   [Derivations](../../concepts/README.md#derivations) are a more advanced Flow skill.)

7. Click **Next**.

   Flow uses the configuration you provided to initiate a connection with S3. It generates a list of **collections** that will store the data inside Flow. In this case, there's just one collection from the bucket.

     Once this process completes, you can move on to the next step. If there's an error, go back and check your configuration.
8. Click **Save and Publish**.

   Flow deploys, or **publishes**, your capture, including your change to the schema. You'll see a notification when the this is complete.

   A subset of data from the Citi Bike tripdata bucket has been captured to a Flow collection. Now, you can materialize that data to Snowflake.

9. Click **Materialize Collections**.

## Prepare Snowflake to use with Flow

Before you can materialize from Flow to Snowflake, you need to complete some setup steps.

1. Leave the Flow web app open. In a new window or tab, go to your Snowflake console.

   If you're a new trial user, you should have received instructions by email. For additional help in this section, see the [Snowflake documentation](https://docs.snowflake.com/en/user-guide-getting-started.html).

2. Create a new SQL worksheet if you don't have one open.

   This provides an interface where you can run queries.

3. Paste the following script into the console, changing the value for `estuary_password` from `secret` to a strong password:

```sql
set database_name = 'ESTUARY_DB';
set warehouse_name = 'ESTUARY_WH';
set estuary_role = 'ESTUARY_ROLE';
set estuary_user = 'ESTUARY_USER';
set estuary_password = 'secret';
set estuary_schema = 'ESTUARY_SCHEMA';
-- create role and schema for Estuary
create role if not exists identifier($estuary_role);
grant role identifier($estuary_role) to role SYSADMIN;
-- Create snowflake DB
create database if not exists identifier($database_name);
use database identifier($database_name);
create schema if not exists identifier($estuary_schema);
-- create a user for Estuary
create user if not exists identifier($estuary_user)
password = $estuary_password
default_role = $estuary_role
default_warehouse = $warehouse_name;
grant role identifier($estuary_role) to user identifier($estuary_user);
grant all on schema identifier($estuary_schema) to identifier($estuary_role);
-- create a warehouse for estuary
create warehouse if not exists identifier($warehouse_name)
warehouse_size = xsmall
warehouse_type = standard
auto_suspend = 60
auto_resume = true
initially_suspended = true;
-- grant Estuary role access to warehouse
grant USAGE
on warehouse identifier($warehouse_name)
to role identifier($estuary_role);
-- grant Estuary access to database
grant CREATE SCHEMA, MONITOR, USAGE on database identifier($database_name) to role identifier($estuary_role);
-- change role to ACCOUNTADMIN for STORAGE INTEGRATION support to Estuary (only needed for Snowflake on GCP)
use role ACCOUNTADMIN;
grant CREATE INTEGRATION on account to role identifier($estuary_role);
use role sysadmin;
COMMIT;
```

4. Click the drop-down arrow next to the **Run** button and click **Run All**.

  Snowflake runs all the queries and is ready to use with Flow.

5. Return to the Flow web application.

## Materialize your Flow collection to Snowflake

You were directed to the **Materializations** page.
All of the available materialization connectors — representing the possible data destinations — are shown as tiles.

1. Find the **Snowflake** tile and click **Materialization**.

   A new form appears with the properties required to materialize to Snowflake.

2. Click inside the **Name** box.

3. Click your prefix from the dropdown and append a unique name after it. For example, `myOrg/yourname/citibiketutorial`.

4. Next, fill out the required properties for Snowflake (most of these come from the script you just ran).

   * **Host URL**: This is the URL you use to log into Snowflake. If you recently signed up for a trial, it should be in your email. Omit the protocol from the beginning. For example, `ACCOUNTID.region.cloudprovider.snowflakecomputing.com` or `orgname-accountname.snowflakecomputing.com`.

      [Learn more about account identifiers and host URLs.](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used)

   * **Account**: Your account identifier. This is part of the Host URL. Using the previous examples, it would be `ACCOUNTID` or `accountname`.

   * **User**: `ESTUARY_USER`

   * **Password**: `secret` (Substitute the password you set in the script.)

   * **Database**: `ESTUARY_DB`

   * **Schema**: `ESTUARY_SCHEMA`

   * **Warehouse**: `ESTUARY_WH`

   * **Role**: `ESTUARY_ROLE`

4. Scroll down to view the **Source Collections** section and change the default name in the **Table** field to `CitiBikeData` or another name of your choosing.

   Every Flow collection is defined by one or more **schemas**.
   Because S3 is a cloud storage bucket, the schema used to ingest the data is quite permissive.

   You'll add a more detailed schema for Flow to use to materialize the data to Snowflake. This will ensure that each field from the source CSV is mapped to a column in the Snowflake table.

5. With the collection still selected, click its **Collection** tab. Then, click **Schema Inference**.

   Flow examines the data and automatically generates a new `readSchema`. Scroll through and note the differences between this and the original schema, renamed `writeSchema`.

6. Click **Apply Inferred Schema**.

7. Click **Next**.

   Flow uses the configuration you provided to initiate a connection with Snowflake and generate a specification with details of the materialization.

   Once this process completes, you can move on to the next step. If there's an error, go back and check your configuration.

8. Click **Save and Publish**.

   Flow publishes the materialization.

9. Return to the Snowflake console and expand ESTUARY_DB and ESTUARY_SCHEMA.
You'll find the materialized table there.

## Conclusion

You've created a complete Data Flow that ingests the Citi Bike CSV files from an Amazon S3 bucket and materializes them into your Snowflake database.

When Citi Bike uploads new data, it'll be reflected in Snowflake in near-real-time, so long as you don't disable your capture or materialization.

Data warehouses like Snowflake are designed to power data analytics. From here, you can begin any number of analytical workflows.

#### Want to learn more?

* For more information on the connectors you used today, see the pages on [S3](../../reference/Connectors/capture-connectors/amazon-s3.md) and [Snowflake](../../reference/Connectors/materialization-connectors/Snowflake.md).

* You can create a Data Flow using any combination of supported connectors with a similar process to the one you followed in this tutorial. For a more generalized procedure, see the [guide to create a Data Flow](../../guides/create-dataflow.md).
