---
sidebar_position: 3
---
# Create a Derivation

Once you're familiar with creating a basic [Data Flow](../../concepts/README.md#essential-concepts), you can take things a step further
and transform your data with [derivations](../../concepts/derivations.md).

A **derivation** is a kind of Flow collection that results from the transformation of one or more other collections.
This transformed stream of data keeps the order of the source data intact,
and can then be **materialized** to an outside system or further transformed with another derivation.
When you master derivations, you unlock the full flexibility and power of Flow.

## Prerequisites

* A Flow account and access to the web app.
If you don't have an account yet, [go to the web app](https://dashboard.estuary.dev) to register for free.

* An existing Flow **collection**. Typically, you create this through a **capture** in the Flow web application.
If you need help, see the [guide to create a Data Flow](../create-dataflow.md).

* The `flowctl` CLI [installed and authenticated](/guides/get-started-with-flowctl).

   You can authorize `flowctl` with a refresh token:

   1. [Generate an Estuary Flow refresh token](/guides/how_to_generate_refresh_token).

   2. Run `flowctl auth token --token <paste-token-here>`

## Start a derivation in the UI

You can create a **draft** derivation in the UI to quickly set up a new derivation template.
This will generate stub files for you that you can use to complete your derivation.

To create a derivation in this manner:

1. Go to the [**collections** page](https://dashboard.estuary.dev/collections) in the dashboard.

2. Click the **New Transformation** button to open the "Derive A New Collection" modal.

3. **Add** one or more collections you want to transform.

4. Choose between **SQL** or **TypeScript** for your language.

5. Enter a **name** for your derived collection.

6. Click **Create Draft**.

Your draft specification will be created and a new modal screen will be displayed with instructions for proceeding with `flowctl`:

1. Make sure to **copy** the provided `flowctl draft select` command. This includes the ID of your new draft specification.

   Run the command in a terminal in your development environment. This sets your derivation spec as your current draft.

2. To start developing your draft locally, run:

   ```shell
   flowctl draft develop
   ```

   This command pulls the current draft specification, generating a new file structure in your local development environment.

3. Open the generated files in a code editor. To complete your transformation, you will edit:

   * The **deepest-nested** `flow.yaml` file. This should contain your desired [schema](/concepts/schemas) and specification for your derived collection.

      [Learn more about crafting a collection specification](/concepts/collections/#specification).

   * SQL or TypeScript **transformation files**. These are generated based on your chosen language and will contain your transformation logic.

      The resulting fields should match the schema defined in your `flow.yaml` file.

      You can see more on modifying these files in the [SQL](#add-a-sql-derivation) and [TypeScript](#add-a-typescript-derivation) sections below.

4. Preview your results to ensure your transformation is working as expected:

   ```shell
   flowctl preview --source flow.yaml
   ```

5. Once you are happy with your results, you can publish your draft back to Estuary.
Since you're working with an existing draft, you first need to sync your local copy back to Estuary:

   ```shell
   flowctl draft author --source flow.yaml
   ```

6. Publish the specification.

   ```shell
   flowctl draft publish
   ```

   This removes the derivation from your drafts. To modify your specification again later, you can access it with the `flowctl catalog` command.

The derivation you created is now live and ready for further use.
You can access it from the web application and [materialize it to a destination](../create-dataflow.md#create-a-materialization),
just as you would any other Flow collection.

## Create a derivation locally

In Estuary, a derivation is a new collection that has been **derived** from an existing source collection.

You can therefore also create a new derivation specification from scratch using a `flow.yaml` file.
For this example, we will add our new derived collection to our source collection spec.

1. Locate the source collection for your derivation. Either:

   * Check the web app's **Collections**.
   All published entities to which you have access are listed and can be searched.

   * Run `flowctl catalog list --collections`. This command returns a complete list of collections to which you have access.
   You can refine by specifying a `--prefix`.

2. Pull the source collection locally using the full collection name.

   ```console
   flowctl catalog pull-specs --name acmeCo/resources/anvils
   ```

   The source files are written to your current working directory.

3. Each slash-delimited prefix of your collection name has become a folder. Open the nested folders to find the `flow.yaml` file with the collection specification.

   Following the example above, you'd open the folders called `acmeCo`, then `resources` to find the correct `flow.yaml` file.

   The file contains the source collection specification and schema.

4. Add the derivation as a second collection in the `flow.yaml` file.

   1. Write the [schema](/concepts/schemas) you'd like your derivation to conform to and specify the [collection key](/concepts/collections/#keys). Reference the source collection's schema, and keep in mind the transformation required to get from the source schema to the new schema.

   2. Add the `derive` stanza. See examples for [SQL](#add-a-sql-derivation) and [TypeScript](#add-a-sql-derivation) below. Give your transform a unique name.

5. Stub out the SQL or TypeScript files for your transform.

   ```console
   flowctl generate --source flow.yaml
   ```

6. Locate the generated file, likely in the same subdirectory as the `flow.yaml` file you've been working in.

7. Write your transformation.

8. Preview the derivation locally.

   ```console
   flowctl preview --source flow.yaml
   ```

9. If the preview output appears how you'd expect, **publish** the derivation.

   ```console
   flowctl catalog publish --source flow.yaml
   ```

The derivation you created is now live and ready for further use.
You can access it from the web application and [materialize it to a destination](../create-dataflow.md#create-a-materialization),
just as you would any other Flow collection.

## Add a SQL derivation

If you chose **SQL** as your transformation language, follow these steps.

Along with the derivation's `flow.yaml` you worked with in the previous steps, you may generate two types of SQL file:

* A **lambda** file. This is where you'll write your first SQL transformation.
Its name follows the pattern `derivation-name.lambda.source-collection-name.sql`.
Using the example above, it'd be called `anvil-status.lambda.anvils.sql`.

* A **migrations** file. [Migrations](/concepts/derivations/#migrations) allow you to leverage other features of the sqlite database that backs your derivation by creating tables, indices, views, and more.
Its name follows the pattern `derivation-name.migration.0.sql`.
Using the example above, it'd be called `anvil-status.migration.0.sql`.

1. Open the `flow.yaml` file for your derivation. It looks something like this:

   ```yaml
   collections:
     acmeCo/resources/anvil-status:
       schema:
         properties:
           your_key:
             type: string
           required:
             - your_key
         type: object
       key:
         - /your_key
       derive:
         using:
           sqlite:
             migrations:
               - anvil-status.migration.0.sql
         transforms:
           - name: anvils
             source: acmeCo/resources/anvils
             shuffle: any
             lambda: anvil-status.lambda.anvils.sql
   ```

   Note the stubbed out schema and key.

2. Write the [schema](/concepts/schemas) you'd like your derivation to conform to and specify its [collection key](/concepts/collections/#keys). Keep in mind:

   * The source collection's schema.

   * The transformation required to get from the source schema to the new schema.

3. Give the transform a unique `name` (by default, it's the name of the source collection).

4. In the lambda file, write your SQL transformation.

:::info Tip
For help writing your derivation, start with these examples:

* [Continuous materialized view tutorial](/getting-started/tutorials/continuous-materialized-view)
* [Acme Bank examples](/getting-started/tutorials/derivations_acmebank)

The main [derivations page](/concepts/derivations) includes many other examples and in-depth explanations of how derivations work.
:::

5. If necessary, open the migration file and write your migration.

   If you won't be using a migration, you can omit the `migrations` stanza:

   ```yaml
   derive:
     using:
       sqlite: {}
   ```

6. Preview the derivation locally.

   ```console
   flowctl preview --source flow.yaml
   ```

7. If the preview output appears as expected, **publish** the derivation.

   ```console
   flowctl catalog publish --source flow.yaml
   ```

The derivation you created is now live and ready for further use.
You can access it from the web application and [materialize it to a destination](../create-dataflow.md#create-a-materialization),
just as you would any other Flow collection.

## Add a TypeScript derivation

If you chose **TypeScript** as your transformation language, follow these steps.

Along with the derivation's `flow.yaml` you worked with in the previous steps, you can create a file to handle the TypeScript transformation.
It follows the naming convention `derivation-name.ts`.
Using the example above, it'd be called `anvil-status.ts`.

1. Open the `flow.yaml` file for your derivation. It looks something like this:

   ```yaml
   collections:
     acmeCo/resources/anvil-status:
       schema:
         properties:
           your_key:
             type: string
           required:
             - your_key
         type: object
       key:
         - /your_key
       derive:
         using:
           typescript:
             module: anvil-status.ts
         transforms:
           - name: anvils
           source: acmeCo/resources/anvils
           shuffle: any
   ```

   Note the stubbed out schema and key.

2. Write the [schema](/concepts/schemas) you'd like your derivation to conform to and specify the [collection key](/concepts/collections/#keys). Keep in mind:

   * The source collection's schema.

   * The transformation required to get from the source schema to the new schema.

3. Give the transform a unique `name` (by default, it's the name of the source collection).

4. In the TypeScript file, write your transformation.

:::info Tip
For help writing a TypeScript derivation, start with [this example](/guides/transform_data_using_typescript).

The main [derivations page](../../concepts/derivations.md) includes many other examples and in-depth explanations of how derivations work.
:::

6. Preview the derivation locally.

   ```console
   flowctl preview --source flow.yaml
   ```

7. If the preview output appears how you'd expect, **publish** the derivation.

   ```console
   flowctl catalog publish --source flow.yaml
   ```

The derivation you created is now live and ready for further use.
You can access it from the web application and [materialize it to a destination](../create-dataflow.md#create-a-materialization),
just as you would any other Flow collection.

## Updating an existing derivation

Derivations are applied on a go-forward basis only.

If you would like to make an update to an existing derivation (for example, adding columns to the derived collection), you can add a new transform by changing the name of your existing transform to a new name, and at the same time updating your lambda or TypeScript module.

From the Flow's perspective, this is equivalent to deleting the old transform and adding a new one. This will backfill over the source collection again with the updated SQL statement.