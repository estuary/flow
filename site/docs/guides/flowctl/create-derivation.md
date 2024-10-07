---
sidebar_position: 3
---
# Create a derivation with flowctl

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

* A development environment to work with flowctl. Choose between:

   * [GitPod](https://www.gitpod.io/), the cloud development environment integrated with Flow.
   GitPod comes ready for derivation writing, with stubbed out files and flowctl installed. You'll need a GitLab, GitHub, or BitBucket account to log in.

   * Your local development environment. [Install flowctl locally](../../getting-started/getting-started#get-started-with-the-flow-cli)

## Get started with GitPod

You'll write your derivation using GitPod, a cloud development environment integrated in the Flow web app.

1. Navigate to the [Collections](https://dashboard.estuary.dev/collections) page in Flow.

2. Click on the **New Transformation** button.

   The **Derive A New Collection** pop-up window appears.

3. In the **Available Collections** dropdown, select the collection you want to use as the source.

   For example, if your organization is `acmeCo`, you might choose the `acmeCo/resources/anvils` collection.

4. Set the transformation language to either **SQL** and **TypeScript**.

   SQL transformations can be a more approachable place to start if you're new to derivations.
   TypeScript transformations can provide more resiliency against failures through static type checking.

5. Give your derivation a name. From the dropdown, choose the name of your catalog prefix and append a unique name, for example `acmeCo/resources/anvil-status.`

6. Click **Proceed to GitPod** to create your development environment. Sign in with one of the available account types.

7. On the **New Workspace** screen, keep the **Context URL** option selected and click **Continue.**

   A GitPod development environment opens.
   A stubbed-out derivation with a transformation has already been created for you in the language you chose. Next, you'll locate and open the source files.

8. Each slash-delimited prefix of your derivation name has become a folder. Open the nested folders to find the `flow.yaml` file with the derivation specification.

   Following the example above, you'd open the folders called `acmeCo`, then `resources` to find the correct `flow.yaml` file.

   The file contains a placeholder collection specification and schema for the derivation.

   In the same folder, you'll also find supplementary TypeScript or SQL files you'll need for your transformation.

[Continue with SQL](#add-a-sql-derivation-in-gitpod)

[Continue with TypeScript](#add-a-typescript-derivation-in-gitpod)

:::info Authentication

When you first connect to GitPod, you will have already authenticated Flow, but if you leave GitPod opened for too long, you may have to reauthenticate Flow. To do this:

1. [Generate an Estuary Flow refresh token](/guides/how_to_generate_refresh_token).

2. Run `flowctl auth token --token <paste-token-here>` in the GitPod terminal.
:::

## Add a SQL derivation in GitPod

If you chose **SQL** as your transformation language, follow these steps.

Along with the derivation's `flow.yaml` you found in the previous steps, there are two other files:

* A **lambda** file. This is where you'll write your first SQL transformation.
Its name follows the pattern `derivation-name.lambda.source-collection-name.sql`.
Using the example above, it'd be called `anvil-status.lambda.anvils.sql`.

* A **migrations** file. [Migrations](../../concepts/derivations.md#migrations) allow you to leverage other features of the sqlite database that backs your derivation by creating tables, indices, views, and more.
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

2. Write the [schema](../../concepts/schemas.md) you'd like your derivation to conform to and specify its [collection key](../../concepts/collections.md#keys). Keep in mind:

   * The source collection's schema.

   * The transformation required to get from the source schema to the new schema.

3. Give the transform a unique `name` (by default, it's the name of the source collection).

4. In the lambda file, write your SQL transformation.

:::info Tip
For help writing your derivation, start with these examples:

* [Continuous materialized view tutorial](../../getting-started/tutorials/continuous-materialized-view.md)
* [Acme Bank examples](../../concepts/derivations.md#tutorial)

The main [derivations page](../../concepts/derivations.md) includes many other examples and in-depth explanations of how derivations work.
:::

5. If necessary, open the migration file and write your migration.

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

## Add a TypeScript derivation in GitPod

If you chose **TypeScript** as your transformation language, follow these steps.

Along with the derivation's `flow.yaml` you found in the previous steps, there's another file for the TypeScript transformation.
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

2. Write the [schema](../../concepts/schemas.md) you'd like your derivation to conform to and specify the [collection key](../../concepts/collections.md#keys). Keep in mind:

   * The source collection's schema.

   * The transformation required to get from the source schema to the new schema.

3. Give the transform a unique `name` (by default, it's the name of the source collection).

4. In the TypeScript file, write your transformation.

:::info Tip
For help writing a TypeScript derivation, start with [this example](../../concepts/derivations.md#current-account-balances).

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

## Create a derivation locally

Creating a derivation locally is largely the same as using GitPod, but has some extra steps. Those extra steps are explained here, but you'll find more useful context in the sections above.

1. Authorize flowctl.

   1. [Generate an Estuary Flow refresh token](/guides/how_to_generate_refresh_token).

   2. Run `flowctl auth token --token <paste-token-here>` in your local environment.

2. Locate the source collection for your derivation.

   * Check the web app's **Collections**.
   All published entities to which you have access are listed and can be searched.

   * Run `flowctl catalog list --collections`. This command returns a complete list of collections to which you have access.
   You can refine by specifying a `--prefix`.

3. Pull the source collection locally using the full collection name.

   ```console
   flowctl catalog pull-specs --name acmeCo/resources/anvils
   ```

   The source files are written to your current working directory.

4. Each slash-delimited prefix of your collection name has become a folder. Open the nested folders to find the `flow.yaml` file with the collection specification.

   Following the example above, you'd open the folders called `acmeCo`, then `resources` to find the correct `flow.yaml` file.

   The file contains the source collection specification and schema.

5. Add the derivation as a second collection in the `flow.yaml` file.

   1. Write the [schema](../../concepts/schemas.md) you'd like your derivation to conform to and specify the [collection key](../../concepts/collections.md#keys). Reference the source collection's schema, and keep in mind the transformation required to get from the source schema to the new schema.

   2. Add the `derive` stanza. See examples for [SQL](#add-a-sql-derivation-in-gitpod) and [TypeScript](#add-a-sql-derivation-in-gitpod) above. Give your transform a a unique name.

3. Stub out the SQL or TypeScript files for your transform.

   ```console
   flowctl generate --source flow.yaml
   ```

4. Locate the generated file, likely in the same subdirectory as the `flow.yaml` file you've been working in.

5. Write your transformation.

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