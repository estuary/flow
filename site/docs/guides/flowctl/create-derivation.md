---
sidebar_position: 3
---
# Create a derivation with flowctl

Once you're familiar with creating a basic [Data Flow](../../concepts/README.md#essential-concepts), you can take things a step further
and transform your data with [derivations](../../concepts/derivations.md).

A **derivation** is a kind of Flow collection that results from the transformation of one or more other collections.
This transformed stream of data keeps the order of the source data intact,
and can then be **materialized** to an outside system or further transformed with another derivation.
When you master derivations, you unlock the full flexibility and power of your Data Flows.

## Prerequisites

* A Flow account and access to the web app.
If you don't have an account yet, [go to the web app](https://dashboard.estuary.dev) to register for a free trial.

* An existing Flow **collection**. Typically, you create this through a **capture** in the Flow web application.
If you need help, see the [guide to create a Data Flow](../create-dataflow.md).

## Getting Started

To create a derivation, navigate to the [Collections](https://dashboard.estuary.dev/collections) page in Flow, click on the **NEW TRANSFORMATION** button. This brings up a **Derive A New Collection** pop-up window.

Deriving a new collection consists of three steps:

* **Step 1:** Select source collections: In the **Available Collections** dropdown, select the collection you want to derive.

* **Step 2:** Transformation Language: There are two language options to select: **SQL** and **Typescript**.

* **Step 3:** Write transformations: Give your derived collection a name. Then click the **PROCEED TO GITPOD** button.

This opens up GitPod in another tab, where an environment is already set up for you.

## GitPod Set-Up

We integrate with GitPod in order to let users leverage the full capabilities of SQLite. GitPod is free to use. It is an online Integrated Development Environment (IDE) that provides a complete development environment that can be accessed through a web browser, with all the necessary tools and dependencies pre-installed.

In GitPod, you will set up your derivation's schema specs in the **flow.yaml** file. The tutorial [here](https://docs.estuary.dev/concepts/derivations/#tutorial) walks through several examples on how to fill out **flow.yaml** depending on your use case.

## Authentication

When connecting to GitPod, you will have already authenticated Flow, but if you leave GitPod opened for too long, you may have to reauthenticate Flow.

To authorize flowctl:

1. Go to the [CLI-API tab of the web app](https://dashboard.estuary.dev/admin/api) and copy your access token.

2. Run `flowctl auth token --token <paste-token-here>` on the Terminal tab in GitPod.

## Add a SQL derivation

1. In the Flow UI **Derive A New Collection** screen, select **SQL** as your transformation language.

2. In GitPod, locate the specification YAML file for the collection you want to transform.

   In your working directory, you'll see a top-level file called `flow.yaml`.
   Within a subdirectory that shares the name of your Data Flow, you'll find a second `flow.yaml` — this contains the collection specification.

3. Open the specification file `flow.yaml`.

   It will look similar to the following. (This example uses the default collection from the Hello World test capture, available in the web app):

   ```yaml
   collections:
      #The Hello World capture outputs a collection called `greetings`.
      namespace/data-flow-name/greetings:
         schema:
            properties:
            count:
               type: integer
            message:
               type: string
            required:
            - count
            - message
            type: object
         key:
            - /count
      derive:
         using:
            sqlite:
               migrations:
                  - greetings.migrations.0.sql
         transforms:
              #The transform name can be anything you'd like.
            - name: greetings-by-dozen
              #Paste the full name of the source collection.
              source: namespace/data-flow-name/greetings
              #The lambda holds your SQL transformation statement(s). You can either place your SQL directly here or in the separate lambda file.
              lambda: greetings.lambda.greetings-by-dozen.sql
    ```

   Fill out the schema specs, key, sqlite migration, transform name, source, and lambda. [This tutorial](https://docs.estuary.dev/concepts/derivations/#tutorial) walks through several examples on how to fill out **flow.yaml** depending on your use case.

   See also [Lambdas](https://docs.estuary.dev/concepts/derivations/#sql-lambdas) and [Migrations](https://docs.estuary.dev/concepts/derivations/#migrations) for additional details.

   Your SQL statements are evaluated with each source collection document. [Here](https://docs.estuary.dev/concepts/derivations/#sqlite) is an example of what the output document from a derivation would look like given an input document and [SQL lambda](https://docs.estuary.dev/concepts/derivations/#sql-lambdas).

## Preview the derivation

Type this command on the Terminal tab in GitPod to **preview** the derivation.

```console
flowctl preview --source flow.yaml --interval 200ms | jq -c 'del(._meta)'
```

## Publish the derivation from GitPod

**Publish** the catalog.

```console
flowctl catalog publish --source flow.yaml
```

## Updating an existing derivation

SQL statements are applied on a go-forward basis only.

If you would like to make an update to an existing derivation (for example, adding columns to the derived collection), you can add a new transform by changing the name of your existing transform to a new name, and at the same time updating your lambda.

From the platform's perspective, this is equivalent to deleting the old transform and adding a new one. This will backfill over the source collection again with the updated SQL statement.

## Transform with a TypeScript module

1. Generate the TypeScript module from the newly updated specification file.

   ```console
   flowctl typescript generate --source ./path-to/your-file/flow.yaml
   ```

   The TypeScript file you named has been created and stubbed out.
   You only need to add the function body.

2. Open the new TypeScript module. It will look similar to the following:

   ```typescript
   import { IDerivation, Document, Register, GreetingsByDozenSource } from 'flow/namespace/data-flow-name/dozen-greetings';

   // Implementation for derivation flow.yaml#/collections/namespace~1data-flow-name~1dozen-greetings/derivation.
   export class Derivation implements IDerivation {
      greetingsByDozenPublish(
         _source: GreetingsByDozenSource,
         _register: Register,
         _previous: Register,
      ): Document[] {
         throw new Error("Not implemented");
      }
   }
   ```

3. Remove the underscore in front of `source` and fill out the function body as required for your required transformation.
For more advanced transforms, you may need to activate `register` and `previous` by removing their underscores.
[Learn more about derivations and see examples.](../../concepts/derivations.md)

   This simple example rounds the `count` field to the nearest dozen.

   ```typescript
   import { IDerivation, Document, Register, GreetingsByDozenSource } from 'flow/namespace/data-flow-name/dozen-greetings';

   // Implementation for derivation namespace/data-flow-name/flow.yaml#/collections/namespace~1data-flow-name~1dozen-greetings/derivation.
   export class Derivation implements IDerivation {
      greetingsByDozenPublish(
         source: GreetingsByDozenSource,
         _register: Register,
         _previous: Register,
      ): Document[] {
          let count = source.count;
          let dozen = count / 12;
          let dozenround = Math.floor(dozen)
          let out = {
          dozens: dozenround,
          ...source
        }
        return [out]
     }
   }
   ```
   Save the file.

4. Optional: add a test to the `flow.yaml` file containing your collections.
This helps you verify that your data is transformed correctly.

   ```yaml
   collections:
      {...}
   tests:
      namespace/data-flow-name/divide-test:
         - ingest:
            collection: namespace/data-flow-name/greetings
            documents:
               - { count: 13, message: "Hello #13" }
         - verify:
            collection: namespace/data-flow-name/dozen-greetings
            documents:
               - { dozens: 1, count: 13, message: "Hello #13"}
   ```

   [Learn about tests.](../../concepts/tests.md)

## Publish the derivation in local terminal

1. **Author** your draft. This adds the changes you made locally to the draft on the Estuary servers:

   ```console
   flowctl draft author --source flow.yaml
   ```

   Note that the file source is the top level `flow.yaml` in your working directory, not the file you worked on.
   This file `imports` all others in the local draft, so your changes will be included.

2. Run generic tests, as well as your custom tests, if you created any.

   ```console
   flowctl draft test
   ```

3. **Publish** the draft to the catalog.

   ```console
   flowctl draft publish
   ```

The derivation you created is now live and ready for further use.
You can access it from the web application and [materialize it to a destination](../create-dataflow.md#create-a-materialization),
just as you would any other Flow collection.