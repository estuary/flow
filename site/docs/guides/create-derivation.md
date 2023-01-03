---
sidebar_position: 3
---
# Create a derivation with flowctl

Once you're familiar with creating a basic [Data Flow](../concepts/README.md#essential-concepts), you can take things a step further
and transform your data with [derivations](../concepts/derivations.md).

A **derivation** is a kind of Flow collection that results from the transformation of one or more other collections.
This transformed stream of data keeps the order of the source data intact,
and can then be **materialized** to an outside system or further transformed with another derivation.
When you master derivations, you unlock the full flexibility and power of your Data Flows.

:::info Beta
Derivation creation is currently a developer workflow that uses [flowctl](../concepts/flowctl.md).
Support for derivations in the Flow web application will be added in the future.
:::

## Prerequisites

* A Flow account and access to the web app.
If you don't have an account yet, [go to the web app](https://dashboard.estuary.dev) to register for a free trial.

* An existing Flow **collection**. Typically, you create this through a **capture** in the Flow web application.
If you need help, see the [guide to create a Data Flow](./create-dataflow.md).

* **flowctl** installed locally. For help, see the [installation instructions](../concepts/flowctl.md#installation-and-setup).

## Pull your specification files locally

To begin working in your local environment, you must authenticate Flow from the command line.
Then, you'll need to add your source collection's specification files to a **draft** and bring the draft into your local environment for editing.

1. In your local environment, authenticate flowctl:

   ```console
   flowctl auth login
   ```

   A browser window opens to the CLI-API tab of the Flow web app.

2. Copy the access token and paste it in the Auth Token prompt in your terminal. Press Enter.

3. Begin by creating a fresh draft. This is where you'll add the specification files you need from the catalog.

   ```console
   flowctl draft create
   ```

   The output table shows the draft ID and creation time. It doesn't have any catalog entities in it yet.
   You'll add the source collection's specification to the draft.

4. Identify the collection (or collections) in the catalog that contains the data you want to derive, and add it to your draft.

   ```console
   flowctl catalog draft --name namespace/data-flow-name/my-collection
   ```

   :::tip
   If you're unsure of the name of the collection, check **Collections** page in the web application.
   You can also use `flowctl draft list` locally
   to begin exploring the catalog items available to you.

   The name of your collection may not follow the structure of the examples provided;
   simply copy the entire name as you see it, including all prefixes.
   :::

   The output confirms that the entity name you specified has been added to your draft, and is of the type `collection`.

   Your draft is set up, but still exists only on the Estuary servers.

5. Pull the draft locally to edit the specification files.

   ```console
   flowctl draft develop
   ```

   The specification files are written to your working directory.

## Add a derivation

1. Locate the specification YAML file for the collection you want to transform.

   In your working directory, you'll see a top-level file called `flow.yaml`.
   Within a subdirectory that shares the name of your Data Flow, you'll find a second `flow.yaml` — this contains the collection specification.

   :::tip
   Use `tree` to visualize your current working directory. This is a helpful tool to understand the [files that underlie your local draft](../concepts/flowctl.md#development-directories).
   For example:

   ```console
   .
   ├── namespace
   │   └── data-flow-name
   │       └── flow.yaml
   ├── flow.yaml
   ├── flow_generated
   │   ├── flow
   │   │   ├── main.ts
   │   │   ├── routes.ts
   │   │   └── server.ts
   │   ├── tsconfig-files.json
   │   └── types
   │       └── namespace
   │           └── data-flow-name
   │               └── my-collection.d.ts
   ├── package.json
   └── tsconfig.json
   ```
   :::

2. Open the specification file in your preferred editor.

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
    ```

   You'll add the derivation to the same file.

   :::tip
   You may also create the derivation specification in a separate file; the results will be the same.
   However, if using separate files, you must make sure that the file with the derivation imports the source collection's specification,
   and that the top-level `flow.yaml` in your working directory imports all specification files.
   [Learn more about imports.](../concepts/import.md)

3. Add a new collection below the first one.

   * The collection must have a schema that reflects your desired transformation output.
     They can be whatever you want, as long as they follow Flow's standard formatting.
     For help, see the [schemas](../concepts/schemas.md) and [collection key](../concepts/collections.md#keys) documentation.
   * Add the `derivation` stanza. The TypeScript module you name will be generated next, and you'll define the transformation's function there.

   ```yaml
   collections:
      namespace/data-flow-name/greetings:
        {...}
      #The name for your new collection can be whatever you want,
      #so long as you have permissions in the namespace.
      #Typically, you'll want to simply copy the source prefix
      #and add a unique collection name.
      namespace/data-flow-name/dozen-greetings:
         #In this example, our objective is to round the number of greetings to the nearest dozen.
         #We keep the `count` and `message` properties from the source,
         #and add a new field called `dozens`.
         schema:
            properties:
               count:
                  type: integer
               message:
                  type: string
               dozens:
                  type: integer
            required:
            - dozens
            - count
            - message
            type: object
         #Since we're interested in estimating by the dozen, we make `dozens` our collection key.
         key:
            - /dozens
         derivation:
            transform:
               #The transform name can be anything you'd like.
               greetings-by-dozen:
                  #Paste the full name of the source collection.
                  source: {name: namespace/data-flow-name/greetings}
                  #This simple transform only requires a **publish lambda* function.
                  #More complex transforms also use **update lambdas**.
                  #See the Derivations documentation to learn more about lambdas.
                  publish: {lambda: typescript}
            #The name you provide for the module will be generated next.
            typescript: {module: divide-by-twelve.ts}
      ```
   Save the file.

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
[Learn more about derivations and see examples.](../concepts/derivations.md)

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

   [Learn about tests.](../concepts/tests.md)

## Publish the derivation

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
You can access it from the web application and [materialize it to a destination](./create-dataflow.md#create-a-materialization),
just as you would any other Flow collection.