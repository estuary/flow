---
sidebar_position: 2
---
# Create a derivation with flowctl

Once you're familiar with creating a basic [Data Flow](../concepts/README.md#essential-concepts), you can take it a step further:
transforming your data with [derivations](../concepts/derivations.md).
A **derivation** is a kind of Flow collection that results from the transformation of one or more other collections.

:::info Beta
Derivation creation is currently a developer workflow that uses [flowctl](../concepts/flowctl.md).
Support for derivations in the Flow web application will be added in the future.
:::

## Prerequisites

* An existing Data Flow with a collection defined. Typically, you create this in the Flow web application.
If you need help, see the [guide to create a data flow](./create-dataflow.md).

* flowctl installed locally. For help, see the [installation instructions](../concepts/flowctl.md#installation-and-setup)

## Pull your specification files locally

To work on the Data Flow locally, you must authenticate with the Flow servers.
Then, you'll need to add the appropriate files from the catalog to a **draft** and copy the draft to your local environment.

1. Go to the [Flow web application](https://dashboard.estuary.dev). On the **Admin** page, click the **CLI-API** tab and copy the access token.

2. In your local environment, authenticate with the token:

   ```console
   flowctl auth token --token ${your-token-here}
   ```

   The output message `Configured access token` indicates that you can now work with your Flow account in your local environment,
   using the capabilities you've been provisioned.
   To learn more about capabilities and permissions, see the [authorization documentation](../reference/authentication.md).

3. Begin by creating a fresh draft. This is where you'll add the specification files you need from the catalog.

   ```console
   flowctl draft create
   ```

   The output table shows the draft ID and creation time. It doesn't have any catalog entities in it yet.
   You'll add the source collection's specification to the draft.

4. Identify the collection(s) from the catalog that contains the data you want to derive, and add it to your draft.

   ```console
   flowctl catalog draft --name namespace/my-dataflow/my-collection
   ```

   :::tip
   If you're unsure of the name the collection, check the web application. You can also use `flowctl draft list` locally
   to begin exploring the catalog items available to you.
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
   :::

2. Open the specification file in your preferred editor.

   It will look similar to the following (this example uses the default collection from the Hello World test capture, available in the web app):

   ```yaml
   collections:
      estuary/data-flow-name/greetings:
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

   * The [schema](../concepts/schemas.md) and [collection key](../concepts/collections.md#keys) should reflect your desired output.
   * Add the `derivation` stanza. The TypeScript module you name will be generated next, and you'll define the transformation's function there.

   ```yaml
   collections:
   estuary/data-flow-name/greetings:
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
   estuary/data-flow-name/dozen-greetings:fl
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
      key:
         - /dozens
      derivation:
         transform:
         greetings-by-dozen:
            source: {name: estuary/data-flow-name/greetings}
            publish: {lambda: typescript}
         typescript: {module: divide-by-twelve.ts}
      ```

## Transform with a TypeScript module

1. Generate the TypeScript module.

   ```console
   flowctl typescript generate --source ./path-to/your-file/flow.yaml
   ```

   The TypeScript file you named has been created and stubbed out.
   You only need to add the function body.

2. Open the new TypeScript module. It will look similar to the following

   ```typescript
   import { IDerivation, Document, Register, GreetingsByDozenSource } from 'flow/estuary/data-flow-name/dozen-greetings';

   // Implementation for derivation flow.yaml#/collections/estuary~1data-flow-name~1dozen-greetings/derivation.
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

   Save the file.

   ```typescript
   import { IDerivation, Document, Register, GreetingsByDozenSource } from 'flow/estuary/data-flow-name/dozen-greetings';

   // Implementation for derivation estuary/data-flow-name/flow.yaml#/collections/estuary~1data-flow-name~1dozen-greetings/derivation.
   export class Derivation implements IDerivation {
      greetingsByDozenPublish(
         source: GreetingsByDozenSource,
         _register: Register,
         _previous: Register,
      ): Document[] {
         let count = source.count;
         let dozen = count / 12;
         let out = {
         dozens: dozen,
         ...source
         }
         return [out]
     }
   }
   ```

4. Optional: add a test to the `flow.yaml` file containing your collections.
This helps you verify that your data is transformed correctly.

```yaml
tests:
  estuary/data-flow-name/divide-test:
    - ingest:
        collection: estuary/data-flow-name/greetings
        documents:
          - { count: 24, message: "Hello #24" }
    - verify:
        collection: estuary/data-flow-name/dozen-greetings
        documents:
          - { dozens: 2, count: 24, message: "Hello #24"}

[Learn about tests.](../concepts/tests.md)

## Publish the derivation

1. **Author** your draft. This adds the changes you made locally to the draft on the Estuary servers:

   ```console
   flowctl draft author --source flow.yaml
   ```

   Note that the file source is the top level `flow.yaml` in your working directory, not the file you worked on.
   This file `imports` all others in the local draft, so your changes will be included.

2. Run your test.

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