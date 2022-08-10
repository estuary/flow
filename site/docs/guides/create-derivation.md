# Create a derivation with flowctl

INTRO

## Prerequisites

* An existing Flow catalog with a collection defined. Typically, you create this in the Flow web application.
If you need help, see the [guide to create a data flow](./create-dataflow.md).
* flowctl installed locally. For help, see the [installation instructions](../concepts/flowctl.md#installation-and-setup)

## Pull your catalog draft locally

Before you edit your catalog locally, you must authenticate with the Flow servers and create a **draft** copy of your existing catalog.

1. Go to the [Flow web application](https://dashboard.estuary.dev). On the **Admin** tab, copy the access token.

2. In your local environment, authenticate with the token:

   ```console
   flowctl auth token --token ${your-token-here}
   ```

   You're now able to work with your Flow account and use the capabilities you've been provisioned in your local environment.
   To learn more, see the [authorization documentation](../reference/authentication.md).

3. Begin by creating a fresh catalog draft.

   ```console
   flowctl draft create
   ```

   The output table shows the draft ID and creation time. It doesn't have any catalog entities in it yet.
   You'll add the catalog you want to work on to this draft (QUESTION IS IT A COPY OR ARE YOU WORKING DIRECTLY??)

4. Add the collection containing the data you want to derive to your draft.

   ```console
   flowctl catalog draft --name namespace/my-catalog/my-collection
   ```

   :::tip
   If you're unsure of the name the collection, check the web application. You can also use `flowctl drafts list`
   to begin exploring available catalogs in your console.
   :::

   Your draft is set up, but still exists only on the Estuary servers.

5. Pull the catalog draft locally to edit the source files.

   ```console
   flowctl draft develop
   ```

   The catalog files are written to your working directory.

## Add a derivation

1. Locate the YAML file that contains the specification for the collection you want to transform.

   Typically, you'll see a top-level file called `flow.yaml`.
   Within a subdirectory that shares the name of your catalog, you'll find a second `flow.yaml`.

   :::tip
   Use `tree` to visualize your current working directory. This is a helpful visual of your catalog's organization.
   :::

2. Open the YAML file with the collection definition in your preferred editor. You'll add the derivation to the same file.

   ???TODO maybe mention you could do it in a new file and import???
   