# Create a simple data flow

Whether you're learning to use Flow or testing a new pipeline, much of your work will take place in a local or virtual environment. This guide outlines the basic steps to create and deploy a data flow using Flow's current GitOps workflow.

:::info Beta
Flow's UI is coming in 2022. In the future, Estuary will continue to support and improve both GitOps and UI-based workflows.
:::

## Prerequisites
This guide assumes a basic understanding of Flow and its key concepts. At a minimum, begin by reading the [high level concepts](../concepts/README.md) documentation.

## Introduction
The simplest Flow **catalog** comprises three types of entities that define your data flow: a data **capture** from an external source, one or more **collections**, which store that data in the Flow runtime, and a **materialization**, to push them to an external destination.

In the majority of cases, the capture and materialization each rely on a plug-in **connector**. Here, we'll walk through how to leverage various connectors, configure them in your **catalog specification**, and run the catalog in a temporary **data plane**.

## Steps
1. Set up a development environment. We recommend a VM-backed environment using GitHub Codespaces, if you have access. Otherwise, you can set up a local environment. Follow the [setup requirements here](../getting-started/installation.md).

    Next, you'll create your catalog spec. Rather than starting from scratch, you'll use the guided `flowctl discover` process to generate one that is pre-configured for the capture connector you're using.

    :::tip
    You may notice the template you cloned in step 1 comes with a catalog spec. It's an example, so you can disregard it unless you choose to run the [tutorial](../../getting-started/flow-tutorials/hello-flow).
    :::

2. Refer to the [capture connectors list](../../reference/connectors/capture-connectors) and find your data source system. Click on its **configuration** link, set up prerequisites as necessary, and follow the instructions to generate a catalog spec with [`flowctl discover`](../concepts/connectors.md#flowctl-discover).

    A generalized version of the `discover` workflow is as follows:
    1. In your terminal, run: `flowctl discover --image=ghcr.io/estuary/<connector-name>:dev`
    2. In the generated file called `discover-source-<connector-name>-config.yaml`, fill in the required values.
    3. Re-run the command. A catalog spec called `discover-source-<connector-name>.flow.yaml` is generated.

    You now have a catalog spec that contains a capture and one or more collections.

    In a production workflow, your collections would be stored in a cloud storage bucket. In the development workflow, cloud storage isn't used, but you must supply a placeholder **storage mapping**.

3. Copy and paste the following section at the top of your catalog spec, where `tenant` matches the tenant used for your collections:

    ```yaml
    storageMappings:
      tenant/:
        stores:
          - bucket: "my-bucket"
            provider: "S3"
    ```

    To complete your end-to-end dataflow, you'll now add a materialization. Like your capture, materializations are configured differently depending on the connector and endpoint system; however, they are configured manually.

4. Go to the [materialization connectors list](../../reference/connectors/materialization-connectors). Find your destination system, open its **configuration** page, and follow the sample to configure your materialization.

5. Launch the system locally:
    ```console
    flowctl temp-data-plane
    ```
6. Leave that running and open a new shell window. There, deploy your catalog:
    ```console
    flowctl deploy --source=your_file.flow.yaml --wait-and-cleanup
    ```
    You'll now be able to see data flowing between your source and destination systems.

    When you're done, press Ctrl-C to exit and clean up.

## What's next?

With Flow, you can build a wide range of scalable real-time data integrations, with optional transformations. Flow is currently in private beta, and its capabilities are growing rapidly.
* You can add multiple captures and materializations to the same catalog spec. Check back regularly; we frequently add new connectors.
* You can add [derivations](../concepts/catalog-entities/derivations/README.md), but note that this area of functionality is under development.
* Current beta customers can work with the Estuary team to set up production-level pipelines.