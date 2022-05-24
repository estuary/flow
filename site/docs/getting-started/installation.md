---
sidebar_position: 1
description: Get set up to run Flow for local development.
---

# Setting up a development environment

:::info
The Flow runtime is available for non-commercial use under the [Business Source License](https://github.com/estuary/flow/blob/master/LICENSE-BSL).

This section of documentation is designed to get you acquainted with using Flow in a local development environment.
It includes setup guidance and a quick but powerful tutorial.

Should you choose to self-host Flow using a cloud provider of your choice, note that setup is _not_ covered in this documentation.
Refer to the [GitHub repository](https://github.com/estuary/flow) or the [Estuary Slack](https://join.slack.com/t/estuary-dev/shared_invite/zt-86nal6yr-VPbv~YfZE9Q~6Zl~gmZdFQ).

If you're using the Flow web application, see the [guide](../guides/create-dataflow.md) to get started.
:::

Flow includes a [**devcontainer**](https://code.visualstudio.com/docs/remote/containers), which provides a nice development experience using a self-contained Docker-based environment. This is an easy way to get a great development experience using Flow, with auto-completion and an ideal setup for your catalog. There are currently two ways to set this up: virtually, using GitHub Codespaces, and locally, using VS Code and Docker on your machine.

## Using GitHub Codespaces

[GitHub codespaces](https://github.com/features/codespaces) provides VM-backed, portable development environments that are ideal for getting started with Flow in minutes. Currently, Codespaces is available to GitHub Teams and Enterprise customers, as well as individuals enrolled in the beta. If you have access, this is the preferred method — setting up a devcontainer in Codespaces is much quicker than doing so locally.

Visit the [Flow Template repository](https://github.com/estuary/flow-template), click **Code**, and choose **New Codespace**.

The VM spins up within a minute or two, and you can immediately begin developing and testing. The template includes a PostgreSQL database for this purpose.

## Using Visual Studio Code locally

If you don't have access to Codespaces, or prefer local development, use this method to create a local environment.

Download and install the following prerequisites:

* [Docker](https://www.docker.com/get-started)
* [VS Code](https://code.visualstudio.com)
* VS Code [Remote Containers extension](https://code.visualstudio.com/docs/remote/containers)

#### Create a Git repository from the Flow Template <a href="#create-a-git-repository-from-the-flow-template" id="create-a-git-repository-from-the-flow-template"></a>

Visit the [Flow Template repository](https://github.com/estuary/flow-template) on GitHub, click on **Use this template**, and proceed to create your repository.

#### Open in VS Code <a href="#open-in-vs-code" id="open-in-vs-code"></a>

Clone your repository locally and open it in VS Code. You'll see a popup in the lower right corner asking if you'd like to re-open the repository in a container. Click **Re-open in container**. It may take several minutes to download components and build the container.

## Test your environment

Regardless of the method you used, first test everything is working as expected. The repository contains a sample project, which includes a test. (It also serves as a quick tutorial, which we recommend as a next step).

In a terminal window, run:
```console
flowctl test --source word-counts.flow.yaml
```
Verify that it returns:
```console
Ran 1 tests, 1 passed, 0 failed
```

You're now ready to start using Flow!

[Proceed to the Flow introductory tutorial](flow-tutorials/hello-flow.md).
