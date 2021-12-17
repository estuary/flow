---
sidebar_position: 1
description: Get set up to run Flow for local development.
---

# Setting up a development environment

There are currently two ways to set up a local development environment for Flow: using a Docker image, and using VS Code devcontainers.&#x20;

Both methods require Docker. [Install Docker now](https://www.docker.com/get-started) if you don't have it already.

## Using a Docker image

Estuary provides a script that installs all the dependencies of Flow for you. It uses a Docker image to wrap Flow's multi-call binary, which allows you to work with Flow in your preferred development tool or directly from the command line.&#x20;

{% hint style="warning" %}
Due to third-party software issues, **** [**Apple silicon**](https://developer.apple.com/documentation/apple-silicon) **(M1) hardware is not fully supported for this setup method.** For now, if you are using an Apple silicon machine, we recommend the [devcontainer method](installation.md#using-visual-studio-code-devcontainers).

Flow's runtime is provided as an x86 Docker image. Docker for Mac has [known issues](https://github.com/docker/for-mac/issues/5123) running this in emulation on Apple silicon. As a result, you may experience occasional crashes, which usually present as segmentation faults.
{% endhint %}

{% hint style="info" %}
The tutorials in the documentation are written and tested with VS Code. You may need to adapt the instructions slightly using other tools.
{% endhint %}

#### Install the script

Run the following command to put the script in your PATH. The following example uses the location `/usr/local/bin`, but you may modify as needed.

```
curl -OL https://raw.githubusercontent.com/estuary/flow/master/scripts/flowctl.sh
chmod 755 flowctl.sh
sudo mv flowctl.sh /usr/local/bin/flowctl.sh
sudo ln -s flowctl.sh /usr/local/bin/flowctl
```

For more technical details and caveats about the script, see its [documentation on GitHub](https://github.com/estuary/flow/blob/master/scripts/flowctl.sh.md).&#x20;

## Using Visual Studio Code devcontainers

Flow includes a **devcontainer**, which provides a nice development experience using a self-contained Docker-based environment. This is an easy way to get a great development experience using Flow, with auto-completion set up for your Flow catalog.&#x20;

Make sure that all of the following components are downloaded and installed:

* [Docker](https://www.docker.com/get-started)
* [VS Code](https://code.visualstudio.com)
* VS Code [Remote Containers extension](https://code.visualstudio.com/docs/remote/containers)&#x20;

#### Create a Git repository from the Flow Template <a href="#create-a-git-repository-from-the-flow-template" id="create-a-git-repository-from-the-flow-template"></a>

Visit the [Flow template repository](https://github.com/estuary/flow-template) on GitHub, click on **Use this template**, and proceed to create your repository.

#### Open in VS Code <a href="#open-in-vs-code" id="open-in-vs-code"></a>

Clone your repository locally and open it in VS Code. You should see a popup in the lower right corner asking if you'd like to re-open the repository in a container. Click **Re-open in container**. This may take a minute or two the first time you do this, as it downloads everything and builds the container.

#### Verify everything works <a href="#verify-everything-works" id="verify-everything-works"></a>

This repository contains a "hello world" Flow project. To verify that everything is working correctly, open the terminal in VS Code and run `flowctl test --source hello-world.flow.yaml`. This command will exit successfully, meaning that the test passed.

## Next Steps

You're now ready to start using Flow! If you're new to Flow, then we recommend going through the [Flow Introductory Tutorial](flow-tutorials/hello-flow.md).
