---
sidebar_position: 1
---

# Registration and setup

Estuary Flow is a hosted web application that also offers a robust CLI.

Flow is currently in private beta. Essentially, this means that it's available to you, but new sign-ups are personally reviewed by our team.

## Get started with the Flow web application

You can sign up to get started as a Flow trial user by visiting the web application [here](https://go.estuary.dev/dashboard).

Once you've signed up with your personal information, an Estuary team member will be in touch to activate your account and discuss your business use-case, if applicable.

## Get started with the Flow CLI

After your account has been activated through the [web app](#get-started-with-the-flow-web-application), you can begin to work with your data flows from the command line.
This is not required, but it enables more advanced workflows or might simply be your preference.

Flow has a single binary, **flowctl**.

flowctl is available for:

* **Linux** x86-64. All distributions are supported.
* **MacOS** 11 (Big Sur) or later. Both Intel and M1 chips are supported.

To install, copy and paste the appropriate script below into your terminal. This will download flowctl, make it executable, and add it to your `PATH`.

   * For Linux:
   ```console
   sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-x86_64-linux' && sudo chmod +x /usr/local/bin/flowctl
   ```

   * For Mac:
   ```console
   sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-multiarch-macos' && sudo chmod +x /usr/local/bin/flowctl
   ```

Alternatively, you can find the source files on GitHub [here](https://go.estuary.dev/flowctl).

Once you've installed flowctl and are ready to begin working, authenticate your session using an access token.

1. Sign into the Flow web application.

2. Click the **Admin** tab.

3. On the Admin page, click the **CLI-API** tab. Copy the token from the **Access Token** box.

4. In the terminal of your local development environment, run:
   ``` console
   flowctl auth token --token=<copied-token>
   ```

The token will expire after a predetermined duration. Generate a new token using the web application and re-authenticate.

[Learn more about using flowctl.](../concepts/flowctl.md)

## Self-hosting Flow

The Flow runtime is available under the [Business Source License](https://github.com/estuary/flow/blob/master/LICENSE-BSL). It's possible to self-host Flow using a cloud provider of your choice.

:::caution Beta
Setup for self-hosting is not covered in this documentation, and full support is not guaranteed at this time.
We recommend using the [hosted version of Flow](#get-started-with-the-flow-web-application) for the best experience.
If you'd still like to self-host, refer to the [GitHub repository](https://github.com/estuary/flow) or the [Estuary Slack](https://join.slack.com/t/estuary-dev/shared_invite/zt-86nal6yr-VPbv~YfZE9Q~6Zl~gmZdFQ).
:::

## What's next?

Start using Flow with these recommended resources.

* **[Create your first data flow](../guides/create-dataflow.md)**:
Follow this guide to create your first data flow in the Flow web app, while learning essential flow concepts.

* **[High level concepts](../concepts/README.md)**: Start here to learn more about important Flow terms.