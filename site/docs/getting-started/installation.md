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

   Alternatively, Mac users can install with Homebrew:
   ```console
   brew tap estuary/flowctl
   brew install flowctl
   ```

The flowctl source files are also on GitHub [here](https://go.estuary.dev/flowctl).

Once you've installed flowctl and are ready to begin working, authenticate your session using an access token.

1. Ensure that you have an Estuary account and have signed into the Flow web app before.

2. In the terminal of your local development environment, run:
   ``` console
   flowctl auth login
   ```
   In a browser window, the web app opens to the CLI-API tab.

3. Copy the access token.

4. Return to the terminal, paste the access token, and press Enter.

The token will expire after a predetermined duration. Repeat this process to re-authenticate.


[Learn more about using flowctl.](../concepts/flowctl.md)

## Configuring your cloud storage bucket for use with Flow

During your trial period, Flow uses Estuary's cloud storage to temporarily store your data.
When you upgrade from a trial to an organizational account, you're provisioned a unique [prefix](../concepts/catalogs.md#namespace) in the Flow namespace,
and transition to using your own cloud storage bucket to store your Flow data. This is called a [storage mapping](../concepts/storage-mappings.md).

Flow supports Google Cloud Storage and Amazon S3 buckets.
Before your account manager configures your bucket as your storage mapping, you must grant access to Estuary.

#### Google Cloud Storage buckets

Follow the steps to [add a principal to a bucket level policy](https://cloud.google.com/storage/docs/access-control/using-iam-permissions#bucket-add).

* For the principal, enter `flow-258@helpful-kingdom-273219.iam.gserviceaccount.com`
* Select the [`roles/storage.admin`](https://cloud.google.com/storage/docs/access-control/iam-roles) role.

#### Amazon S3 buckets

Contact your Estuary account manager for instructions.

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