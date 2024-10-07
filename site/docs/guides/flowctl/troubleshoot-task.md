---
sidebar_position: 3
---
# Troubleshoot a task with flowctl

:::caution
The flowctl logs and stats subcommands have been temporarily disabled while we work on some important changes to our authorization system. We expect to have these working again soon. In the meantime, please reach out to us via Slack or email (support@estuary.dev) if you want any help.
:::

flowctl offers the most advanced views of [task logs](../../concepts/advanced/logs-stats.md).
If a task has errors or is failing in the web app, you'll be able to troubleshoot more effectively with flowctl.

## Prerequisites

To complete this workflow, you need:

* An [Estuary account](../../getting-started/getting-started)

* [flowctl installed locally](../../getting-started/getting-started#get-started-with-the-flow-cli)

## Print task logs

1. Authorize flowctl.

   1. Go to the [CLI-API tab of the web app](https://dashboard.estuary.dev/admin/api) and copy your access token.

   2. Run `flowctl auth token --token <paste-token-here>`

2. Identify the name of the failing task in the web app; for example `myOrg/marketing/leads`.
Use the tables on the Captures or Materializations pages of the web app to do so.

3. Run `flowctl logs --task <task-name>`. You have several options to get more specific. For example:

   * `flowctl logs --task myOrg/marketing/leads --follow` — If the task hasn't failed, continuously print logs as they're generated.

   * `flowctl logs --task myOrg/marketing/leads --since 1h` — Print logs from approximately the last hour.
   The actual output window is approximate and may somewhat exceed this time boundary.
   You may use any time, for example `10m` and `1d`.

## Change log level

If your logs aren't providing enough detail, you can change the log level.

Flow offers several log levels. From least to most detailed, these are:

* `error`
* `warn`
* `info` (default)
* `debug`
* `trace`

1. Follow the guide to [edit a specification with flowctl](./edit-specification-locally.md).

   1. Working in your local specification file, add the `shards` stanza to the capture or materialization specification:

    ```yaml
    myOrg/marketing/leads:
      shards:
        logLevel: debug
      endpoint:
        {}
    ```
   2. Finish the workflow as described, re-publishing the task.

[Learn more about working with logs](../../reference/working-logs-stats.md)