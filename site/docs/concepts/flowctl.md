---
description: >-
  Learn how to use the command-line interface to build, test, deploy, and run
  catalogs
---

# flowctl

The `flowctl` command-line interface is used to test, deploy, and run Flow catalogs. It is the one and only Flow binary that you need to deal with, so distribution and upgrades are all simple. For now, the [Docker image](https://quay.io/estuary/flow:dev) is the only official release artifact, but `flowctl` will also be released as a statically linked binary in the near future.

`flowctl` includes a number of sub-commands. The most common sub-commands you'll use are `discover`, `develop`, `apply`,  and `test`. We'll talk a bit about each of these in order, as each one builds on the previous.

* `discover` auto-creates a catalog spec given an open-source connector and a data source. It’s an assisted way to quickly capture data and expedite the initial Flow deployment.&#x20;
* `develop`\*\* starts a small local Flow runtime and applies your catalog spec to it. In essence, you are using Flow as it would be used in production, but with locally running captures and materializations.

{% hint style="warning" %}
\*\* The commands used to run Flow locally are currently undergoing enhancements at a rapid pace. Due to this pace, not all updates are reflected in the documentation. As we fine-tune the ideal Flow onboarding experience, you may see unexpected behavior while attempting to use `flowctl develop`.

You can reach our team via [email](<mailto:info@estuary.dev >) for more information.&#x20;


{% endhint %}

* `apply` persists your catalog to a previously set-up Flow deployment for production updates. You pass the address of the etcd cluster and the Flow reactor in your deployment as arguments.
* `test` is used to run your catalog tests and ensure that their output matches your expectations given an input. To do so, it starts the same way as `develop`, and then runs each of your tests against that environment.

{% hint style="info" %}
For help while using the`flowctl`CLI, use `-h` or `--help` after the command or subcommand. For example, `flowctl -h` or `flowctl develop --help`.
{% endhint %}
