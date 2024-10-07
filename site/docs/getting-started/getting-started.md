---
sidebar_position: 1
---

# What is Estuary Flow?

Estuary Flow is a data movement and transformation platform for the whole data team.

Flow synchronizes your systems – SaaS, databases, streaming, and more – around the same datasets, which it stores in the
cloud and updates in milliseconds. It combines the easy cross-system integration of an ELT tool and a flexible streaming
backbone,
all while remaining aware of your data's complete history.

A few examples of what you can do with Flow:

* Perform *change data capture* from MySQL tables into PostgreSQL or a cloud analytics warehouse like Snowflake or
  Databricks
* Fetch, transform, and load logs from cloud delivery networks (CDNs) into Elasticsearch or BigQuery
* Hydrate real-time analytics systems such as Tinybird or StarTree
* Instrument real-time analytics over your business events, accessible from current tools like PostgreSQL or even Google
  Sheets
* Capture and organize your data from your SaaS vendors (like Hubspot or Facebook), into a Parquet data lake

Under the hood, Flow comprises cloud-native streaming infrastructure, a powerful runtime for data processing,
and an open-source ecosystem of pluggable connectors for integrating your existing data systems.

## Get started with Flow

To start using Flow for free, visit the [dashboard](https://go.estuary.dev/dashboard) and register for free.

Start using Flow with these recommended resources.

- **[Quickstart](quickstart/quickstart.md)**: Take a look at the Quickstart to see how easy it is to set up a real-time
  data flow.

- **[High level concepts](../concepts/README.md)**: Start here to learn more about important Flow terms.

### **Other resources**

* Our [website](https://www.estuary.dev) offers general information about Flow, Estuary, and how we fit into the data
  infrastructure landscape.
* Our source code lives on [GitHub](https://github.com/estuary).

## Self-hosting Flow

The Flow runtime is available under
the [Business Source License](https://github.com/estuary/flow/blob/master/LICENSE-BSL). It's possible to self-host Flow
using a cloud provider of your choice.

:::caution Beta
Setup for self-hosting is not covered in this documentation, and full support is not guaranteed at this time.
We recommend using the [hosted version of Flow](#get-started-with-the-flow-web-application) for the best experience.
If you'd still like to self-host, refer to the [GitHub repository](https://github.com/estuary/flow) or
the [Estuary Slack](https://join.slack.com/t/estuary-dev/shared_invite/zt-86nal6yr-VPbv~YfZE9Q~6Zl~gmZdFQ).
:::
