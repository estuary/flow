---
sidebar_position: 1
description: Get to know Estuary Flow and learn how to get started.
slug: /
---

# Flow documentation

Estuary Flow is a tool for building, testing, and evolving pipelines that continuously capture, transform, and materialize data across all of your systems. It unifies today's batch and stream processing models so that your systems – current and future – are synchronized around the same datasets, updating in milliseconds. Use Flow to:

 * Perform *change data capture* from MySQL tables into PostgreSQL or a cloud analytics warehouse
 * Fetch, transform, and load logs from cloud delivery networks (CDNs) into Elastic Search or BigQuery
 * Instrument real-time analytics over your business events, accessible from current tools like PostgreSQL or even Google Sheets
 * Capture and organize your data from your SaaS vendors (like Salesforce or Facebook), into a Parquet data lake

You work with Flow through its intuitive web application or using the command line interface.
The system is designed so that whole teams of data stakeholders can collaborate on the same data pipelines (called **data flows** in the Flow ecosystem).
Business users and analysts can configure data flows to connect disparate systems in minutes,
and engineers can then refine those pipelines, troubleshoot, and configure complex transformations in their preferred environment.

Under the hood, Flow comprises cloud-native streaming infrastructure, a powerful runtime for data processing,
and an open-source ecosystem of pluggable connectors for integrating your existing data systems.

### Quick start

:::info Beta
Flow is in private beta.
To get started, read more about the beta program and set up a free discovery call with the Estuary team [here](https://go.estuary.dev/sign-up).
:::

**Wondering if Flow is right for you?**

If you're unsure if Flow is the right solution for your data integration needs, you can read about the technical benefits and clear comparisons with similar systems, from an engineering perspective.

* **[Who should use Flow?](overview/who-should-use-flow.md)**
* **[Comparisons with other systems](overview/comparisons.md)**

**Want to get up and running ASAP?**

There are two ways to get started with Flow: using the web application,
or self-hosting.

**Using the web application** is the recommended pathway for production data flows
and collaboration with large teams.

* Follow the guide to [create your first data flow](../guides/create-dataflow.md).

**To self-host**, you'll work directly from the Flow runtime source code, using your own development environment and storage.
Flow is available for non-commercial use and testing under the [Business Source License](https://github.com/estuary/flow/blob/master/LICENSE-BSL).
Begin by trying Flow in a local environment as described in the following documentation.

* **[Set up a development environment](getting-started/installation.md)**
* **[Complete a quick tutorial](getting-started/flow-tutorials/)**

**Looking to understand the concepts behind Flow at a deeper level?**

We recommend starting with a tutorial or guide to get acquainted with basic Flow concepts in action.
After that, read the **[Concepts](concepts/README.md)** to go deeper.

****

### **Other resources**

* Our [website](https://www.estuary.dev) offers general information about Flow, Estuary, and how we fit into the data infrastructure landscape.
* Our source code lives on [GitHub](https://github.com/estuary).

