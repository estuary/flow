---
sidebar_position: 12
---

# Web Application

Estuary's web application, or dashboard, is located at [dashboard.estuary.dev](https://dashboard.estuary.dev).

The dashboard is the central, low-code environment for creating, managing, and monitoring data flows.

## When to use the web app

The web app and the [`flowctl` CLI](./flowctl.md) are designed to work together as a complete platform.
You can use either, or both, to work on your data flows, depending on your preference.

In Estuary's dashboard, you can perform most common workflows, including:

* Creating end-to-end data flows: **capturing** data from source systems and **materializing** it to destinations.
* Creating, viewing, and editing individual captures and materializations.
* Monitoring the amount of data being processed by the system.
* Viewing data **collections**.
* Viewing users and permissions.
* Granting permissions to other users.
* Authenticating with the `flowctl` CLI.
* Managing billing details.

Some advanced workflows, like transforming data with **derivations**, require using `flowctl`.

Even if you prefer the command line or plan to perform a task that's only available through `flowctl`, we recommend you begin your work in the dashboard;
it provides a quicker and easier path to create captures and materializations. You can then switch to `flowctl` to continue working.

## Navigating the web app

When you log into the dashboard, you land on the **Welcome** page.
There are four additional pages visible as tabs in the side navigation: **Sources (captures)**, **Collections**, **Destinations (materializations)**, and **Admin**.

The order of the tabs mirrors the order of a basic data flow:

import Mermaid from '@theme/Mermaid';

<Mermaid chart={`
	graph LR;
		Capture-->Collection;
        Collection-->Materialization;
`}/>

While you may choose to [use the tabs in this sequence](../guides/create-dataflow.md), it's not necessary.
All Estuary entities exist individually, outside of the context of a complete data flow.
You can use the different pages in the dashboard to monitor and manage your items in a number of other ways.

---

### Capture pages

The main **Sources** page provides a list of existing captures. From here, you can:

* Create a new capture
* Manage existing captures, such as editing, enabling/disabling, or deleting them
* Select a capture to view its **Details** page

Each capture has a **Details** page with additional information. The Details page allows you to:

* Monitor usage
* View alerts and logs
* Inspect the capture's underlying specification and its history

[See in-depth details on navigating these pages.](/guides/dashboard/captures)

---

### Collection pages

The main **Collections** page provides a list of existing data collections. From here, you can:

* Create a draft for a new derived, or transformed, collection
* Manage existing collections, such as enabling/disabling or deleting them
* Select a collection to view its **Details** page

Each collection has a **Details** page with additional information. The Details page allows you to:

* Monitor usage
* View alerts
* Inspect the collection's underlying specification and its history

[See in-depth details on navigating these pages.](/guides/dashboard/collections)

---

### Materialization pages

The main **Destinations** page provides a list of existing materializations. From here, you can:

* Create a new materialization
* Manage existing materializations, such as editing, enabling/disabling, or deleting them
* Select a materialization to view its **Details** page

Each materialization has a **Details** page with additional information. The Details page allows you to:

* Monitor usage
* View alerts and logs
* Inspect the materialization's underlying specification and its history

[See in-depth details on navigating these pages.](/guides/dashboard/materializations)

---

### Admin pages

The **Admin** section in the dashboard provides information and settings related to your tenant.
Here, you can manage:

* Organization membership and data sharing
* Data planes and storage mappings
* Billing information
* Refresh tokens and access tokens for `flowctl`

[See in-depth details on navigating these pages.](/guides/dashboard/admin)
