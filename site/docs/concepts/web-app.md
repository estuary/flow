---
sidebar_position: 6
---

# Web application

Flow's web application is at [dashboard.estuary.dev](https://dashboard.estuary.dev).

It's the central, low-code environment for creating, managing, and monitoring Data Flows.

## When to use the web app

The web app and [flowctl](./flowctl.md) are designed to work together as a complete platform.
You can use either, or both, to work on your Data Flows, depending on your preference.

With the Flow web app, you can perform most common workflows, including:

* Creating end-to-end Data Flows: **capturing** data from source systems and **materializing** it to destinations.
* Creating, viewing, and editing individual captures and materializations.
* Viewing data **collections**.
* Viewing users and permissions.
* Authenticating with the flowctl CLI.

Some advanced workflows, like granting or revoking permissions and transforming data with **derivations**, aren't available in the web app.

Even if you prefer the command line or plan to perform a task that's only available through flowctl, we recommend you begin your work in the web app;
it provides a quicker and easier path to create captures and materializations. You can then switch to flowctl to continue working.

## Signing in

You use either a Google or GitHub account to sign into Flow.

If you've never used Flow before, you'll be prompted to register before being issued a trial account.
If you want to use Flow for production workflows or collaborate with team members, you'll need an organizational account.
[Contact Estuary](mailto:support@estuary.dev) to create a new organizational account or join an existing organization.

SCREENSHOTS AND UPDATES HERE

## Navigating the web app

When you log into the web app, you land on the **Welcome** page.
There are four additional pages visible as tabs in the side navigation: **Captures**, **Collections**, **Materializations**, and **Admin**.

The order of the tabs mirrors the order of a basic Data Flow:

import Mermaid from '@theme/Mermaid';

<Mermaid chart={`
	graph LR;
		Capture-->Collection;
        Collection-->Materialization;
`}/>

While you may choose to [use the tabs in this sequence](../guides/create-dataflow.md), it's not necessary.
All Flow entities exist individually, outside of the context of complete Data Flow.
You can use the different pages in the web app to monitor and manage your items in a number of other ways, as described below.

## Captures page

The **Captures** pages shows you a table of existing Flow [captures](./captures.md) to which you have [access](../reference/authentication.md).
The **New Capture** button is also visible.
You use the table to monitor your captures.

SCREENSHOT HERE>>>> Show header and first two rows, one checkbox checked. Numbered as indicated in text.

**A:** Select all or deselect all.

**B:** Enable, Disable, and Delete buttons. These actions will be applied to the selected table rows. Choose **Disable** to temporarily pause the flow of data, **Enable** to resume, and **Delete** to permanently remove the capture(s).

**C:** Materialize button. When you click this button, you're directed to the **Create materializations** page.
All the collections of the selected capture(s) will be added to the materialization.

**D:** Filter captures by name.

**E:** Shard status indicator. Shows the status of the task [shard](./advanced/shards.md) that backs this capture.

* **Primary (Green)**: Data is actively flowing through the capture.
* **Pending (Yellow)**: The capture is attempting to re-connect. Often, you'll see this after you re-enable the capture as Flow backfills historical data.
* **Failed (Red)**: The capture has failed with an unrecoverable error.
* **Disabled (White)**: The capture is disabled.
* **Unknown (Black)**: The web app is unable to determine shard status. Usually, this is due to a temporary connection error.

**F:** Capture name. The full name is shown, including all [prefixes](./catalogs.md#namespace).

**G:** Capture type. The icon shows the type of source system data is captured from.

**H:** Associated collections. The **Writes to** column shows all the collections to which the capture writes data. For captures with a large number of collections, hover over this column and scroll to view the full list.

**I:** Publish time. Hover over this value to see the exact time the capture was first published.

**J:** Options. Choose to **View Details** or **Edit Specification**.

### Detail view

When you click **View Details** for a capture, the **Status** and **Specification** viewers are revealed.

The **Status** window shows the full identifier of the shard that backs your capture. If there's an error, you can view its logs.

In the **Specification** window, you can view the specification of the capture itself, as well as each collection to which it writes.
Select a specification from the **Files** list to view the JSON.

### Editing captures and collections

When you click **Edit specification** for a capture, you're taken to the **Edit Capture** page.

This page is similar to the [**Create Capture**](#creating-a-capture) page as it was filled out just before the capture was published.

To edit a capture or its collections:

1. Edit the connection to the source system, if desired. You can either update fields in the **Endpoint Configuration** section or manually update the JSON in the **Specification Editor**.

2. Use the **Collection Selector** to add or remove collections from the capture, if desired.

3. Click **Discover Endpoint.**

   Collection specifications become editable.

4. Use the **Specification Editor** to edit collection specifications, if desired.

5. Click **Save and Publish**.

Editing a capture only affects how it will work going forward.
Data that was captured before editing will reflect the original configuration.

It's important to be mindful of how your edits will effect downstream processes. For more information on the implications of editing, see the [reference documentation](../reference/editing.md).

### Creating a capture

When you click **Create Capture**, you're taken to the Create Capture page.
In the first view, all available capture connectors are displayed.

Select the tile of the system from which you want to capture data to show the full capture form.
The form details are specific to the connector you chose.

For detailed steps to create a capture, see the [guide](../guides/create-dataflow.md#create-a-capture).

After you successfully publish a capture, you're given the option to materialize the collections you just captured.
You can proceed to the materialization, or opt to exit to a different page of the web app.

## Collections page

The **Collections** page shows a read-only table of [collections](./collections.md) to which you have access.
You can view each collection's specification and see a sample of its data.
This can help you verify that collection data was captured as expected and that you'll be able to materialize it how you want, and troubleshoot it necessary.

To reveal the **Specification** and **Data Preview** sections, expand **Details** next to a collection name.

>>>>>>SCREEnshot

The **Specification** section shows the collection specification as JSON in a read-only editor.
(If you need to modify a collection, edit the [capture](#editing-captures) that it came from.)

The **Data Preview** section shows a sample of collection [documents](./collections.md#documents): the individual JSON files that comprise the collection.
Documents are organized by their collection key value. Click a key from the list to view its document.

## Materializations page

The **Materializations** page shows you a table of existing Flow [materializations](./materialization.md) to which you have [access](../reference/authentication.md).
The **New Materialization** button is also visible.

You use the table to monitor your materializations. It's nearly identical to the table on the [Captures page](#captures-page), with a few exceptions.

ANOTHER SCREENSHOT HERE

**A:** Select all or deselect all.

**B:** Enable, Disable, and Delete buttons. These actions will be applied to the selected table rows. Choose **Disable** to temporarily pause the flow of data, **Enable** to resume, and **Delete** to permanently remove the materialization(s).

**C:** Filter materializations by name.

**D:** Shard status indicator. Shows the status of the task [shard](./advanced/shards.md) that backs this materialization.

* **Primary (Green)**: Data is actively flowing through the materialization.
* **Pending (Yellow)**: The materialization is attempting to re-connect. Often, you'll see this after you re-enable the materialization as Flow backfills historical data.
* **Failed (Red)**: The materialization has failed with an unrecoverable error.
* **Disabled (White)**: The materialization is disabled.
* **Unknown (Black)**: The web app is unable to determine shard status. Usually, this is due to a temporary connection error.

**E:** Materialization name. The full name is shown, including all [prefixes](./catalogs.md#namespace).

**F:** Materialization type. The icon shows the type of destination system data is materialized to.

**G:** Associated collections. The **Reads from** column shows all the collections from which the materialization reads data. For materializations with a large number of collections, hover over this column and scroll to view the full list.

**H:** Publish time. Hover over this value to see the exact time the materialization was first published.

**I:** Options. Choose to **View Details** or **Edit Specification**

### Detail view

When you click **View Details** for a materialization, the **Status** and **Specification** viewers are revealed.

The **Status** window shows the full identifier of the shard that backs your materialization. If there's an error, you can view its logs.

In the **Specification** window, you can view the specification of the materialization itself, as well as each collection from which it reads.
Select a specification from the **Files** list to view the JSON.

### Editing materializations

When you click **Edit specification** for a materialization, you're taken to the **Edit Materialization** page.

This page is similar to the [**Create Materialization**](#creating-a-materialization) page as it was filled out just before the materialization was published.

To edit a materialization:

1. Edit the connection to the destination system, if desired. You can either update fields in the **Endpoint Configuration** section or manually update the JSON in the **Specification Editor**.

2. Use the **Collection Selector** to add or remove collections from the materialization, if desired.

3. Click **Discover Endpoint.**

4. Click **Save and Publish**.

Editing a materialization only affects how it will work going forward.
Data that was materialized before editing will reflect the original configuration.

It's important to be mindful of how your edits will effect downstream processes. For more information on the implications of editing, see the [reference documentation](../reference/editing.md).

### Creating a materialization

There are three ways to begin creating a materialization:

* Clicking **New Materialization** on the Materializations page.
* Selecting one or more captures from the Captures page and clicking **Materialize**.
* Clicking **Materialize Collections** immediately after publishing a capture.

When you initiate the workflow in any of these ways, all available materialization connectors are displayed.
Select a connector to reveal the full form with configuration options specific to your desired destination.

Fill out the **Endpoint Config** form and use the **Collection Selector** to map Flow collections to
resources in the destination system.
Note that if you entered the workflow from the Captures page or after publishing a capture, collections will be pre-populated for you.

For detailed steps to create a materialization, see the [guide](../guides/create-dataflow.md#create-a-materialization).

## Admin page

On the **Admin** page, you can view users' capabilities, view a complete list of connectors, and obtain an access token to authenticate with flowctl.

The **Users** tab shows you all provisioned capabilities on objects to which you also have access.
Each capability has its own row, so a given user may have multiple rows.

For example, if you had read access to `foo/` and write access to `bar/`, you'd have a separate table row for each of these capabilities.
If users Alice, Bob, and Carol each had write access on `foo/`, you'd see three more table rows representing these capabilities.

Use the search box to filter by username or object.

[Learn more about capabilities and access.](../reference/authentication.md)

The **Connectors** tab offers a complete view of all connectors that are currently available through the web application, including both capture and materialization connectors.
If a connector you need is missing, you can request it.

The **API** tab provides the access token required to [authenticate with flowctl](../reference/authentication.md#authenticating-flow-using-the-cli).
