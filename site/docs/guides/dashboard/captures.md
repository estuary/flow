---
sidebar_position: 1
---

# Sources

The **Sources** page displays a table of existing [captures](/concepts/captures) to which you have [access](/reference/authentication).
You can also create a capture with the **New Capture** button.

![Annotated captures table](<./dashboard-images/capture-page.png>)

**1:** Select all or deselect all.

**2:** Enable, Disable, and Delete buttons. These actions will be applied to the selected table rows. Choose **Disable** to temporarily pause the flow of data, **Enable** to resume, and **Delete** to permanently remove the capture(s).

**3:** Materialize button. When you click this button, you're directed to the **Create Materializations** page.
All the collections of the selected capture(s) will be added to the materialization.

**4:** Filter captures. Type a catalog prefix, unique capture name, or connector name to return captures that match your query.

Capture names follow the pattern `prefix/unique-identifier/connector-name`, with `prefix` supporting multiple layers of nesting.
You can search for any part of this full capture name. You can also use the `*` wildcard.
For example, if you have a capture called `acmeCo/logistics/anvil-locations/source-postgres`,
you can find it by filtering for `acmeCo*source-postgres`.

**5:** Status indicator. Shows the status of the primary task [shard](/concepts/advanced/shards) that backs this capture.

* **Primary (Green)**: Data is actively flowing through the capture.
* **Pending (Yellow)**: The capture is attempting to re-connect. Often, you'll see this after you re-enable the capture as Estuary backfills historical data.
* **Failed (Red)**: The capture has failed with an unrecoverable error.
* **Disabled (Hollow circle)**: The capture is disabled.
* **Unknown (Black when app is in light mode; white when app is in dark mode)**: The web app is unable to determine shard status. Usually, this is due to a temporary connection error.

**6:** Capture name. The full name is shown, including all [prefixes](/concepts/catalogs/#namespace). It also links to the [capture details page](#capture-details-page).

**7:** Capture type. The icon shows the type of source system data is captured from.

**8:** Capture [statistics](/concepts/advanced/logs-stats/#statistics). The **Data Written** column shows the total amount of data, in bytes and in [documents](/concepts/collections/#documents),
that the capture has written to its associated collections within a configurable time interval.
Click the time interval in the header to select from **Today**, **Yesterday**, **This Week**, **Last Week**, **This Month**, **Last Month**, or **All Time**.

**9:** Associated collections. The **Writes to** column shows all the collections to which the capture writes data. For captures with a large number of collections, there is a chip stating how many collections are hidden. Clicking on this will allow you to hover over this column and scroll to view the full list. These also link to the details page of the collection.

**10:** Publish time. Hover over this value to see the exact time the capture was last published.

**11:** Edit button. Click to edit the capture.

## Editing captures and collections

When you click **Edit** for a capture, you're taken to the **Edit Capture** page.

This page is similar to the [**Create Capture**](#creating-a-capture) page.
It comes pre-filled with the capture's current configuration, with the exception of sensitive SOPS-encrypted credentials.

For detailed steps to edit a capture, see the [guide](/guides/edit-data-flows/#edit-a-capture).

## Creating a capture

When you click **Create Capture**, you're taken to the Create Capture page.
In the first view, all available capture connectors are displayed.

Select the tile of the system from which you want to capture data to show the full capture form.
The form details are specific to the connector you chose.

For detailed steps to create a capture, see the [guide](/guides/create-dataflow/#create-a-capture).

After you successfully publish a capture, you're directed to the [capture details page](#capture-details-page) where you can review the capture's status and start materializing its associated collections.

## Capture Details page

When you click on the **name** of a capture on the captures page, you will be taken to the capture details page to view data stats, sharding information, and general details.

### Overview tab

![Annotated capture details page](<./dashboard-images/capture-details-overview.png>)

**1:** The full name of the capture.

**2:** Edit and Materialize buttons. Modify your capture or create a materialization to send the capture's associated data collections to a destination.

**3:** Page tabs. Switch between the Overview, [Alerts](#alerts-tab), [Spec](#spec-tab), [History](#history-tab), and [Logs](#logs-tab) tabs for detailed information in each category.

**4:** Usage chart filters. Click the time interval to select from **6 hours**, **12 hours**, **24 hours**, **48 hours**, **30 days**, or **Year**.
You can also choose to view usage in **Data Written** (in bytes) or **[Docs](/concepts/collections/#documents) Written**.

**5:** Usage chart with capture [statistics](/concepts/advanced/logs-stats/#statistics).
This chart displays the data written by the capture in the chosen timeframe. Hover over a section to see specific details for that timeframe.
The most recent segment will automatically update every 15 seconds with the latest data and docs.

**6:** Details section. This contains information about the capture, such as the connector type, the data plane being used, and create/update timestamps.

**7:** Connector status. The current status of the connector. This may indicate backfill progress or streaming status.

**8:** Associated collections. Shows all the collections to which the capture writes data.
These link to the collection's [detail page](/guides/dashboard/collections/#collection-details-page).

The Overview tab also lists additional information, such as:

* A **Shard Information** section. This displays the full identifier of the shard(s) that back your capture. If there's an error, you'll see an alert identifying the failing shard(s).

* Any associated **Endpoints**. Webhook captures will list the public endpoint to provide to the source system.

### Alerts tab

The capture's Alerts tab breaks down into three main sections.

**Notification Settings**

This section allows you to configure a **Data Movement** alert for the capture.

You can choose a time interval between **1 Hour** and **7 Days**. If data has not been processed within this timeframe, a notification will be sent to anyone subscribed to the alert.

If you are not subscribed, this section will display an info box with the option to **Click Here** to subscribe.

**Active Alerts**

The Active Alerts section displays any currently firing alerts.
This includes details on the [alert type](/reference/notifications/#alert-types), when the alert fired, any recipient emails, and error messages or additional details related to why the alert fired.

**Resolved Alerts**

The Resolved Alerts section displays historical alerts.
The table provides information similar to the Active Alerts table with the addition of a resolution time for the alert.

### Spec tab

In the Spec tab, you can view the specification of the capture itself.

The spec includes the capture's endpoint configuration, auto discover settings, and associated bindings.

### History tab

The History tab provides a change history of the capture's specification.

The list of changes includes:

* The date and time the change occurred
* Who initiated the change
* Any notes related to the change; for example, noting an automatic update, such as auto discovery or a dataflow reset

Selecting a change from the list will display a **diff** to the right that highlights changes made to the specification.

### Logs tab

The Logs tab displays **task logs** for the capture.

Logs include the log level, timestamp, and log message. Select a log to expand the log message.
