---
sidebar_position: 2
---

# Collections

The **Collections** page shows a table of [collections](/concepts/collections) to which you have access. There is also a button to begin a new derivation, or transformation.

The table has nearly all of the same features as the [**Captures** table](./captures.md). These features include:

* Select or deselect the collection checkbox to use the Enable, Disable, and Delete buttons.
Choose **Disable** to temporarily pause the flow of data, **Enable** to resume, and **Delete** to permanently remove the collection(s).

* Select the collection's name to navigate to the [collection details page](#collection-details-page).
The full name is shown, including all [prefixes](/concepts/catalogs/#namespace). You may also filter collections by name to only display collections that match your query.

* View collection status with the status indicator. If the collection does not contain a [derivation](/concepts/#derivations), the indicator should show green.
In the event that the server cannot be reached, the indicator will show "Unknown" status (black in light mode and white in dark mode).

   If the collection is a derivation, the derivation's [shard](/concepts/advanced/shards) status will be indicated:

   * **Primary (Green)**: Data is actively flowing through the derivation.
   * **Pending (Yellow)**: The derivation is attempting to re-connect.
   * **Failed (Red)**: The derivation has failed with an unrecoverable error.
   * **Disabled (Hollow circle)**: The derivation is disabled.
   * **Unknown (Black when app is in light mode; white when app is in dark mode)**: The web app is unable to determine shard status. Usually, this is due to a temporary connection error.

* View collection [statistics](/concepts/advanced/logs-stats/#statistics).
Columns include data (in bytes) and [documents](/concepts/collections/#documents) that have been read in or written out by the collection.
The time interval in the header controls the range for these statistics.

* View the collection's last published date. Hover over the value to see the exact timestamp.

## Collection Details page

When you click on the **name** of a collection on the collections page, you will be taken to the collection details page.
This page includes data stats, sharding information, a preview of the collection data, and general details, similar to the [capture details page](./captures.md#capture-details-page).

### Overview tab

The collection overview breaks down into several main sections.

**Usage chart**

The usage chart contains collection [statistics](/concepts/advanced/logs-stats/#statistics).
You can select a timeframe for the data and choose between viewing usage in data processed (bytes) or [documents](/concepts/collections/#documents).

The chart displays both the amount of data read into the collection and the amount of data written out.
Hover over a section for details on that timeframe or hover over the legend to highlight either data in or out.

**Details section**

The Details section displays general information about the collection, including:

* The collection's data plane
* Create/update timestamps
* The capture that writes to the data collection, if applicable
* Any materializations that read from the data collection

**Shard information**

Derivations will include a section with shard information. This displays the full identifier of the shard(s) that back your collection along with the shard status.
If there's an error, you'll see an alert identifying the failing shard(s).

**Data preview**

The Data Preview section shows a sample of collection [documents](/concepts/collections/#documents): the individual JSON files that comprise the collection.
Documents are organized by their collection key value. Click a key from the list to view its document.

### Alerts tab

The collection's Alerts tab displays information on both active and historical alerts.

The **Active Alerts** section displays any currently firing alerts.
This includes details on the [alert type](/reference/notifications/#alert-types), when the alert fired, any recipient emails, and error messages or additional details related to why the alert fired.

The **Resolved Alerts** section displays historical alerts.
The table provides information similar to the Active Alerts table with the addition of a resolution time for the alert.

### Spec tab

The Spec tab provides the collection's specification, including its schema.
You can view the [schema](/concepts/schemas) as a table or the full specification JSON as code.

The specification includes the [collection key](/concepts/collections/#keys), field names, field data types, and additional information, such as the source data type.

:::tip
If you need to modify a collection, edit the [capture](/concepts/captures) or [derivation](/concepts/derivations) that provides its data.
:::

### History tab

The History tab provides a change history of the collection's specification.

The list of changes includes:

* The date and time the change occurred
* Who initiated the change
* Any notes related to the change; for example, noting an automatic update, such as auto discovery or a dataflow reset

Selecting a change from the list will display a **diff** to the right that highlights changes made to the specification.
