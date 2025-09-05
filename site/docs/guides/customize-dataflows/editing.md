---
slug: /reference/editing/
---

# Editing Considerations

You can edit the components of your Data Flows in the Flow web app and with flowctl, but before you do so, it's important to consider the implications of your changes.

Though Flow entities like captures, collections, and materializations are discrete components, they depend on one another to form complete Data Flows. Changing a configuration or a name can have adverse effects downstream.

As you edit, you'll also have to account for any updates to the configuration for the connector used.

## How to edit Flow entities

In the Flow web app, you can edit captures and materializations, and use the **Schema Inference** tool to edit collection schemas.

* [Editing captures and associated collections](/guides/edit-data-flows/#edit-a-capture)
* [Editing materializations and associated collections](/guides/edit-data-flows/#edit-a-materialization)

With flowctl, you can edit captures, materializations, collections, derivations, and tests.
You do this by pulling the desired specification locally, editing, and re-publishing.

* [Editing with flowctl](/concepts/flowctl/#editing-data-flows-with-flowctl)

## Endpoint configuration changes

A common reason to edit a capture or materialization is to fix a broken endpoint configuration:
for example, if a database is now accessed through a different port.
Changes that prevent Flow from finding the source system immediately cause the capture or materialization to fail.

By contrast, certain credential changes might not cause issues *unless* you attempt to edit the capture or materialization.
Because Flow tasks run continuously, the connector doesn't have to re-authenticate and an outdated credential won't cause failure.
Editing, however, requires the task to re-start, so you'll need to provide current credentials to the endpoint configuration.
Before editing, take note of any changed credentials, even if the task is still running successfully.

## Managing connector updates

Connectors are updated periodically. In some cases, required fields are added or removed.
When you edit a capture or materialization, you'll need to update the configuration to comply with the current connector version.
You may need to change a property's formatting or add a new field.

Additionally, certain updates to capture connectors can affect the way available collections are named.
After editing, the connector may map a data resource to new collection with a different name.

For example, say you have capture that writes to a collection called `post/fruity_pebbles/nutritionFacts`.
You begin to edit the capture using the latest version of the connector.
The connector detects the same set of nutrition facts data,
but maps it to a collection called `post/fruity_pebbles/nutrition-facts`.
If you continue to publish the edited capture, both collections will persist,
but new data will be written to the new collection.

Before editing, check if a connector has been updated:

* Go to the **Admin** tab and view the list of connectors. Each tile shows the date it was last updated.
* Check the connector's [documentation](/reference/Connectors). Pertinent updates, if any, are noted in the **Changelog** section.

## Considerations for name changes

You're not able to change the name of a capture or materialization after you create it.
You're also unable to manually change the names of collections;
however, connector updates can cause collection names to change, as discussed above.

It *is* possible to manually change the names of destination resources (tables or analogous data storage units to which collections are written) when editing a materialization.
You should avoid doing so unless you want to route future data to a new location.

If you do this, a new resource with that name will be created and the old resource will continue to exist.
Historical data may not be backfilled into the new resource, depending on the connector used.

