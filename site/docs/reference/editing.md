---
sidebar_position: 1
---

# Editing considerations

You can edit the components of your Data Flows in the Flow web app and with flowctl, but before you do so, it's important to consider the implications of your changes.

Though Flow entities like captures, collections, and materializations are discrete components, they depend on one another to form complete Data Flows. Changing a configuration or a name can have adverse effects downstream.

As you edit, you'll also have to account for any updates to the configuration for the connector used.

## How to edit Flow entities

In the Flow web app, you can edit captures, captured collections, and materializations.

* [Editing captures and their collections](../concepts/web-app.md#editing-captures-and-collections)
* [Editing materializations](../concepts/web-app.md#editing-materializations)

With flowctl, you can edit captures, materializations, collections, derivations, and tests.
You do this by pulling the desired specification to a local **draft**, editing, and re-publishing.

* [Working with drafts in flowctl](../concepts/flowctl.md#working-with-drafts)

## Endpoint configuration changes

A common reason to edit a capture or materialization to fix a broken endpoint configuration:
for example, if a database was moved to a different port.
Changes that prevent Flow from finding the source system immediately cause the capture or materialization to fail.

TODO FACT CHECK THIS:

By contrast, certain credential changes might not cause issues *unless* you attempt to edit the capture or materialization.
Because Flow tasks run continuously, the connector doesn't have to re-authenticate and an outdated credential won't cause failure.
Editing, however, requires the task to re-start, so you'll need to provide current credentials to the endpoint configuration.
Before editing, take note of any changed credentials, even if the task is still running successfully.

## Managing connector updates

Connectors are updated periodically. In some cases, required fields are added or removed.
When you edit a capture or materialization, you'll need to update the configuration to comply with the current connector version.

To see check if a connector has been updated:

* Go to the **Admin** tab and view the list of connectors. Each tile shows the date it was last updated.
* Check the connector's [documentation](./Connectors/README.md). Pertinent updates, if any, are noted in the **Changelog**.

## Considerations for name changes

You're not able to change the name of a capture or materialization after you create it.

THE FOLLOWING IS VERY SPECULATIVE; FACT CHECK/TEST

It is possible to change collection names (by editing a capture) and destination resource names (by editing a materialization.
You should avoid doing so unless you want to route future data to a new location.

* If you change a collection name while editing a capture, the original collection will continue to exist (???), but the new collection will be backfilled with historical data.
  You'll need to edit any downstream derivations and materializations to reflect the new collection name.

* If you change a destination resource name while editing a materialization (for instance, a database table), a new resource with that name will be created and the old resource will continue to exist.
  Historical data will *not* be backfilled into the new resource.

