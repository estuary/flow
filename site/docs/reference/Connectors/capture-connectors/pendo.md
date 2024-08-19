# Pendo

This connector captures data from Pendo into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-pendo:dev`](https://ghcr.io/estuary/source-pendo:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.
You can find their documentation [here](https://docs.airbyte.com/integrations/sources/pendo),
but keep in mind that the two versions may be significantly different.

## Supported data resources

The following data resources are supported through the Pendo API:

* [Feature](https://engageapi.pendo.io/#75c6b443-eb07-4a0c-9e27-6c12ad3dbbc4)
* [Guide](https://engageapi.pendo.io/#4f1e3ca1-fc41-4469-bf4b-da90ee8caf3d)
* [Page](https://engageapi.pendo.io/#a53463f9-bdd3-443e-b22f-b6ea6c7376fb)
* [Report](https://engageapi.pendo.io/#2ac0699a-b653-4082-be11-563e5c0c9410)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* A Pendo account with the integration feature enabled.
* A Pendo [API key](https://app.pendo.io/admin/integrationkeys)

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Pendo source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/api_key`** | API Key | Your Pendo API key. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource in Pendo from which collections are captured. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-pendo:dev
        config:
          api_key: <secret>
    bindings:
      - resource:
          stream: Feature
          syncMode: full_refresh
        target: ${PREFIX}/Feature
      - resource:
          stream: Guide
          syncMode: full_refresh
        target: ${PREFIX}/Guide
      - resource:
          stream: Page
          syncMode: full_refresh
        target: ${PREFIX}/Page
      - resource:
          stream: Report
          syncMode: full_refresh
        target: ${PREFIX}/Report
```
