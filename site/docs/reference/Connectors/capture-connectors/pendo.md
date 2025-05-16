# Pendo

This connector captures data from Pendo into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-pendo:dev`](https://ghcr.io/estuary/source-pendo:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Pendo API:

* [Feature](https://engageapi.pendo.io/#75c6b443-eb07-4a0c-9e27-6c12ad3dbbc4)
* [Guide](https://engageapi.pendo.io/#4f1e3ca1-fc41-4469-bf4b-da90ee8caf3d)
* [Page](https://engageapi.pendo.io/#a53463f9-bdd3-443e-b22f-b6ea6c7376fb)
* [Report](https://engageapi.pendo.io/#2ac0699a-b653-4082-be11-563e5c0c9410)
* [TrackType](https://engageapi.pendo.io/#9f83f648-4fe7-45db-b30d-963679af6304)
* [Visitor](https://engageapi.pendo.io/#dd943e68-5bff-4a1a-9891-b55638ae2c3d)
* [PageEvents](https://engageapi.pendo.io/#9af41daf-e6f2-4dc2-8031-836922aad09e)
* [FeatureEvents](https://engageapi.pendo.io/#a26da609-62d0-43ea-814b-956551f2abeb)
* [TrackEvents](https://engageapi.pendo.io/#97927543-0222-42b9-93a2-0775d2c62e1e)
* [GuideEvents](https://engageapi.pendo.io/#7b6aa7b0-117d-478b-942b-c339196e636d)
* [PollEvents](https://engageapi.pendo.io/#a6ff15d6-f989-4c11-b7a7-1de0f1577306)

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
| **`/credentials/access_token`** | API Key | Your Pendo API key. | string | Required |
| `/startDate` | Replication Start Date | UTC date and time in the format "YYYY-MM-DDTHH:MM:SSZ". Data prior to this date will not be replicated. | string | 1 hour before the current time |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Resource in Pendo from which collections are captured. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-pendo:dev
        config:
          credentials:
            access_token: <secret>
    bindings:
      - resource:
          name: FeatureEvents
        target: ${PREFIX}/FeatureEvents
```
