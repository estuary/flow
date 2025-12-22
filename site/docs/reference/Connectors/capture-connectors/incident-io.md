# Incident.io

This connector captures data from Incident.io into Estuary collections.

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/source-incident-io:dev`](https://ghcr.io/estuary/source-incident-io:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Incident.io API:

* [catalog entries](https://api-docs.incident.io/tag/Catalog-V3#operation/Catalog%20V3_ListEntries)
* [catalog resources](https://api-docs.incident.io/tag/Catalog-V3#operation/Catalog%20V3_ListResources)
* [catalog types](https://api-docs.incident.io/tag/Catalog-V3#operation/Catalog%20V3_ListTypes)
* [custom fields](https://api-docs.incident.io/tag/Custom-Fields-V2#operation/Custom%20Fields%20V2_List)
* [incident attachments](https://api-docs.incident.io/tag/Incident-Attachments-V1#operation/Incident%20Attachments%20V1_List)
* [incident roles](https://api-docs.incident.io/tag/Incident-Roles-V2#operation/Incident%20Roles%20V2_List)
* [incident statuses](https://api-docs.incident.io/tag/Incident-Statuses-V1)
* [incident timestamps](https://api-docs.incident.io/tag/Incident-Timestamps-V2)
* [incident types](https://api-docs.incident.io/tag/Incident-Types-V1)
* [incidents](https://api-docs.incident.io/tag/Incidents-V2)
* [severities](https://api-docs.incident.io/tag/Severities-V1)
* [users](https://api-docs.incident.io/tag/Users-V2#operation/Users%20V2_List)


By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

* An Incident.io account.
* An Incident.io [API key](https://app.incident.io/settings/api-keys)

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Incident.io source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication Method | Set to `Private App Credentials`. | string | Required |
| **`/credentials/access_token`** | API Key | Your Incident.io API key. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Resource in Incident.io from which collections are captured. | string | Required |
| `/interval` | Interval | Interval between data syncs | string | PT5M |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-incident-io:dev
        config:
          credentials:
            access_token: <secret>
            credentials_title: Private App Credentials
    bindings:
      - resource:
          name: incidents
        target: ${PREFIX}/incidents
```
