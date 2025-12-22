# Gladly

This connector captures data from Gladly into Estuary collections.

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/source-gladly:dev`](https://ghcr.io/estuary/source-gladly:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

This connector can be used to sync the following [Event entity types](https://developer.gladly.com/rest/#tag/Events) from Gladly:

* **AGENT_AVAILABILITY**
* **AGENT_STATUS**
* **CONTACT**
* **CONVERSATION**
* **CUSTOMER**
* **PAYMENT_REQUEST**
* **TASK**

By default, each entity type is mapped to an Estuary collection through a separate binding.

## Prerequisites

To set up the Gladly source connector, you'll need a Gladly account with an [API token](https://connect.gladly.com/docs/implementation/article/get-your-api-tokens/).

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Gladly source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/organization`** | Organization | Organization to Request Data From | string | Required |
| **`/agentEmail`** | Agent Email | Agent Email Address to use for Authentication | string | Required |
| **`/apiToken`** | API Token | API Token to use for Authentication | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Name | Name of this resource | string | Required |
| `/interval` | Interval | Interval between updates for this resource | string | |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-gladly:dev
        config:
          organization:
          agentEmail:
          apiToken: <secret>
    bindings:
      - resource:
          name: AgentAvailabilityEvents
          interval: PT30S
        target: ${PREFIX}/AgentAvailabilityEvents
      {...}
```
