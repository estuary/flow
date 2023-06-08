---
sidebar_position: 1
---
# NetSuite

This connector captures data from NetSuite into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-netsuite:dev`](https://ghcr.io/estuary/source-netsuite:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

You can find their documentation [here](https://docs.airbyte.com/integrations/sources/netsuite/),
but keep in mind that the two versions may be significantly different.

## Supported data resources

Flow captures collections from any NetSuite object to which you grant access during [setup](#setup), including `Transactions`, `Reports`, `Lists`, and `Setup`.

## Setup

Create a custom role estuary-role following [these steps](https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/bridgehead_N286284.html). Any objects to which you grant the estuary-role access will be captured.

## Prerequisites

* Oracle NetSuite [account](https://system.netsuite.com/pages/customerlogin.jsp?country=US)
* Allowed access to all Account permissions options
* A new integration with token-based authentication
* A custom role with access to objects you want to capture. See [setup](#setup).
* A new user assigned to the custom role
* Access token generated for the custom role

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the NetSuite source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/realm` | Realm | Netsuite realm e.g. 2344535, as for `production` or 2344535_SB1, as for the `sandbox` | string | Required |
| `/consumer_key` | Consumer Key | Consumer key associated with your integration. | string | Required |
| `/consumer_secret` | Consumer Secret | Consumer secret associated with your integration. | string | Required |
| `/token_key` | Token Key | Access token key | string | Required |
| `/token_secret` | Token Secret | Access token secret | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your NetSuite project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-netsuite:dev
        config:
          realm: <your account id>
          consumer_key: <key>
          consumer_secret: <secret>
          token_key: <key>
          token_secret: <secret>
    bindings:
      - resource:
          stream: items
          syncMode: full_refresh
        target: ${PREFIX}/items
      {...}
```
