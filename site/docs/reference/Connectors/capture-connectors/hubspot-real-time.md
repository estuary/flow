# HubSpot ( Real-Time )

This connector captures data from HubSpot into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-hubspot-native:dev`](https://ghcr.io/estuary/source-hubspot-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported HubSpot Resources

The connector automatically discovers bindings for the following HubSpot resources:

* [Companies](https://developers.hubspot.com/docs/api/crm/companies)
* [Contacts](https://developers.hubspot.com/docs/api/crm/contacts)
* [Custom Objects](https://developers.hubspot.com/docs/api/crm/crm-custom-objects)
* [Deal Pipelines](https://developers.hubspot.com/beta-docs/guides/api/crm/pipelines)
* [Deals](https://developers.hubspot.com/docs/api/crm/deals)
* [Engagements](https://developers.hubspot.com/docs/api/crm/engagements)
* [Email Events](https://developers.hubspot.com/docs/methods/email/get_events)
* [Line Items](https://developers.hubspot.com/beta-docs/guides/api/crm/objects/line-items)
* [Owners](https://developers.hubspot.com/beta-docs/reference/api/crm/owners/v2)
* [Products](https://developers.hubspot.com/beta-docs/guides/api/crm/objects/products)
* [Properties](https://developers.hubspot.com/docs/api/crm/properties)
* [Tickets](https://developers.hubspot.com/docs/api/crm/tickets)

## Prerequisites

OAuth2 is used to authenticate the connector with HubSpot. A HubSpot account is required for the OAuth2 authentication process.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the HubSpot Real-Time connector.

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Credentials | OAuth2 credentials | object | Required |
| **`/credentials/credentials_title`** | Credentials | Name of the credentials set | string | Required, `"OAuth Credentials"` |
| **`/credentials/client_id`** | OAuth Client ID | The OAuth app's client ID. | string | Required |
| **`/credentials/client_secret`** | OAuth Client Secret | The OAuth app's client secret. | string | Required |
| **`/credentials/refresh_token`** | Refresh Token | The refresh token received from the OAuth app. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string |          |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-hubspot-native:dev
        config:
          client_id: <secret>
          client_secret: <secret>
          credentials_title: OAuth Credentials
          refresh_token: <secret>
    bindings:
      - resource:
          name: companies
        target: ${PREFIX}/${COLLECTION_NAME}
```