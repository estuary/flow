# HubSpot ( Real-Time )

This connector captures data from HubSpot into Flow collections.

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

There are two ways to authenticate with HubSpot when capturing data: using OAuth2, or with a private app access token.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app.

### Using OAuth2 to authenticate with HubSpot in the Flow web app

* A HubSpot account

### Configuring the connector specification manually

* A HubSpot account

* The access token for an appropriately configured [private app](https://developers.hubspot.com/docs/api/private-apps) on the Hubspot account.

#### Setup

To create a private app in HubSpot and generate its access token, do the following.

1. Ensure that your HubSpot user account has [super admin](https://knowledge.hubspot.com/settings/hubspot-user-permissions-guide#super-admin) privileges.

2. In HubSpot, create a [new private app](https://developers.hubspot.com/docs/api/private-apps#create-a-private-app).

   1. Name the app "Estuary Flow," or choose another name that is memorable to you.

   2. Grant the new app **Read** access for all available scopes.

   3. Copy the access token for use in the connector configuration.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the HubSpot Real-Time connector.

#### Endpoint

The following properties reflect the access token authentication method.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Private Application | Authenticate with a private app access token | object | Required |
| **`/credentials/access_token`** | Access Token | HubSpot Access token. | string | Required |
| **`/credentials/credentials_title`** | Credentials | Name of the credentials set | string | Required, `"Private App Credentials"` |

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
          credentials_title: Private App Credentials
          access_token: <secret>
    bindings:
      - resource:
          name: companies
        target: ${PREFIX}/${COLLECTION_NAME}
```