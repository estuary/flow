# Hubspot ( Native )

This connector captures data from Hubspot into Flow collections. 

## Supported data resources

* [Companies](https://developers.hubspot.com/docs/api/crm/companies)
* [Contacts](https://developers.hubspot.com/docs/api/crm/contacts)
* [Deals](https://developers.hubspot.com/docs/api/crm/deals)
* [Engagements](https://developers.hubspot.com/docs/api/crm/engagements)
* [Contact Lists](https://legacydocs.hubspot.com/docs/methods/lists/get_lists) 
* [Contact Lists Subscriptions](https://legacydocs.hubspot.com/docs/methods/contacts/get_contacts)
* [Campaigns](https://legacydocs.hubspot.com/docs/methods/email/get_campaign_data)
* [Subscription Changes](https://developers.hubspot.com/docs/methods/email/get_subscriptions_timeline)
* [Email Events](https://developers.hubspot.com/docs/methods/email/get_events)
* [Ticket Pipelines](https://developers.hubspot.com/docs/methods/pipelines/get_pipelines_for_object_type)
* [Deal Pipelines](https://developers.hubspot.com/docs/methods/pipelines/get_pipelines_for_object_type)
* [Engagements Calls](https://developers.hubspot.com/docs/api/crm/calls)
* [Engagements Emails](https://developers.hubspot.com/docs/api/crm/email)
* [Engagements Meetings](https://developers.hubspot.com/docs/api/crm/meetings)
* [Engagements Notes](https://developers.hubspot.com/docs/api/crm/notes)
* [Engagements Tasks](https://developers.hubspot.com/docs/api/crm/tasks)
* [Goal Targets](https://developers.hubspot.com/docs/api/crm/goals)
* [Line Items](https://developers.hubspot.com/docs/api/crm/line-items)
* [Products](https://developers.hubspot.com/docs/api/crm/products)
* [Tickets](https://developers.hubspot.com/docs/api/crm/tickets)
* [Emails Subscriptions](https://developers.hubspot.com/docs/api/marketing-api/subscriptions-preferences)
* [Marketing Forms](https://developers.hubspot.com/docs/api/marketing/forms)
* [Owners](https://developers.hubspot.com/docs/api/crm/owners)
* [Properties](https://developers.hubspot.com/docs/api/crm/properties)

The following data resources are supported for pro accounts:

* [Feedback Submissions](https://developers.hubspot.com/docs/api/crm/feedback-submissions)
* [Marketing Emails](https://developers.hubspot.com/docs/api/marketing/marketing-email)
* [Workflows](https://legacydocs.hubspot.com/docs/methods/workflows/v3/get_workflows)

## Prerequisites

There are two ways to authenticate with Hubspot when capturing data: using OAuth2, or with a private app access token.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;

### Using OAuth2 to authenticate with Hubspot in the Flow web app

* A Hubspot account

### Configuring the connector specification manually

* A Hubspot account

* The access token for an appropriately configured [private app](https://developers.hubspot.com/docs/api/private-apps) on the Hubspot account.

#### Setup

To create a private app in Hubspot and generate its access token, do the following.

1. Ensure that your Hubspot user account has [super admin](https://knowledge.hubspot.com/settings/hubspot-user-permissions-guide#super-admin) privileges.

2. In Hubspot, create a [new private app](https://developers.hubspot.com/docs/api/private-apps#create-a-private-app).

   1. Name the app "Estuary Flow," or choose another name that is memorable to you.

   2. Grant the new app **Read** access for all available scopes.

   3. Copy the access token for use in the connector configuration.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the hubspot-native connector.

#### Endpoint

The following properties reflect the access token authentication method.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Private Application | Authenticate with a private app access token | object | Required |
| **`/credentials/access_token`** | Access Token | HubSpot Access token. | string | Required |
| **`/credentials/credentials_title`** | Credentials | Name of the credentials set | string | Required, `"Private App Credentials"` |
| **`/start_date`** | Start Date | UTC date and time in the format 2017-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |
| `/subscription_type` | Your HubSpot account subscription type | Some streams are only available to certain subscription packages, we use this information to select which streams to pull data from. | string | `"starter"` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| **`/interval`** | Interval | Interval between data syncs | string | Required |

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
          interval: PT420S
        target: ${PREFIX}/${COLLECTION_NAME}
```


