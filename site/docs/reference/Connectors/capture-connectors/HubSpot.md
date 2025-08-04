
# Hubspot

This connector captures data from a Hubspot account.

Estuary offers a in-house real time version of this connector. For more information take a look at our [HubSpot Real-Time](/reference/Connectors/capture-connectors/hubspot-real-time.md) docs.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-hubspot:dev`](https://ghcr.io/estuary/source-hubspot:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

By default, each resource associated with your Hubspot account is mapped to a Flow collection through a separate binding.

The following data resources are supported for all subscription levels:

* [Campaigns](https://developers.hubspot.com/docs/methods/email/get_campaign_data)
* [Companies](https://developers.hubspot.com/docs/api/crm/companies)
* [Contact Lists](http://developers.hubspot.com/docs/methods/lists/get_lists)
* [Contacts](https://developers.hubspot.com/docs/methods/contacts/get_contacts)
* [Contacts List Memberships](https://legacydocs.hubspot.com/docs/methods/contacts/get_contacts)
* [Deal Pipelines](https://developers.hubspot.com/docs/methods/pipelines/get_pipelines_for_object_type)
* [Deals](https://developers.hubspot.com/docs/api/crm/deals)
* [Email Events](https://developers.hubspot.com/docs/methods/email/get_events)
* [Engagements](https://legacydocs.hubspot.com/docs/methods/engagements/get-all-engagements)
* [Engagements Calls](https://developers.hubspot.com/docs/api/crm/calls)
* [Engagements Emails](https://developers.hubspot.com/docs/api/crm/email)
* [Engagements Meetings](https://developers.hubspot.com/docs/api/crm/meetings)
* [Engagements Notes](https://developers.hubspot.com/docs/api/crm/notes)
* [Engagements Tasks](https://developers.hubspot.com/docs/api/crm/tasks)
* [Forms](https://developers.hubspot.com/docs/api/marketing/forms)
* [Form Submissions](https://legacydocs.hubspot.com/docs/methods/forms/get-submissions-for-a-form)
* [Line Items](https://developers.hubspot.com/docs/api/crm/line-items)
* [Owners](https://developers.hubspot.com/docs/methods/owners/get_owners)
* [Products](https://developers.hubspot.com/docs/api/crm/products)
* [Property History](https://legacydocs.hubspot.com/docs/methods/contacts/get_contacts)
* [Quotes](https://developers.hubspot.com/docs/api/crm/quotes)
* [Subscription Changes](https://developers.hubspot.com/docs/methods/email/get_subscriptions_timeline)
* [Tickets](https://developers.hubspot.com/docs/api/crm/tickets)
* [Ticket Pipelines](https://developers.hubspot.com/docs/api/crm/pipelines)

The following data resources are supported for pro accounts (set **Subscription type** to `pro` in the [configuration](#endpoint)):

* [Feedback Submissions](https://developers.hubspot.com/docs/api/crm/feedback-submissions)
* [Marketing Emails](https://legacydocs.hubspot.com/docs/methods/cms_email/get-all-marketing-email-statistics)
* [Workflows](https://legacydocs.hubspot.com/docs/methods/workflows/v3/get_workflows)

## Prerequisites

There are two ways to authenticate with Hubspot when capturing data: using OAuth2 or with a private app access token.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the access token method is the only supported method using the command line.

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
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Hubspot source connector.

### Properties

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
| **`/stream`** | Data resource | Name of the data resource. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-hubspot:dev
          config:
            credentials:
              credentials_title: Private App Credentials
              access_token: <secret>
      bindings:
        - resource:
            stream: companies
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}
```
Your configuration will have many more bindings representing all supported [resources](#supported-data-resources)
in your Hubspot account.

[Learn more about capture definitions.](../../../concepts/captures.md)
