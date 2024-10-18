
# Intercom

This connector captures data from Intercom into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-intercom:dev`](https://ghcr.io/estuary/source-intercom:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Intercom API:

* [Admins](https://developers.intercom.com/intercom-api-reference/reference/list-admins)
* [Companies](https://developers.intercom.com/intercom-api-reference/reference/list-companies)
* [Company attributes](https://developers.intercom.com/intercom-api-reference/reference/list-data-attributes)
* [Company segments](https://developers.intercom.com/intercom-api-reference/reference/list-attached-segments-1)
* [Contacts](https://developers.intercom.com/intercom-api-reference/reference/list-contacts)
* [Contact attributes](https://developers.intercom.com/intercom-api-reference/reference/list-data-attributes)
* [Conversations](https://developers.intercom.com/intercom-api-reference/reference/list-conversations)
* [Conversation parts](https://developers.intercom.com/intercom-api-reference/reference/retrieve-a-conversation)
* [Segments](https://developers.intercom.com/intercom-api-reference/reference/list-segments)
* [Tags](https://developers.intercom.com/intercom-api-reference/reference/list-tags-for-an-app)
* [Teams](https://developers.intercom.com/intercom-api-reference/reference/list-teams)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

There are two ways to authenticate with Intercom:

* In the Flow web app, you sign in directly. You'll need the username and password associated with [a user with full permissions](https://www.intercom.com/help/en/articles/280-how-do-i-add-remove-or-delete-a-teammate) on your Intercom workspace.

* Using the flowctl CLI, you configure authentication manually. You'll need the [access token](https://developers.intercom.com/building-apps/docs/authentication-types#section-how-to-get-your-access-token) for you Intercom account.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Intercom source connector.

### Properties

#### Endpoint

The properties in the table below reflect manual authentication using the CLI. In the Flow web app,
you'll sign in directly and won't need the access token.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/access_token`** | Access token | Access token for making authenticated requests. | string | Required |
| **`/start_date`** | Start date | UTC date and time in the format 2017-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource from Intercom from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |


### Sample

The sample below reflects manual authentication in the CLI.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-intercom:dev
        config:
            access_token: <secret>
            start_date: 2022-06-18T00:00:00Z
    bindings:
      - resource:
          stream: admins
          syncMode: full_refresh
        target: ${PREFIX}/admins
      - resource:
          stream: companies
          syncMode: incremental
        target: ${PREFIX}/companies
      - resource:
          stream: company_segments
          syncMode: incremental
        target: ${PREFIX}/companysegments
      - resource:
          stream: conversations
          syncMode: incremental
        target: ${PREFIX}/conversations
      - resource:
          stream: conversation_parts
          syncMode: incremental
        target: ${PREFIX}/conversationparts
      - resource:
          stream: contacts
          syncMode: incremental
        target: ${PREFIX}/contacts
      - resource:
          stream: company_attributes
          syncMode: full_refresh
        target: ${PREFIX}/companyattributes
      - resource:
          stream: contact_attributes
          syncMode: full_refresh
        target: ${PREFIX}/contactattributes
      - resource:
          stream: segments
          syncMode: incremental
        target: ${PREFIX}/segments
      - resource:
          stream: tags
          syncMode: full_refresh
        target: ${PREFIX}/tags
      - resource:
          stream: teams
          syncMode: full_refresh
        target: ${PREFIX}/teams
```
