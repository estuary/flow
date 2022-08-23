---
sidebar_position: 12
---

# Intercom

This connector captures data from Intercom into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-intercom:dev`](https://ghcr.io/estuary/source-intercom:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.
You can find their documentation [here](https://docs.airbyte.com/integrations/sources/intercom/),
but keep in mind that the two versions may be significantly different.

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

* The [access token](https://developers.intercom.com/building-apps/docs/authentication-types#section-how-to-get-your-access-token) for you Intercom account.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Intercom source connector.

### Properties

#### Endpoint

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